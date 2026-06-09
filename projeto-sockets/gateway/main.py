import asyncio
import collections
import logging
import math
import sqlite3
import time
import struct
import os
import socket
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Tuple

# Biblioteca assíncrona para I/O não-bloqueante no SQLite
import aiosqlite
from google.protobuf.message import DecodeError

# ====================================================================
# [M5] LOGGING CONFIGURÁVEL VIA ENV VAR
#   LOG_LEVEL=DEBUG   → telemetria e probes (verboso)
#   LOG_LEVEL=INFO    → descoberta, comandos, eventos (padrão)
#   LOG_LEVEL=WARNING → apenas alertas e erros
# ====================================================================

_LOG_LEVEL_STR = os.getenv("LOG_LEVEL", "INFO").upper()
_LOG_LEVEL     = getattr(logging, _LOG_LEVEL_STR, logging.INFO)

logging.basicConfig(
    level=_LOG_LEVEL,
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("Gateway")

# ====================================================================
# CONFIGURAÇÕES DE BANCO DE DADOS
# ====================================================================

DB_DIR = "db"
if not os.path.exists(DB_DIR):
    os.makedirs(DB_DIR)

DB_FILE            = os.path.join(DB_DIR, "smartcity_gateway.db")
DB_POOL_SIZE       = max(1, int(os.getenv("DB_POOL_SIZE", "4")))
DB_BUSY_TIMEOUT_MS = 10000

# Intervalo do WAL checkpoint em segundos (padrão 5 min, mínimo 60 s)
WAL_CHECKPOINT_INTERVAL_SECS = max(60.0, float(os.getenv("WAL_CHECKPOINT_INTERVAL_SECS", "300")))

# [OLAP] Gestão de volume e aceleração de consultas
TELEMETRY_QUEUE_MAXSIZE = max(100, int(os.getenv("TELEMETRY_QUEUE_MAXSIZE", "10000")))
TELEMETRY_BATCH_MAX_PAYLOADS = max(1, int(os.getenv("TELEMETRY_BATCH_MAX_PAYLOADS", "100")))
TELEMETRY_BATCH_MAX_ROWS = max(1, int(os.getenv("TELEMETRY_BATCH_MAX_ROWS", "500")))
TELEMETRY_BATCH_FLUSH_INTERVAL_SECS = max(
    0.05,
    float(os.getenv("TELEMETRY_BATCH_FLUSH_INTERVAL_SECS", "1.0")),
)

METRICS_RAW_RETENTION_SECS = max(0, int(os.getenv("METRICS_RAW_RETENTION_SECS", str(7 * 24 * 3600))))
ROLLUP_1M_RETENTION_SECS = max(0, int(os.getenv("ROLLUP_1M_RETENTION_SECS", str(30 * 24 * 3600))))
ROLLUP_5M_RETENTION_SECS = max(0, int(os.getenv("ROLLUP_5M_RETENTION_SECS", str(180 * 24 * 3600))))
ROLLUP_1H_RETENTION_SECS = max(0, int(os.getenv("ROLLUP_1H_RETENTION_SECS", str(365 * 24 * 3600))))
METRICS_RETENTION_INTERVAL_SECS = max(60.0, float(os.getenv("METRICS_RETENTION_INTERVAL_SECS", "3600")))
ROLLUP_BACKFILL_ON_STARTUP = os.getenv("ROLLUP_BACKFILL_ON_STARTUP", "1").lower() not in {"0", "false", "no"}

OLAP_RAW_MAX_WINDOW_SECS = max(60, int(os.getenv("OLAP_RAW_MAX_WINDOW_SECS", "3600")))
OLAP_1M_MAX_WINDOW_SECS = max(OLAP_RAW_MAX_WINDOW_SECS, int(os.getenv("OLAP_1M_MAX_WINDOW_SECS", str(24 * 3600))))
OLAP_5M_MAX_WINDOW_SECS = max(OLAP_1M_MAX_WINDOW_SECS, int(os.getenv("OLAP_5M_MAX_WINDOW_SECS", str(7 * 24 * 3600))))

ROLLUP_TABLES = (
    ("metrics_rollup_1m", 60, ROLLUP_1M_RETENTION_SECS),
    ("metrics_rollup_5m", 300, ROLLUP_5M_RETENTION_SECS),
    ("metrics_rollup_1h", 3600, ROLLUP_1H_RETENTION_SECS),
)

DB_POOL = None
TELEMETRY_QUEUE: asyncio.Queue | None = None

# Armazena o último pacote processado por dispositivo para idempotência de DataPayload
# Usa OrderedDict com evicção LRU (máx 1000 entradas) para evitar memory leak
LAST_MESSAGE_INFO = collections.OrderedDict()

# Referências fortes para tasks UDP criadas via asyncio.create_task.
# O event loop mantém apenas referências fracas; sem este set, o GC pode coletar
# a task antes de ela concluir, descartando telemetria/descoberta silenciosamente.
_BACKGROUND_TASKS: set[asyncio.Task] = set()

# Importa as classes do Protobuf geradas dinamicamente
import messages_pb2  # pyright: ignore[reportMissingImports]

# ====================================================================
# CONFIGURAÇÕES DE REDE
# ====================================================================

UDP_TELEMETRY_PORT = 5000   # Porta dedicada exclusivamente à ingestão de dados
UDP_DISCOVERY_PORT = 5002   # Porta dedicada exclusivamente aos handshakes de topologia
TCP_PORT           = 5001

# Timeout para leitura de cabeçalho e payload TCP do cliente (configurável)
TCP_CLIENT_READ_TIMEOUT = max(5.0, float(os.getenv("TCP_CLIENT_READ_TIMEOUT", "10")))
TCP_CLIENT_IDLE_TIMEOUT = max(
    TCP_CLIENT_READ_TIMEOUT,
    float(os.getenv("TCP_CLIENT_IDLE_TIMEOUT", "60")),
)
TCP_MAX_FRAME_BYTES = max(1024, int(os.getenv("TCP_MAX_FRAME_BYTES", str(1024 * 1024))))

MULTICAST_GROUP          = "239.0.0.1"
MULTICAST_PORT           = 5005
DISCOVERY_PROBE_PAYLOAD  = b"SMARTCITY_DISCOVERY_PROBE"
DISCOVERY_PROBE_INTERVAL_SECS = max(1.0, float(os.getenv("DISCOVERY_PROBE_INTERVAL_SECS", "15")))
MULTICAST_TTL            = max(1, int(os.getenv("MULTICAST_TTL", "1")))
DEVICE_OFFLINE_TIMEOUT_SECS = max(1.0, float(os.getenv("DEVICE_OFFLINE_TIMEOUT_SECS", "45")))
DEVICE_OFFLINE_CHECK_INTERVAL_SECS = max(1.0, float(os.getenv("DEVICE_OFFLINE_CHECK_INTERVAL_SECS", "5")))

# ====================================================================
# INICIALIZAÇÃO DE BANCO DE DADOS
# ====================================================================

@dataclass(slots=True)
class MetricSample:
    device_id: str
    timestamp: int
    metric_name: str
    value: float
    unit: str


@dataclass(slots=True)
class TelemetryEnvelope:
    device_id: str
    ip: str
    timestamp: int
    status: int
    metrics: list[MetricSample]


class SQLiteConnectionPool:
    """Pool simples para reutilizar conexões aiosqlite entre requisições."""

    def __init__(self, db_file: str, size: int, timeout: float = 10.0):
        self.db_file = db_file
        self.size    = size
        self.timeout = timeout
        self._queue       = asyncio.Queue(maxsize=size)
        self._connections = []
        self._started     = False

    async def start(self):
        if self._started:
            return

        for _ in range(self.size):
            db = await aiosqlite.connect(self.db_file, timeout=self.timeout)
            await db.execute(f"PRAGMA busy_timeout = {DB_BUSY_TIMEOUT_MS};")
            await db.execute("PRAGMA foreign_keys = ON;")
            await db.commit()
            self._connections.append(db)
            await self._queue.put(db)

        self._started = True
        log.info("Pool SQLite inicializado com %d conexões.", self.size)

    @asynccontextmanager
    async def connection(self):
        if not self._started:
            raise RuntimeError("Pool SQLite ainda não foi inicializado.")

        db = await self._queue.get()
        try:
            yield db
        except Exception:
           
            try:
                await db.rollback()
            except Exception as rollback_exc:
                log.warning("Rollback SQLite falhou (conexão possivelmente inválida): %s", rollback_exc)
            raise
        finally:
            self._queue.put_nowait(db)

    async def close(self):
        while not self._queue.empty():
            self._queue.get_nowait()

        for db in self._connections:
            await db.close()

        self._connections.clear()
        self._started = False
        log.info("Pool SQLite encerrado.")


def get_db_pool() -> SQLiteConnectionPool:
    if DB_POOL is None:
        raise RuntimeError("Pool SQLite não inicializado.")
    return DB_POOL


def get_telemetry_queue() -> asyncio.Queue:
    if TELEMETRY_QUEUE is None:
        raise RuntimeError("Fila de telemetria não inicializada.")
    return TELEMETRY_QUEUE


def ensure_metrics_index(cursor: sqlite3.Cursor):
    """Garante índices compatíveis com ingestão, retenção e consultas OLAP."""

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_metrics_metric_time_device
        ON metrics (metric_name, timestamp, device_id)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_metrics_device_metric_time
        ON metrics (device_id, metric_name, timestamp)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_metrics_timestamp
        ON metrics (timestamp)
    """)


def ensure_rollup_tables(cursor: sqlite3.Cursor):
    """Cria tabelas agregadas para reduzir varreduras OLAP em janelas grandes."""
    for table_name, _, _ in ROLLUP_TABLES:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                bucket_start INTEGER NOT NULL,
                device_id    TEXT    NOT NULL,
                metric_name  TEXT    NOT NULL,
                unit         TEXT,
                sample_count INTEGER NOT NULL,
                value_sum    REAL    NOT NULL,
                value_sum_sq REAL    NOT NULL,
                value_min    REAL    NOT NULL,
                value_max    REAL    NOT NULL,
                PRIMARY KEY (bucket_start, device_id, metric_name)
            )
        """)
        cursor.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{table_name}_metric_bucket_device
            ON {table_name} (metric_name, bucket_start, device_id)
        """)
        cursor.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{table_name}_device_metric_bucket
            ON {table_name} (device_id, metric_name, bucket_start)
        """)


def backfill_rollup_tables(cursor: sqlite3.Cursor):
    """Popula rollups a partir de métricas brutas já existentes no banco."""
    if not ROLLUP_BACKFILL_ON_STARTUP:
        return

    for table_name, bucket_size, _ in ROLLUP_TABLES:
        cursor.execute(f"""
            INSERT OR IGNORE INTO {table_name}
            (bucket_start, device_id, metric_name, unit, sample_count,
             value_sum, value_sum_sq, value_min, value_max)
            SELECT
                CAST(timestamp / ? AS INTEGER) * ? AS bucket_start,
                device_id,
                metric_name,
                MAX(unit) AS unit,
                COUNT(*) AS sample_count,
                SUM(value) AS value_sum,
                SUM(value * value) AS value_sum_sq,
                MIN(value) AS value_min,
                MAX(value) AS value_max
            FROM metrics
            WHERE timestamp IS NOT NULL
              AND metric_name IS NOT NULL
              AND device_id IS NOT NULL
            GROUP BY bucket_start, device_id, metric_name
        """, (bucket_size, bucket_size))


def init_db():
    """Inicialização síncrona executada apenas no boot do Gateway."""
    conn   = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Ativa o modo WAL (Write-Ahead Logging) para otimizar concorrência de leitura/escrita
    cursor.execute("PRAGMA journal_mode=WAL;")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS devices (
            device_id    TEXT PRIMARY KEY,
            type         INTEGER,
            status       INTEGER,
            ip_address   TEXT,
            control_port INTEGER,
            is_controllable INTEGER,
            last_seen    INTEGER
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_devices_last_seen
        ON devices (last_seen)
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS metrics (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id   TEXT,
            timestamp   INTEGER,
            metric_name TEXT,
            value       REAL,
            unit        TEXT
        )
    """)
    ensure_metrics_index(cursor)
    ensure_rollup_tables(cursor)
    backfill_rollup_tables(cursor)
    conn.commit()
    conn.close()

# ====================================================================
# WORKERS ASSÍNCRONOS DE PERSISTÊNCIA
# ====================================================================

def build_telemetry_envelope(payload: messages_pb2.DataPayload, ip: str) -> TelemetryEnvelope:
    return TelemetryEnvelope(
        device_id=payload.device_id,
        ip=ip,
        timestamp=int(payload.timestamp),
        status=int(payload.current_status),
        metrics=[
            MetricSample(
                device_id=payload.device_id,
                timestamp=int(payload.timestamp),
                metric_name=m.name,
                value=float(m.value),
                unit=m.unit,
            )
            for m in payload.metrics
        ],
    )


def build_rollup_rows(metric_rows: list[tuple[str, int, str, float, str]]):
    rollup_rows: dict[str, list[tuple[int, str, str, str, int, float, float, float, float]]] = {}

    for table_name, bucket_size, _ in ROLLUP_TABLES:
        aggregated: dict[tuple[int, str, str], dict[str, float | int | str]] = {}

        for device_id, timestamp, metric_name, value, unit in metric_rows:
            bucket_start = (int(timestamp) // bucket_size) * bucket_size
            key = (bucket_start, device_id, metric_name)
            current = aggregated.get(key)

            if current is None:
                aggregated[key] = {
                    "unit": unit,
                    "sample_count": 1,
                    "value_sum": value,
                    "value_sum_sq": value * value,
                    "value_min": value,
                    "value_max": value,
                }
            else:
                current["unit"] = unit or current["unit"]
                current["sample_count"] = int(current["sample_count"]) + 1
                current["value_sum"] = float(current["value_sum"]) + value
                current["value_sum_sq"] = float(current["value_sum_sq"]) + (value * value)
                current["value_min"] = min(float(current["value_min"]), value)
                current["value_max"] = max(float(current["value_max"]), value)

        rollup_rows[table_name] = [
            (
                bucket_start,
                device_id,
                metric_name,
                str(values["unit"]),
                int(values["sample_count"]),
                float(values["value_sum"]),
                float(values["value_sum_sq"]),
                float(values["value_min"]),
                float(values["value_max"]),
            )
            for (bucket_start, device_id, metric_name), values in aggregated.items()
        ]

    return rollup_rows


async def persist_telemetry_batch(batch: list[TelemetryEnvelope]):
    if not batch:
        return

    now = int(time.time())
    device_updates = {
        # control_port=0 incluído explicitamente para evitar NULL quando
        # a telemetria chega antes da mensagem de descoberta (race condition de rede).
        # INSERT OR IGNORE garante que uma descoberta posterior sobrescreva via
        # process_discovery (ON CONFLICT DO UPDATE com o valor real da porta).
        envelope.device_id: (envelope.device_id, 0, envelope.status, envelope.ip, 0, 0, now)
        for envelope in batch
    }
    device_update_rows = [(now, row[2], row[0]) for row in device_updates.values()]
    metric_rows = [
        (
            sample.device_id,
            sample.timestamp,
            sample.metric_name,
            sample.value,
            sample.unit,
        )
        for envelope in batch
        for sample in envelope.metrics
    ]

    async with get_db_pool().connection() as db:
        await db.executemany("""
            INSERT OR IGNORE INTO devices (device_id, type, status, ip_address, control_port, is_controllable, last_seen)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, list(device_updates.values()))

        await db.executemany("""
            UPDATE devices SET last_seen = ?, status = ? WHERE device_id = ?
        """, device_update_rows)

        if metric_rows:
            await db.executemany("""
                INSERT INTO metrics (device_id, timestamp, metric_name, value, unit)
                VALUES (?, ?, ?, ?, ?)
            """, metric_rows)

            for table_name, rows in build_rollup_rows(metric_rows).items():
                if not rows:
                    continue
                await db.executemany(f"""
                    INSERT INTO {table_name}
                    (bucket_start, device_id, metric_name, unit, sample_count,
                     value_sum, value_sum_sq, value_min, value_max)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(bucket_start, device_id, metric_name) DO UPDATE SET
                        unit         = excluded.unit,
                        sample_count = {table_name}.sample_count + excluded.sample_count,
                        value_sum    = {table_name}.value_sum + excluded.value_sum,
                        value_sum_sq = {table_name}.value_sum_sq + excluded.value_sum_sq,
                        value_min    = MIN({table_name}.value_min, excluded.value_min),
                        value_max    = MAX({table_name}.value_max, excluded.value_max)
                """, rows)

        await db.commit()

    log.debug(
        "Batch de telemetria persistido: %d payload(s), %d métrica(s).",
        len(batch), len(metric_rows),
    )


async def process_telemetry(payload: messages_pb2.DataPayload, ip: str):
    """Enfileira telemetria para persistência em lote sem travar o Event Loop."""
    envelope = build_telemetry_envelope(payload, ip)
    await get_telemetry_queue().put(envelope)


async def telemetry_batch_worker_loop():
    """Consome a fila UDP e grava métricas brutas + rollups em batches."""
    queue = get_telemetry_queue()

    while True:
        batch: list[TelemetryEnvelope] = []
        try:
            first = await queue.get()
            batch.append(first)
            metric_rows = len(first.metrics)
            deadline = asyncio.get_running_loop().time() + TELEMETRY_BATCH_FLUSH_INTERVAL_SECS

            while (
                len(batch) < TELEMETRY_BATCH_MAX_PAYLOADS
                and metric_rows < TELEMETRY_BATCH_MAX_ROWS
            ):
                timeout = deadline - asyncio.get_running_loop().time()
                if timeout <= 0:
                    break

                try:
                    item = await asyncio.wait_for(queue.get(), timeout=timeout)
                except asyncio.TimeoutError:
                    break

                batch.append(item)
                metric_rows += len(item.metrics)

            await persist_telemetry_batch(batch)
        except asyncio.CancelledError:
            if batch:
                await persist_telemetry_batch(batch)
                for _ in batch:
                    queue.task_done()
            raise
        except Exception as exc:
            log.warning("Falha no worker de batch de telemetria: %s", exc)
            for _ in batch:
                queue.task_done()
        else:
            for _ in batch:
                queue.task_done()


async def process_discovery(disc: messages_pb2.DiscoveryResponse, ip: str):
    """Registra ou renova a presença de nós operacionais assincronamente."""
    now = int(time.time())

    announced = disc.ip_address.strip() if disc.ip_address else ""
    effective_ip = announced if announced else ip

    async with get_db_pool().connection() as db:
        async with db.execute(
            "SELECT 1 FROM devices WHERE device_id = ?",
            (disc.device_id,),
        ) as cursor:
            existing_device = await cursor.fetchone()

        await db.execute("""
            INSERT INTO devices
            (device_id, type, status, ip_address, control_port, is_controllable, last_seen)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(device_id) DO UPDATE SET
                type            = excluded.type,
                status          = excluded.status,
                ip_address      = excluded.ip_address,
                control_port    = excluded.control_port,
                is_controllable = excluded.is_controllable,
                last_seen       = excluded.last_seen
        """, (disc.device_id, disc.type, disc.initial_status, effective_ip,
              disc.control_port, int(disc.is_controllable), now))
        await db.commit()

    if existing_device is None:
        log.info(
            "Nó registrado: '%s' — rede=%s anunciado='%s' porta=%d (controlável=%s).",
            disc.device_id, ip, announced or "<não informado>",
            disc.control_port, disc.is_controllable,
        )
    else:
        log.debug(
            "Heartbeat/topologia renovado: '%s' — rede=%s anunciado='%s' porta=%d.",
            disc.device_id, ip, announced or "<não informado>", disc.control_port,
        )


# ====================================================================
# CAMADA DE REDE: INGESTÃO E DESCOBERTA (MULTIPLEXAÇÃO FÍSICA UDP)
# ====================================================================

class TelemetryUDPProtocol(asyncio.DatagramProtocol):
    """Protocolo de transporte focado estritamente na ingestão contínua (Porta 5000)."""

    def connection_made(self, transport):
        self.transport = transport
        log.info("Interface de Telemetria ativa na porta %d.", UDP_TELEMETRY_PORT)

    def datagram_received(self, data: bytes, addr: Tuple[str, int]):
        """Decodifica estritamente fluxos operacionais DataPayload."""

        # Tenta extrair datagramas de Telemetria (Métricas Físicas)
        try:
            payload = messages_pb2.DataPayload()
            payload.ParseFromString(data)

            # Acesso correto aos enumeradores Protobuf exportados em módulo
            if payload.device_id and (
                payload.current_status != messages_pb2.STATUS_UNKNOWN
                or len(payload.metrics) > 0
            ):
                last_info = LAST_MESSAGE_INFO.get(payload.device_id)
                if last_info is not None:
                    last_ts, last_msg_id = last_info
                    if payload.timestamp < last_ts:
                        log.warning(
                            "Mensagem atrasada de '%s' (ts=%d) ignorada. Último ts=%d.",
                            payload.device_id, payload.timestamp, last_ts,
                        )
                        return
                    if payload.timestamp == last_ts and payload.message_id == last_msg_id:
                        log.debug("Mensagem duplicada de '%s' ignorada.", payload.device_id)
                        return

                # Evicção LRU: manter máximo de 1000 entradas em LAST_MESSAGE_INFO
                # `while` em vez de `if` — garante que o dict nunca
                # ultrapasse 1000 entradas mesmo em inserções simultâneas.
                while len(LAST_MESSAGE_INFO) >= 1000:
                    LAST_MESSAGE_INFO.popitem(last=False)
                
                LAST_MESSAGE_INFO[payload.device_id] = (payload.timestamp, payload.message_id)
                # Guarda referência forte para evitar coleta pelo GC
                # antes da task completar (event loop mantém apenas refs fracas).
                task = asyncio.create_task(process_telemetry(payload, addr[0]))
                _BACKGROUND_TASKS.add(task)
                task.add_done_callback(_BACKGROUND_TASKS.discard)
                log.debug(
                    "Pacote ID [%s] de '%s' — atraso %ds.",
                    payload.message_id, payload.device_id,
                    int(time.time()) - payload.timestamp,
                )
                return
        except Exception as exc:
            log.debug("Datagrama de telemetria inválido de %s: %s", addr, exc)


class DiscoveryUDPProtocol(asyncio.DatagramProtocol):
    """Protocolo de transporte focado no registro de topologia (Porta 5002)."""

    def connection_made(self, transport):
        self.transport = transport
        log.info("Interface de Descoberta ativa na porta %d.", UDP_DISCOVERY_PORT)

    def datagram_received(self, data: bytes, addr: Tuple[str, int]):
        """Decodifica estritamente fluxos de handshake e heartbeat."""
        try:
            disc = messages_pb2.DiscoveryResponse()
            disc.ParseFromString(data)

            # Aceita dispositivos não-controláveis validando apenas o device_id
            if disc.device_id:
                # [FIX BUG-01] Idem: referência forte evita GC prematuro da task.
                task = asyncio.create_task(process_discovery(disc, addr[0]))
                _BACKGROUND_TASKS.add(task)
                task.add_done_callback(_BACKGROUND_TASKS.discard)
        except Exception as e:
            log.error("Falha na decodificação de datagrama de Descoberta %s: %s", addr, e)


async def multicast_discovery_probe_loop():
    """Solicita periodicamente que sensores reanunciem a própria topologia."""
    await asyncio.sleep(2.0)

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP) as sock:
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, MULTICAST_TTL)

            while True:
                try:
                    sock.sendto(DISCOVERY_PROBE_PAYLOAD, (MULTICAST_GROUP, MULTICAST_PORT))
                    log.debug(
                        "Probe de descoberta enviado para %s:%d.",
                        MULTICAST_GROUP, MULTICAST_PORT,
                    )
                except OSError as exc:
                    log.warning("Falha ao enviar probe de descoberta: %s", exc)

                await asyncio.sleep(DISCOVERY_PROBE_INTERVAL_SECS)
    except asyncio.CancelledError:
        raise
    except OSError as exc:
        log.error("Loop de probes indisponível: %s", exc)


# ====================================================================
# [M3] MANUTENÇÃO DO BANCO: WAL CHECKPOINT PERIÓDICO
# ====================================================================

async def wal_checkpoint_loop():
    """Emite PRAGMA wal_checkpoint(PASSIVE) periodicamente para limitar crescimento do WAL."""
    await asyncio.sleep(WAL_CHECKPOINT_INTERVAL_SECS)

    while True:
        try:
            async with get_db_pool().connection() as db:
                await db.execute("PRAGMA wal_checkpoint(PASSIVE);")
                await db.commit()
            log.debug("WAL checkpoint executado (intervalo=%ds).", WAL_CHECKPOINT_INTERVAL_SECS)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("Falha no WAL checkpoint: %s", exc)

        await asyncio.sleep(WAL_CHECKPOINT_INTERVAL_SECS)


async def device_offline_monitor_loop():
    """Marca dispositivos como offline quando deixam de renovar last_seen."""
    await asyncio.sleep(DEVICE_OFFLINE_CHECK_INTERVAL_SECS)

    while True:
        try:
            cutoff = int(time.time() - DEVICE_OFFLINE_TIMEOUT_SECS)
            # rowcount capturado dentro do bloco async with, enquanto a
            # conexão ainda está checada para uso. Acessá-lo após o bloco é frágil
            # pois a conexão pode ser reutilizada por outra corrotina.
            marked_offline = 0
            async with get_db_pool().connection() as db:
                cursor = await db.execute("""
                    UPDATE devices
                    SET status = ?
                    WHERE last_seen < ? AND status != ?
                """, (messages_pb2.STATUS_OFF, cutoff, messages_pb2.STATUS_OFF))
                await db.commit()
                marked_offline = cursor.rowcount

            if marked_offline > 0:
                log.warning(
                    "%d dispositivo(s) sem heartbeat por mais de %.1fs marcados como offline.",
                    marked_offline, DEVICE_OFFLINE_TIMEOUT_SECS,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("Falha no monitor de presença dos dispositivos: %s", exc)

        await asyncio.sleep(DEVICE_OFFLINE_CHECK_INTERVAL_SECS)


async def metrics_retention_loop():
    """Remove dados antigos conforme políticas de retenção configuradas."""
    await asyncio.sleep(METRICS_RETENTION_INTERVAL_SECS)

    while True:
        try:
            now = int(time.time())
            deleted_parts: list[str] = []

            async with get_db_pool().connection() as db:
                if METRICS_RAW_RETENTION_SECS > 0:
                    cursor = await db.execute(
                        "DELETE FROM metrics WHERE timestamp < ?",
                        (now - METRICS_RAW_RETENTION_SECS,),
                    )
                    deleted_parts.append(f"raw={cursor.rowcount}")

                for table_name, _, retention_secs in ROLLUP_TABLES:
                    if retention_secs <= 0:
                        continue
                    cursor = await db.execute(
                        f"DELETE FROM {table_name} WHERE bucket_start < ?",
                        (now - retention_secs,),
                    )
                    deleted_parts.append(f"{table_name}={cursor.rowcount}")

                await db.commit()

            log.debug("Retenção de métricas executada (%s).", ", ".join(deleted_parts) or "sem limites")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("Falha na retenção de métricas: %s", exc)

        await asyncio.sleep(METRICS_RETENTION_INTERVAL_SECS)


# ====================================================================
# CAMADA DE REDE: ATENDIMENTO AO CLIENTE (TCP COM FRAMING)
# ====================================================================

def new_client_response(success: bool = True) -> messages_pb2.ClientResponse:
    resp = messages_pb2.ClientResponse(success=success)
    resp.message_id = f"GW-RESP-{uuid.uuid4().hex[:8]}"
    resp.timestamp  = int(time.time())
    return resp


@dataclass(slots=True)
class OlapSource:
    name: str
    table_name: str
    bucket_size: int
    is_rollup: bool


@dataclass(slots=True)
class OlapQueryResult:
    success: bool
    message: str
    analytics_result: float
    result_metadata: str
    graph_rows: list[tuple[int, float, str]]
    sample_count: int


def choose_olap_source(start_timestamp: int, end_timestamp: int) -> OlapSource:
    window_secs = max(0, end_timestamp - start_timestamp)
    if window_secs <= OLAP_RAW_MAX_WINDOW_SECS:
        return OlapSource("raw", "metrics", 0, False)
    if window_secs <= OLAP_1M_MAX_WINDOW_SECS:
        return OlapSource("rollup_1m", "metrics_rollup_1m", 60, True)
    if window_secs <= OLAP_5M_MAX_WINDOW_SECS:
        return OlapSource("rollup_5m", "metrics_rollup_5m", 300, True)
    return OlapSource("rollup_1h", "metrics_rollup_1h", 3600, True)


def olap_time_range(source: OlapSource, start_timestamp: int, end_timestamp: int) -> tuple[int, int]:
    if not source.is_rollup:
        return start_timestamp, end_timestamp

    return (
        (start_timestamp // source.bucket_size) * source.bucket_size,
        (end_timestamp // source.bucket_size) * source.bucket_size,
    )


def sample_stddev(sample_count: int, value_sum: float, value_sum_sq: float) -> float:
    if sample_count <= 1:
        return 0.0

    variance = (value_sum_sq - ((value_sum * value_sum) / sample_count)) / (sample_count - 1)
    return math.sqrt(max(0.0, variance))


async def fetch_graph_rows(
    db,
    source: OlapSource,
    metric_name: str,
    start_timestamp: int,
    end_timestamp: int,
    target_device_id: str = "",
) -> list[tuple[int, float, str]]:
    # Query construída dinamicamente com base em (is_rollup × target_device_id).
    query_start, query_end = olap_time_range(source, start_timestamp, end_timestamp)

    params: list = [metric_name, query_start, query_end]
    device_filter = ""
    if target_device_id:
        device_filter = "AND device_id = ?"
        params.append(target_device_id)

    if source.is_rollup:
        query = f"""
            SELECT bucket_start,
                   value_sum / sample_count AS bucket_avg,
                   device_id
            FROM {source.table_name}
            WHERE metric_name = ? AND bucket_start BETWEEN ? AND ? {device_filter}
            ORDER BY bucket_start ASC, device_id ASC
        """
    else:
        query = f"""
            SELECT timestamp, value, device_id
            FROM metrics
            WHERE metric_name = ? AND timestamp BETWEEN ? AND ? {device_filter}
            ORDER BY timestamp ASC
        """

    async with db.execute(query, params) as cursor:
        return [
            (int(ts), float(value), str(device_id))
            for ts, value, device_id in await cursor.fetchall()
        ]


async def execute_olap_from_source(
    db,
    req: messages_pb2.ClientRequest,
    source: OlapSource,
) -> OlapQueryResult:
    # Queries construídas dinamicamente com base em (is_rollup × target_device_id).
    query_start, query_end = olap_time_range(source, req.start_timestamp, req.end_timestamp)

    # Fragmentos que diferem entre rollup e raw
    if source.is_rollup:
        time_col    = "bucket_start"
        count_col   = "SUM(sample_count)"
        sum_col     = "SUM(value_sum)"
        sumsq_col   = "SUM(value_sum_sq)"
        min_col     = "MIN(value_min)"
        max_col     = "MAX(value_max)"
    else:
        time_col    = "timestamp"
        count_col   = "COUNT(*)"
        sum_col     = "COALESCE(SUM(value), 0.0)"
        sumsq_col   = "COALESCE(SUM(value * value), 0.0)"
        min_col     = "MIN(value)"
        max_col     = "MAX(value)"

    # Filtro de dispositivo opcional
    params_base: list = [req.query_metric, query_start, query_end]
    device_filter = ""
    if req.target_device_id:
        device_filter = "AND device_id = ?"
        params_base.append(req.target_device_id)

    if req.query_op in (messages_pb2.OP_AVERAGE, messages_pb2.OP_STD_DEV):
        async with db.execute(f"""
            SELECT COALESCE({count_col}, 0),
                   COALESCE({sum_col}, 0.0),
                   COALESCE({sumsq_col}, 0.0)
            FROM {source.table_name}
            WHERE metric_name = ? AND {time_col} BETWEEN ? AND ? {device_filter}
        """, params_base) as cursor:
            row = await cursor.fetchone()

        sample_count = int(row[0] or 0)
        value_sum    = float(row[1] or 0.0)
        value_sum_sq = float(row[2] or 0.0)

        if sample_count == 0:
            return OlapQueryResult(
                success=False,
                message="Janela de dados insuficiente para computação estatística.",
                analytics_result=0.0,
                result_metadata="",
                graph_rows=[],
                sample_count=0,
            )

        if req.query_op == messages_pb2.OP_AVERAGE:
            analytics_result = value_sum / sample_count
        else:
            analytics_result = sample_stddev(sample_count, value_sum, value_sum_sq)

        graph_rows = await fetch_graph_rows(
            db, source, req.query_metric, req.start_timestamp, req.end_timestamp, req.target_device_id,
        )
        bucket_label = f", bucket={source.bucket_size}s" if source.is_rollup else ""
        return OlapQueryResult(
            success=True,
            message="",
            analytics_result=analytics_result,
            result_metadata=(
                f"Fonte OLAP: {source.name}{bucket_label}. "
                f"Amostras processadas: {sample_count}. Pontos retornados: {len(graph_rows)}."
            ),
            graph_rows=graph_rows,
            sample_count=sample_count,
        )

    if req.query_op == messages_pb2.OP_MAX_VARIATION:
        async with db.execute(f"""
            SELECT device_id,
                   {count_col} AS total_count,
                   {min_col}   AS min_value,
                   {max_col}   AS max_value
            FROM {source.table_name}
            WHERE metric_name = ? AND {time_col} BETWEEN ? AND ? {device_filter}
            GROUP BY device_id
            HAVING {count_col} > 1
        """, params_base) as cursor:
            variation_rows = await cursor.fetchall()

        if not variation_rows:
            return OlapQueryResult(
                success=False,
                message="Dados insuficientes para calcular variação por dispositivo.",
                analytics_result=0.0,
                result_metadata="",
                graph_rows=[],
                sample_count=0,
            )

        best_dev, total_count, min_value, max_value = max(
            variation_rows,
            key=lambda row: float(row[3]) - float(row[2]),
        )
        max_variation = float(max_value) - float(min_value)
        sample_count  = sum(int(row[1]) for row in variation_rows)
        graph_rows    = await fetch_graph_rows(
            db, source, req.query_metric, req.start_timestamp, req.end_timestamp, req.target_device_id,
        )
        bucket_label = f", bucket={source.bucket_size}s" if source.is_rollup else ""
        return OlapQueryResult(
            success=True,
            message="",
            analytics_result=max_variation,
            result_metadata=(
                f"Fonte OLAP: {source.name}{bucket_label}. "
                f"Maior variação: dispositivo {best_dev} — "
                f"{len(variation_rows)} nós avaliados, {sample_count} amostras."
            ),
            graph_rows=graph_rows,
            sample_count=sample_count,
        )

    return OlapQueryResult(
        success=False,
        message=f"Operação analítica desconhecida: {req.query_op}.",
        analytics_result=0.0,
        result_metadata="",
        graph_rows=[],
        sample_count=0,
    )


async def execute_adaptive_olap(db, req: messages_pb2.ClientRequest) -> OlapQueryResult:
    source = choose_olap_source(req.start_timestamp, req.end_timestamp)
    result = await execute_olap_from_source(db, req, source)

    if result.success or not source.is_rollup:
        return result

    raw_source = OlapSource("raw-fallback", "metrics", 0, False)
    fallback = await execute_olap_from_source(db, req, raw_source)
    if fallback.success:
        fallback.result_metadata = (
            fallback.result_metadata
            + " Rollup escolhido sem dados suficientes; consulta bruta usada como fallback."
        )
    return fallback


async def build_client_response(
    req: messages_pb2.ClientRequest,
    peer,
) -> messages_pb2.ClientResponse:
    """Processa uma requisição já desserializada e preserva o contrato ClientResponse."""
    resp = new_client_response(success=True)

    # ── Rota: Sincronização de Inventário ────────────────────────────
    if req.type == messages_pb2.REQUEST_TYPE_LIST_DEVICES:
        async with get_db_pool().connection() as db:
            # [R1] Colunas explícitas — resistente a mudanças futuras de schema
            async with db.execute("""
                SELECT device_id, type, status, ip_address,
                       control_port, is_controllable, last_seen
                FROM devices
            """) as cursor:
                async for row in cursor:
                    device_id, dtype, status, ip, ctrl_port, is_ctrl, last_seen = row
                    d = resp.devices.add()
                    d.device_id           = device_id
                    d.type                = dtype
                    d.status              = status
                    d.ip_address          = ip
                    d.control_port        = ctrl_port
                    d.is_controllable     = bool(is_ctrl)
                    d.last_seen_timestamp = last_seen

        resp.message = "Sincronização de topologia extraída via pool aiosqlite."
        log.info("LIST_DEVICES → %d nós retornados para %s.", len(resp.devices), peer)
        return resp

    # ── Rota: Proxy de Atuação Remota ────────────────────────────────
    if req.type == messages_pb2.REQUEST_TYPE_SEND_COMMAND:
        async with get_db_pool().connection() as db:
            async with db.execute(
                "SELECT ip_address, control_port FROM devices WHERE device_id = ?",
                (req.target_device_id,),
            ) as cursor:
                target = await cursor.fetchone()

        if not target or target[1] <= 0:
            resp.success = False
            resp.message = "Nó não encontrado ou desprovido de porta de controle."
            return resp

        s_w = None
        try:
            s_r, s_w = await asyncio.wait_for(
                asyncio.open_connection(target[0], target[1]),
                timeout=5.0,
            )

            # Aplica Framing no envio do comando para os Atuadores (Lua/Java/Python)
            req.command_payload.target_device_id = req.target_device_id
            cmd_bytes = req.command_payload.SerializeToString()
            s_w.write(struct.pack(">I", len(cmd_bytes)) + cmd_bytes)
            await s_w.drain()

            # Lê a resposta binária do nó alvo respeitando a janela de Framing
            s_header = await asyncio.wait_for(s_r.readexactly(4), timeout=5.0)
            s_len    = struct.unpack(">I", s_header)[0]
            if s_len <= 0 or s_len > TCP_MAX_FRAME_BYTES:
                raise ValueError(f"frame de resposta inválido do nó alvo: {s_len} bytes")
            s_body   = await asyncio.wait_for(s_r.readexactly(s_len), timeout=5.0)

            node_resp = messages_pb2.ConfigResponse()
            node_resp.ParseFromString(s_body)

            resp.success, resp.message = node_resp.success, node_resp.message
            log.info(
                "SEND_COMMAND '%s' → '%s': sucesso=%s.",
                req.command_payload.command_id, req.target_device_id, resp.success,
            )
        except asyncio.TimeoutError:
            resp.success = False
            resp.message = "Timeout de I/O com o nó alvo durante atuação remota."
            log.warning("Timeout ao encaminhar comando para '%s'.", req.target_device_id)
        except ConnectionRefusedError as exc:
            resp.success = False
            resp.message = f"Nó alvo recusou a conexão TCP: {exc}"
            log.warning("Conexão recusada por '%s': %s", req.target_device_id, exc)
        except (asyncio.IncompleteReadError, struct.error, DecodeError, ValueError) as exc:
            resp.success = False
            resp.message = f"Resposta TCP/Protobuf inválida do nó alvo: {exc}"
            log.warning("Frame inválido de '%s': %s", req.target_device_id, exc)
        except OSError as exc:
            resp.success = False
            resp.message = f"Falha de socket com o nó alvo: {exc}"
            log.warning("Falha de socket ao encaminhar comando para '%s': %s", req.target_device_id, exc)
        finally:
            if s_w is not None:
                s_w.close()
                await s_w.wait_closed()

        return resp

    # ── Rota: Agregações Estatísticas (OLAP) ─────────────────────────
    if req.type == messages_pb2.REQUEST_TYPE_ANALYTICS_QUERY:
        async with get_db_pool().connection() as db:
            olap_result = await execute_adaptive_olap(db, req)

        resp.success = olap_result.success
        resp.message = olap_result.message
        resp.analytics_result = olap_result.analytics_result
        resp.result_metadata = olap_result.result_metadata

        if resp.success:
            for timestamp, value, device_id in olap_result.graph_rows:
                pt           = resp.graph_points.add()
                pt.timestamp = timestamp
                pt.value     = value
                pt.device_id = device_id

        log.info(
            "ANALYTICS_QUERY metric='%s' op=%d → %d amostras/%d pontos (sucesso=%s).",
            req.query_metric, req.query_op,
            olap_result.sample_count, len(olap_result.graph_rows), resp.success,
        )

        return resp

    resp.success = False
    resp.message = f"Tipo de requisição desconhecido: {req.type}."
    return resp


async def send_client_response(
    writer: asyncio.StreamWriter,
    resp: messages_pb2.ClientResponse,
):
    resp_bytes = resp.SerializeToString()
    writer.write(struct.pack(">I", len(resp_bytes)) + resp_bytes)
    await writer.drain()


async def handle_client_request(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    """Pipeline TCP assíncrono com Length-Prefix Framing e conexão persistente."""
    peer = writer.get_extra_info("peername")
    try:
        while True:
            try:
                header = await asyncio.wait_for(
                    reader.readexactly(4),
                    timeout=TCP_CLIENT_IDLE_TIMEOUT,
                )
            except asyncio.TimeoutError:
                log.debug("Conexão TCP ociosa com %s encerrada.", peer)
                break
            except asyncio.IncompleteReadError:
                log.debug("Cliente TCP %s encerrou a conexão.", peer)
                break

            try:
                msg_len = struct.unpack(">I", header)[0]
            except struct.error as exc:
                log.warning("Cabeçalho TCP inválido de %s: %s", peer, exc)
                break

            if msg_len <= 0 or msg_len > TCP_MAX_FRAME_BYTES:
                log.warning("Frame TCP inválido de %s: %d bytes.", peer, msg_len)
                resp = new_client_response(success=False)
                resp.message = f"Frame TCP inválido: {msg_len} bytes."
                await send_client_response(writer, resp)
                break

            try:
                data = await asyncio.wait_for(
                    reader.readexactly(msg_len),
                    timeout=TCP_CLIENT_READ_TIMEOUT,
                )
            except asyncio.TimeoutError:
                log.warning(
                    "Timeout lendo payload TCP de %s (%d bytes) — conexão encerrada.",
                    peer, msg_len,
                )
                break
            except asyncio.IncompleteReadError:
                log.warning("Payload TCP incompleto de %s — conexão encerrada.", peer)
                break

            req = messages_pb2.ClientRequest()
            try:
                req.ParseFromString(data)
            except DecodeError as exc:
                resp = new_client_response(success=False)
                resp.message = f"Payload Protobuf inválido: {exc}"
                await send_client_response(writer, resp)
                continue

            try:
                resp = await build_client_response(req, peer)
            except sqlite3.Error as exc:
                log.warning("Falha SQLite processando requisição de %s: %s", peer, exc)
                resp = new_client_response(success=False)
                resp.message = f"Falha SQLite no Gateway: {exc}"
            except Exception as exc:
                # Captura exceções inesperadas (ex.: AttributeError, TypeError)
                # que escapariam para o handler de conexão e encerrariam o canal TCP
                # sem enviar resposta ao cliente.
                log.error(
                    "Erro inesperado processando requisição de %s: %s",
                    peer, exc, exc_info=True,
                )
                resp = new_client_response(success=False)
                resp.message = "Erro interno no Gateway — consulte os logs do servidor."
            await send_client_response(writer, resp)

    except (ConnectionResetError, BrokenPipeError) as exc:
        log.warning("Conexão TCP interrompida por %s: %s", peer, exc)
    except OSError as exc:
        log.warning("Erro de socket no handler TCP (%s): %s", peer, exc)
    finally:
        writer.close()
        await writer.wait_closed()


# ====================================================================
# INICIALIZAÇÃO DO LOOP DE EVENTOS E KERNEL DE BORDAS
# ====================================================================

async def main():
    global DB_POOL, TELEMETRY_QUEUE

    log.info("============================================================")
    log.info("Inicializando nó central (aiosqlite / Framing Distribuído)...")
    log.info("LOG_LEVEL=%s | TCP_READ_TIMEOUT=%.1fs | TCP_IDLE_TIMEOUT=%.1fs | WAL_CHECKPOINT=%ds",
             _LOG_LEVEL_STR, TCP_CLIENT_READ_TIMEOUT, TCP_CLIENT_IDLE_TIMEOUT,
             WAL_CHECKPOINT_INTERVAL_SECS)
    log.info("OFFLINE_TIMEOUT=%.1fs | OFFLINE_CHECK=%.1fs",
             DEVICE_OFFLINE_TIMEOUT_SECS, DEVICE_OFFLINE_CHECK_INTERVAL_SECS)
    log.info(
        "OLAP raw<=%ds | 1m<=%ds | 5m<=%ds | raw_retention=%ds | batch=%d payloads/%d rows",
        OLAP_RAW_MAX_WINDOW_SECS, OLAP_1M_MAX_WINDOW_SECS, OLAP_5M_MAX_WINDOW_SECS,
        METRICS_RAW_RETENTION_SECS, TELEMETRY_BATCH_MAX_PAYLOADS, TELEMETRY_BATCH_MAX_ROWS,
    )
    log.info("============================================================")

    init_db()
    DB_POOL = SQLiteConnectionPool(DB_FILE, DB_POOL_SIZE)
    await DB_POOL.start()
    TELEMETRY_QUEUE = asyncio.Queue(maxsize=TELEMETRY_QUEUE_MAXSIZE)

    loop = asyncio.get_running_loop()

    # Provisionando as instâncias de transporte baseadas na segregação de portas
    telemetry_transport, _ = await loop.create_datagram_endpoint(
        TelemetryUDPProtocol, local_addr=("0.0.0.0", UDP_TELEMETRY_PORT)
    )
    discovery_transport, _ = await loop.create_datagram_endpoint(
        DiscoveryUDPProtocol, local_addr=("0.0.0.0", UDP_DISCOVERY_PORT)
    )

    # Servidor de Controle TCP
    server = await asyncio.start_server(handle_client_request, "0.0.0.0", TCP_PORT)

    # Tasks de background
    probe_task      = asyncio.create_task(multicast_discovery_probe_loop())
    checkpoint_task = asyncio.create_task(wal_checkpoint_loop())  # [M3]
    offline_task    = asyncio.create_task(device_offline_monitor_loop())
    telemetry_task  = asyncio.create_task(telemetry_batch_worker_loop())
    retention_task  = asyncio.create_task(metrics_retention_loop())

    log.info(
        "Hub pronto. TCP:%d | UDP(Telem):%d | UDP(Disc):%d",
        TCP_PORT, UDP_TELEMETRY_PORT, UDP_DISCOVERY_PORT,
    )

    try:
        async with server:
            await server.serve_forever()
    finally:
        # Cancela tasks UDP em voo (telemetria/descoberta) antes de fechar
        # os transportes e o pool. Sem isso, tasks que chegaram no último instante
        # podem tentar acessar o pool já encerrado.
        for task in list(_BACKGROUND_TASKS):
            task.cancel()
        if _BACKGROUND_TASKS:
            await asyncio.gather(*_BACKGROUND_TASKS, return_exceptions=True)

        probe_task.cancel()
        checkpoint_task.cancel()
        offline_task.cancel()
        telemetry_task.cancel()
        retention_task.cancel()
        await asyncio.gather(
            probe_task, checkpoint_task, offline_task, telemetry_task, retention_task,
            return_exceptions=True,
        )
        telemetry_transport.close()
        discovery_transport.close()
        await DB_POOL.close()
        DB_POOL = None
        TELEMETRY_QUEUE = None
        log.info("Gateway encerrado.")


if __name__ == "__main__":
    asyncio.run(main())