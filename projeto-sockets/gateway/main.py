import asyncio
import logging
import sqlite3
import time
import statistics
import struct
import os
import socket
import uuid
from contextlib import asynccontextmanager
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

# [M3] Intervalo do WAL checkpoint em segundos (padrão 5 min, mínimo 60 s)
WAL_CHECKPOINT_INTERVAL_SECS = max(60.0, float(os.getenv("WAL_CHECKPOINT_INTERVAL_SECS", "300")))

DB_POOL = None

# Armazena o último pacote processado por dispositivo para idempotência de DataPayload
LAST_MESSAGE_INFO = {}

# Importa as classes do Protobuf geradas dinamicamente
import messages_pb2  # pyright: ignore[reportMissingImports]

# ====================================================================
# CONFIGURAÇÕES DE REDE
# ====================================================================

UDP_TELEMETRY_PORT = 5000   # Porta dedicada exclusivamente à ingestão de dados
UDP_DISCOVERY_PORT = 5002   # Porta dedicada exclusivamente aos handshakes de topologia
TCP_PORT           = 5001

# [B5] Timeout para leitura de cabeçalho e payload TCP do cliente (configurável)
TCP_CLIENT_READ_TIMEOUT = max(5.0, float(os.getenv("TCP_CLIENT_READ_TIMEOUT", "10")))
TCP_CLIENT_IDLE_TIMEOUT = max(
    TCP_CLIENT_READ_TIMEOUT,
    float(os.getenv("TCP_CLIENT_IDLE_TIMEOUT", "60")),
)
TCP_MAX_FRAME_BYTES = max(1024, int(os.getenv("TCP_MAX_FRAME_BYTES", str(1024 * 1024))))

MULTICAST_GROUP          = "239.0.0.1"
DISCOVERY_PROBE_PAYLOAD  = b"SMARTCITY_DISCOVERY_PROBE"
DISCOVERY_PROBE_INTERVAL_SECS = max(1.0, float(os.getenv("DISCOVERY_PROBE_INTERVAL_SECS", "15")))
MULTICAST_TTL            = max(1, int(os.getenv("MULTICAST_TTL", "1")))
DEVICE_OFFLINE_TIMEOUT_SECS = max(1.0, float(os.getenv("DEVICE_OFFLINE_TIMEOUT_SECS", "45")))
DEVICE_OFFLINE_CHECK_INTERVAL_SECS = max(1.0, float(os.getenv("DEVICE_OFFLINE_CHECK_INTERVAL_SECS", "5")))

# ====================================================================
# INICIALIZAÇÃO DE BANCO DE DADOS
# ====================================================================

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
            await db.rollback()
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


def ensure_metrics_index(cursor: sqlite3.Cursor):
    """Garante que as consultas OLAP sobre métricas UDP usem o índice correto."""
    expected_columns = ["metric_name", "timestamp"]

    cursor.execute("""
        SELECT tbl_name
        FROM sqlite_master
        WHERE type = 'index' AND name = 'idx_metrics'
    """)
    row = cursor.fetchone()

    if row is not None:
        index_table = row[0]
        cursor.execute("PRAGMA index_info(idx_metrics)")
        current_columns = [info[2] for info in cursor.fetchall()]

        if index_table != "metrics" or current_columns != expected_columns:
            cursor.execute("DROP INDEX IF EXISTS idx_metrics")

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_metrics
        ON metrics (metric_name, timestamp)
    """)


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
    conn.commit()
    conn.close()

# ====================================================================
# WORKERS ASSÍNCRONOS DE PERSISTÊNCIA
# ====================================================================

async def process_telemetry(payload: messages_pb2.DataPayload, ip: str):
    """Persiste telemetria utilizando aiosqlite sem travar o Event Loop."""
    async with get_db_pool().connection() as db:
        # Garante a existência do nó (UPSERT/Resiliência para nós estritamente emissores)
        await db.execute("""
            INSERT OR IGNORE INTO devices (device_id, type, status, ip_address, is_controllable, last_seen)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (payload.device_id, 0, payload.current_status, ip, 0, int(time.time())))

        await db.execute("""
            UPDATE devices SET last_seen = ?, status = ? WHERE device_id = ?
        """, (int(time.time()), payload.current_status, payload.device_id))

        for m in payload.metrics:
            await db.execute("""
                INSERT INTO metrics (device_id, timestamp, metric_name, value, unit)
                VALUES (?, ?, ?, ?, ?)
            """, (payload.device_id, payload.timestamp, m.name, m.value, m.unit))

        await db.commit()

    log.debug("Ingestão concluída para '%s' (%d métricas).", payload.device_id, len(payload.metrics))


async def process_discovery(disc: messages_pb2.DiscoveryResponse, ip: str):
    """Registra descobrimento de nós operacionais assincronamente."""
    async with get_db_pool().connection() as db:
        await db.execute("""
            INSERT OR REPLACE INTO devices
            (device_id, type, status, ip_address, control_port, is_controllable, last_seen)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (disc.device_id, disc.type, disc.initial_status, ip,
              disc.control_port, int(disc.is_controllable), int(time.time())))
        await db.commit()

    log.info("Nó registrado: '%s' em %s:%d (controlável=%s).",
             disc.device_id, ip, disc.control_port, disc.is_controllable)


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

                LAST_MESSAGE_INFO[payload.device_id] = (payload.timestamp, payload.message_id)
                # Delega o I/O pesado de banco de dados para task background
                asyncio.create_task(process_telemetry(payload, addr[0]))
                log.debug(
                    "Pacote ID [%s] de '%s' — atraso %ds.",
                    payload.message_id, payload.device_id,
                    int(time.time()) - payload.timestamp,
                )
                return
        except Exception:
            pass


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
                asyncio.create_task(process_discovery(disc, addr[0]))
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
                    sock.sendto(DISCOVERY_PROBE_PAYLOAD, (MULTICAST_GROUP, UDP_TELEMETRY_PORT))
                    log.debug(
                        "Probe de descoberta enviado para %s:%d.",
                        MULTICAST_GROUP, UDP_TELEMETRY_PORT,
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
            async with get_db_pool().connection() as db:
                cursor = await db.execute("""
                    UPDATE devices
                    SET status = ?
                    WHERE last_seen < ? AND status != ?
                """, (messages_pb2.STATUS_OFF, cutoff, messages_pb2.STATUS_OFF))
                await db.commit()

            if cursor.rowcount > 0:
                log.warning(
                    "%d dispositivo(s) sem heartbeat por mais de %.1fs marcados como offline.",
                    cursor.rowcount, DEVICE_OFFLINE_TIMEOUT_SECS,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("Falha no monitor de presença dos dispositivos: %s", exc)

        await asyncio.sleep(DEVICE_OFFLINE_CHECK_INTERVAL_SECS)


# ====================================================================
# CAMADA DE REDE: ATENDIMENTO AO CLIENTE (TCP COM FRAMING)
# ====================================================================

def new_client_response(success: bool = True) -> messages_pb2.ClientResponse:
    resp = messages_pb2.ClientResponse(success=success)
    resp.message_id = f"GW-RESP-{uuid.uuid4().hex[:8]}"
    resp.timestamp  = int(time.time())
    return resp


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
            # Extrai timestamp (0), value (1) e device_id (2) em uma única passagem
            async with db.execute("""
                SELECT timestamp, value, device_id FROM metrics
                WHERE metric_name = ? AND timestamp BETWEEN ? AND ?
                ORDER BY timestamp ASC
            """, (req.query_metric, req.start_timestamp, req.end_timestamp)) as cursor:
                rows = await cursor.fetchall()
                vals = [r[1] for r in rows]

        if not vals:
            resp.success = False
            resp.message = "Janela de dados insuficiente para computação estatística."
        else:
            # Avaliação de complexidade O(n) sobre o vetor na memória 'vals'
            if req.query_op == messages_pb2.OP_AVERAGE:
                resp.analytics_result = sum(vals) / len(vals)

            elif req.query_op == messages_pb2.OP_STD_DEV:
                resp.analytics_result = statistics.stdev(vals) if len(vals) > 1 else 0.0

            elif req.query_op == messages_pb2.OP_MAX_VARIATION:
                # Hash map para agrupar leituras por dispositivo sem nova query SQL
                per_device: dict[str, list[float]] = {}
                for row in rows:
                    per_device.setdefault(row[2], []).append(row[1])

                if per_device:
                    # Expressão geradora calculando amplitude pico-a-pico por nó
                    max_var, best_dev = max(
                        ((max(v) - min(v), d) for d, v in per_device.items() if len(v) > 1),
                        default=(0.0, "N/A"),
                    )
                    resp.analytics_result = max_var
                    resp.result_metadata  = (
                        f"Maior variação: dispositivo {best_dev} — "
                        f"{len(per_device)} nós avaliados."
                    )
                else:
                    resp.success = False
                    resp.message = "Dados insuficientes para calcular variação por dispositivo."

            else:
                resp.success = False
                resp.message = f"Operação analítica desconhecida: {req.query_op}."

            # [B1] graph_points e result_metadata só populados em respostas bem-sucedidas
            if resp.success:
                for row in rows:
                    pt           = resp.graph_points.add()
                    pt.timestamp = row[0]
                    pt.value     = row[1]
                    pt.device_id = row[2]

                # [R2] fallback de metadata apenas quando não houve erro
                resp.result_metadata = (
                    resp.result_metadata
                    or f"Processados {len(vals)} vetores de dados computados."
                )

            log.info(
                "ANALYTICS_QUERY metric='%s' op=%d → %d pontos (sucesso=%s).",
                req.query_metric, req.query_op, len(vals), resp.success,
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
            except statistics.StatisticsError as exc:
                log.warning("Falha estatística processando requisição de %s: %s", peer, exc)
                resp = new_client_response(success=False)
                resp.message = f"Falha estatística no Gateway: {exc}"
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
    global DB_POOL

    log.info("============================================================")
    log.info("Inicializando nó central (aiosqlite / Framing Distribuído)...")
    log.info("LOG_LEVEL=%s | TCP_READ_TIMEOUT=%.1fs | TCP_IDLE_TIMEOUT=%.1fs | WAL_CHECKPOINT=%ds",
             _LOG_LEVEL_STR, TCP_CLIENT_READ_TIMEOUT, TCP_CLIENT_IDLE_TIMEOUT,
             WAL_CHECKPOINT_INTERVAL_SECS)
    log.info("OFFLINE_TIMEOUT=%.1fs | OFFLINE_CHECK=%.1fs",
             DEVICE_OFFLINE_TIMEOUT_SECS, DEVICE_OFFLINE_CHECK_INTERVAL_SECS)
    log.info("============================================================")

    init_db()
    DB_POOL = SQLiteConnectionPool(DB_FILE, DB_POOL_SIZE)
    await DB_POOL.start()

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

    log.info(
        "Hub pronto. TCP:%d | UDP(Telem):%d | UDP(Disc):%d",
        TCP_PORT, UDP_TELEMETRY_PORT, UDP_DISCOVERY_PORT,
    )

    try:
        async with server:
            await server.serve_forever()
    finally:
        probe_task.cancel()
        checkpoint_task.cancel()
        offline_task.cancel()
        await asyncio.gather(probe_task, checkpoint_task, offline_task, return_exceptions=True)
        telemetry_transport.close()
        discovery_transport.close()
        await DB_POOL.close()
        DB_POOL = None
        log.info("Gateway encerrado.")


if __name__ == "__main__":
    asyncio.run(main())
