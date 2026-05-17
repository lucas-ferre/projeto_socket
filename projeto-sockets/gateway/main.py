import asyncio
import sqlite3
import time
import statistics
import struct
import os
import uuid
from contextlib import asynccontextmanager
from typing import Tuple

# Biblioteca assíncrona para I/O não-bloqueante no SQLite
import aiosqlite 

DB_DIR = "db"
if not os.path.exists(DB_DIR):
    os.makedirs(DB_DIR)

DB_FILE = os.path.join(DB_DIR, "smartcity_gateway.db")
DB_POOL_SIZE = max(1, int(os.getenv("DB_POOL_SIZE", "4")))
DB_BUSY_TIMEOUT_MS = 10000

DB_POOL = None

# Armazena o último pacote processado por dispositivo para controle de sequência de DataPayload
LAST_MESSAGE_INFO = {}
# Armazena o último sequence number recebido por sensor_id em SensorData
LAST_SEQUENCE = {}

# Importa as classes do Protobuf geradas dinamicamente
import messages_pb2 # pyright: ignore[reportMissingImports]

# ====================================================================
# CONFIGURAÇÕES DE REDE
# ====================================================================

UDP_TELEMETRY_PORT = 5000  # Porta dedicada exclusivamente à ingestão de dados
UDP_DISCOVERY_PORT = 5002  # Porta dedicada exclusivamente aos handshakes de topologia
TCP_PORT = 5001
MULTICAST_GROUP = "239.0.0.1"

# ====================================================================
# INICIALIZAÇÃO DE BANCO DE DADOS
# ====================================================================

class SQLiteConnectionPool:
    """Pool simples para reutilizar conexões aiosqlite entre requisições."""

    def __init__(self, db_file: str, size: int, timeout: float = 10.0):
        self.db_file = db_file
        self.size = size
        self.timeout = timeout
        self._queue = asyncio.Queue(maxsize=size)
        self._connections = []
        self._started = False

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
        print(f"[Gateway:DB] Pool SQLite inicializado com {self.size} conexões.")

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
        print("[Gateway:DB] Pool SQLite encerrado.")


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
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Ativa o modo WAL (Write-Ahead Logging) para otimizar concorrência de leitura/escrita
    cursor.execute("PRAGMA journal_mode=WAL;")
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS devices (
            device_id TEXT PRIMARY KEY,
            type INTEGER,
            status INTEGER,
            ip_address TEXT,
            control_port INTEGER,
            is_controllable INTEGER,
            last_seen INTEGER
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id TEXT,
            timestamp INTEGER,
            metric_name TEXT,
            value REAL,
            unit TEXT
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
        """, (payload.device_id, 4, payload.current_status, ip, 0, int(time.time())))

        await db.execute("""
            UPDATE devices SET last_seen = ?, status = ? WHERE device_id = ?
        """, (int(time.time()), payload.current_status, payload.device_id))
        
        for m in payload.metrics:
            await db.execute("""
                INSERT INTO metrics (device_id, timestamp, metric_name, value, unit)
                VALUES (?, ?, ?, ?, ?)
            """, (payload.device_id, payload.timestamp, m.name, m.value, m.unit))
            
        await db.commit()
    print(f"[Gateway:Telemetria] Ingestão não-bloqueante concluída para '{payload.device_id}'.")

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
    print(f"[Gateway:Descoberta] Novo nó registrado: '{disc.device_id}' em {ip}:{disc.control_port}")


# ====================================================================
# CAMADA DE REDE: INGESTÃO E DESCOBERTA (MULTIPLEXAÇÃO FÍSICA UDP)
# ====================================================================

class TelemetryUDPProtocol(asyncio.DatagramProtocol):
    """Protocolo de transporte focado estritamente na ingestão contínua (Porta 5000)."""
    
    def connection_made(self, transport):
        self.transport = transport
        print(f"[Gateway:UDP] Interface de Telemetria ativa na porta {UDP_TELEMETRY_PORT}.")

    def datagram_received(self, data: bytes, addr: Tuple[str, int]):
        """Decodifica estritamente fluxos operacionais: SensorData ou DataPayload."""
        
        # 1. Tenta extrair datagramas de controle de Sequência
        try:
            msg = messages_pb2.SensorData()
            msg.ParseFromString(data)
            if msg.sensor_id:
                sensor_id = msg.sensor_id
                seq = msg.sequence
                if sensor_id in LAST_SEQUENCE:
                    expected = LAST_SEQUENCE[sensor_id] + 1
                    if seq != expected:
                        print(f"[Gateway:Sequência] perda detectada em '{sensor_id}': esperado {expected}, recebido {seq}")
                LAST_SEQUENCE[sensor_id] = seq
        except Exception:
            pass

        # 2. Tenta extrair datagramas de Telemetria (Métricas Físicas)
        try:
            payload = messages_pb2.DataPayload()
            payload.ParseFromString(data)
            
            # CORREÇÃO 1: Acesso correto aos enumeradores Protobuf exportados em módulo
            if payload.device_id and (payload.current_status != messages_pb2.STATUS_UNKNOWN or len(payload.metrics) > 0):
                last_info = LAST_MESSAGE_INFO.get(payload.device_id)
                if last_info is not None:
                    last_ts, last_msg_id = last_info
                    if payload.timestamp < last_ts:
                        print(f"[Gateway:Telemetria] Mensagem atrasada de '{payload.device_id}' (ts={payload.timestamp}) ignorada. Último ts={last_ts}.")
                        return
                    if payload.timestamp == last_ts and payload.message_id == last_msg_id:
                        print(f"[Gateway:Telemetria] Mensagem duplicada de '{payload.device_id}' ignorada.")
                        return

                LAST_MESSAGE_INFO[payload.device_id] = (payload.timestamp, payload.message_id)
                # Delegando o I/O pesado de banco de dados para a task background
                asyncio.create_task(process_telemetry(payload, addr[0]))
                print(f"[Gateway:Telemetria] Pacote ID [{payload.message_id}] recebido de '{payload.device_id}' com atraso de {int(time.time()) - payload.timestamp}s.")
                return
        except Exception:
            pass


class DiscoveryUDPProtocol(asyncio.DatagramProtocol):
    """Protocolo de transporte focado no registro de topologia (Porta 5002)."""

    def connection_made(self, transport):
        self.transport = transport
        print(f"[Gateway:UDP] Interface de Descoberta ativa na porta {UDP_DISCOVERY_PORT}.")

    def datagram_received(self, data: bytes, addr: Tuple[str, int]):
        """Decodifica estritamente fluxos de handshake e heartbeat."""
        try:
            disc = messages_pb2.DiscoveryResponse()
            disc.ParseFromString(data)
            
            # CORREÇÃO 3: Aceita dispositivos não-controláveis validando apenas o device_id
            if disc.device_id:
                asyncio.create_task(process_discovery(disc, addr[0]))
        except Exception as e:
            print(f"[Gateway:Erro] Falha na decodificação de datagrama de Descoberta {addr}: {e}")

# ====================================================================
# CAMADA DE REDE: ATENDIMENTO AO CLIENTE (TCP COM FRAMING)
# ====================================================================

async def handle_client_request(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    """Pipeline TCP totalmente assíncrono com Length-Prefix Framing na Camada de Aplicação."""
    try:
        # 1. Leitura estrita do cabeçalho de framing (Prefixo de Tamanho de 4 Bytes Inteiros)
        header = await reader.readexactly(4)
        msg_len = struct.unpack('>I', header)[0]
        
        # 2. Leitura exata da carga útil baseada no tamanho extraído
        data = await reader.readexactly(msg_len)
        
        req = messages_pb2.ClientRequest()
        req.ParseFromString(data)
        
        resp = messages_pb2.ClientResponse(success=True)
        resp.message_id = f"GW-RESP-{uuid.uuid4().hex[:8]}"
        resp.timestamp = int(time.time())
        
        # Rota: Sincronização de Inventário
        if req.type == messages_pb2.REQUEST_TYPE_LIST_DEVICES:
            async with get_db_pool().connection() as db:
                async with db.execute("SELECT * FROM devices") as cursor:
                    async for row in cursor:
                        d = resp.devices.add()
                        d.device_id, d.type, d.status, d.ip_address, d.control_port, is_ctrl, d.last_seen_timestamp = row
                        d.is_controllable = bool(is_ctrl)
            resp.message = "Sincronização de topologia extraída via pool aiosqlite."

        # Rota: Proxy de Atuação Remota
        elif req.type == messages_pb2.REQUEST_TYPE_SEND_COMMAND:
            async with get_db_pool().connection() as db:
                async with db.execute("SELECT ip_address, control_port FROM devices WHERE device_id = ?", (req.target_device_id,)) as cursor:
                    target = await cursor.fetchone()
                
            if target and target[1] > 0:
                try:
                    s_r, s_w = await asyncio.open_connection(target[0], target[1])
                    
                    # Aplica Framing no envio do comando para os Atuadores (Lua/Java)
                    req.command_payload.target_device_id = req.target_device_id
                    cmd_bytes = req.command_payload.SerializeToString()
                    s_w.write(struct.pack('>I', len(cmd_bytes)) + cmd_bytes)
                    await s_w.drain()
                    
                    # Lê a resposta binária do nó alvo respeitando a janela de Framing
                    s_header = await asyncio.wait_for(s_r.readexactly(4), timeout=5.0)
                    s_len = struct.unpack('>I', s_header)[0]
                    s_body = await asyncio.wait_for(s_r.readexactly(s_len), timeout=5.0)
                    
                    node_resp = messages_pb2.ConfigResponse()
                    node_resp.ParseFromString(s_body)
                    
                    resp.success, resp.message = node_resp.success, node_resp.message
                    s_w.close()
                    await s_w.wait_closed()
                except Exception as e:
                    resp.success, resp.message = False, f"Falha de I/O com o nó alvo: {e}"
            else:
                resp.success, resp.message = False, "Nó não encontrado ou desprovido de porta de controle."

        # Rota: Agregações Estatísticas (OLAP)
        elif req.type == messages_pb2.REQUEST_TYPE_ANALYTICS_QUERY:
            async with get_db_pool().connection() as db:
                async with db.execute("""
                    SELECT value FROM metrics 
                    WHERE metric_name = ? AND timestamp BETWEEN ? AND ?
                """, (req.query_metric, req.start_timestamp, req.end_timestamp)) as cursor:
                    rows = await cursor.fetchall()
                    vals = [r[0] for r in rows]

            if not vals:
                resp.success, resp.message = False, "Janela de dados insuficiente para computação estatística."
            else:
                if req.query_op == messages_pb2.OP_AVERAGE:
                    resp.analytics_result = sum(vals) / len(vals)
                elif req.query_op == messages_pb2.OP_STD_DEV:
                    resp.analytics_result = statistics.stdev(vals) if len(vals) > 1 else 0.0
                resp.result_metadata = f"Processados {len(vals)} vetores de dados computados."

        # 3. Transmissão da resposta empacotada com Length-Prefix Framing de volta ao cliente
        resp_bytes = resp.SerializeToString()
        writer.write(struct.pack('>I', len(resp_bytes)) + resp_bytes)
        await writer.drain()

    except asyncio.IncompleteReadError:
        print("[Gateway:TCP] Desconexão abrupta e falha de frame durante I/O de rede.")
    except Exception as e:
        print(f"[Gateway:TCP] Erro de engine de processamento analítico: {e}")
    finally:
        writer.close()
        await writer.wait_closed()

# ====================================================================
# INICIALIZAÇÃO DO LOOP DE EVENTOS E KERNEL DE BORDAS
# ====================================================================

async def main():
    global DB_POOL

    print("============================================================")
    print("[Gateway] Inicializando nó central (aiosqlite/Framing Distribuído)...")
    print("============================================================")
    init_db()
    DB_POOL = SQLiteConnectionPool(DB_FILE, DB_POOL_SIZE)
    await DB_POOL.start()
    
    loop = asyncio.get_running_loop()

    # Provisionando as instâncias de transporte baseadas na segregação de portas
    telemetry_transport, _ = await loop.create_datagram_endpoint(TelemetryUDPProtocol, local_addr=('0.0.0.0', UDP_TELEMETRY_PORT))
    discovery_transport, _ = await loop.create_datagram_endpoint(DiscoveryUDPProtocol, local_addr=('0.0.0.0', UDP_DISCOVERY_PORT))
    
    # Servidor de Controle TCP
    server = await asyncio.start_server(handle_client_request, '0.0.0.0', TCP_PORT)
    
    print(f"[Gateway] Hub multi-threaded pronto. TCP:{TCP_PORT} | UDP(Telem):{UDP_TELEMETRY_PORT} | UDP(Disc):{UDP_DISCOVERY_PORT}")
    try:
        async with server:
            await server.serve_forever()
    finally:
        telemetry_transport.close()
        discovery_transport.close()
        await DB_POOL.close()
        DB_POOL = None

if __name__ == "__main__":
    asyncio.run(main())
