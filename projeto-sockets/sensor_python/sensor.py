import os
import random
import signal
import socket
import struct
import threading
import time
import uuid
from contextlib import closing

import messages_pb2  # pyright: ignore[reportMissingImports]


GATEWAY_HOST = "gateway"
GATEWAY_TELEMETRY_PORT = 5000
GATEWAY_DISCOVERY_PORT = 5002

CONTROL_TCP_PORT = 5004
MULTICAST_GROUP = "239.0.0.1"
MULTICAST_PORT = 5005

SECTORS = (
    ("Pici", "pici"),
    ("Benfica", "benfica"),
    ("Porangabussu", "porangabussu"),
)
CAMERA_DEVICE_COUNT = max(1, int(os.getenv("CAMERA_DEVICE_COUNT", str(len(SECTORS)))))
DEVICE_HOSTNAME = "sensor_camera"


def env_float(name: str, default: float, min_value: float) -> float:
    raw_value = os.getenv(name, str(default))
    try:
        value = float(raw_value)
    except (TypeError, ValueError):
        print(f"[sensor_camera] | [Sensor Python:Config] {name}='{raw_value}' invalido. Usando {default}s.")
        return default

    if value < min_value:
        print(
            f"[sensor_camera] | [Sensor Python:Config] {name}='{raw_value}' abaixo do minimo "
            f"{min_value}s. Usando {default}s."
        )
        return default

    return value


MAX_FRAME_SIZE = 1024 * 1024
MAX_UDP_SEND_ATTEMPTS = 3
BASE_RETRY_DELAY_SECS = 0.2
MAX_RETRY_DELAY_SECS = 1.5
TELEMETRY_JITTER_SECS = 0.35
DISCOVERY_PROBE_JITTER_SECS = 2.0
HEARTBEAT_INTERVAL_SECS = env_float("SENSOR_HEARTBEAT_INTERVAL_SECS", 10.0, 1.0)
HEARTBEAT_JITTER_SECS = env_float("SENSOR_HEARTBEAT_JITTER_SECS", 2.0, 0.0)
MANUAL_OVERRIDE_SECS = 30.0
THRESHOLD_SCAN_INTERVAL_SECS = 1.0
THRESHOLD_EVENT_COOLDOWN_SECS = 3.0
TRAFFIC_VEHICLES_THRESHOLD = int(os.getenv("TRAFFIC_VEHICLES_THRESHOLD", "80"))
TRAFFIC_INFRACTIONS_THRESHOLD = int(os.getenv("TRAFFIC_INFRACTIONS_THRESHOLD", "3"))

shutdown_event = threading.Event()
state_lock = threading.Lock()


def build_device_fleet(count: int) -> dict[str, dict]:
    devices = {}
    for idx in range(count):
        sector_name, sector_slug = SECTORS[idx % len(SECTORS)]
        sector_ordinal = (idx // len(SECTORS)) + 1
        device_id = f"camera_{sector_slug}_{sector_ordinal:02d}"
        devices[device_id] = {
            "device_id": device_id,
            "sector": sector_name,
            "status": messages_pb2.STATUS_ON,
            "frequency_secs": 5,
            "next_send_at": 0.0,
            "next_threshold_check_at": 0.0,
            "last_threshold_send_at": 0.0,
            "manual_until": 0.0,
        }
    return devices


DEVICES = build_device_fleet(CAMERA_DEVICE_COUNT)
DEVICE_ID = next(iter(DEVICES))
DEVICE_SECTOR = DEVICES[DEVICE_ID]["sector"]


def status_name(status: int) -> str:
    return messages_pb2.DeviceStatus.Name(status)


def random_device_status() -> int:
    return random.choices(
        [messages_pb2.STATUS_ON, messages_pb2.STATUS_OFF, messages_pb2.STATUS_ERROR],
        weights=[0.78, 0.12, 0.10],
        k=1,
    )[0]


def retry_delay_secs(attempt: int) -> float:
    backoff = min(MAX_RETRY_DELAY_SECS, BASE_RETRY_DELAY_SECS * (2 ** attempt))
    return backoff + random.uniform(0, BASE_RETRY_DELAY_SECS)


def telemetry_wait_secs(frequency_secs: int) -> float:
    return max(1.0, float(frequency_secs) + random.uniform(0, TELEMETRY_JITTER_SECS))


def discovery_probe_jitter_secs() -> float:
    return random.uniform(0.0, DISCOVERY_PROBE_JITTER_SECS)


def heartbeat_wait_secs() -> float:
    return HEARTBEAT_INTERVAL_SECS + random.uniform(0.0, HEARTBEAT_JITTER_SECS)


def send_udp_message(message, port: int) -> None:
    data = message.SerializeToString()
    last_error = None

    for attempt in range(MAX_UDP_SEND_ATTEMPTS):
        try:
            with closing(socket.socket(socket.AF_INET, socket.SOCK_DGRAM)) as sock:
                sock.settimeout(1.0)
                sock.sendto(data, (GATEWAY_HOST, port))
            return
        except OSError as exc:
            last_error = exc
            if attempt == MAX_UDP_SEND_ATTEMPTS - 1:
                break

            delay = retry_delay_secs(attempt)
            print(
                f"[Sensor Python:Retry] Falha UDP porta {port} "
                f"(tentativa {attempt + 1}/{MAX_UDP_SEND_ATTEMPTS}): {exc}. "
                f"Retry em {delay:.2f}s."
            )
            shutdown_event.wait(delay)

    raise last_error or OSError("falha desconhecida no envio UDP")


def device_ids_for_discovery(target_device_id: str | None = None) -> list[str]:
    with state_lock:
        if target_device_id:
            return [target_device_id] if target_device_id in DEVICES else []
        return list(DEVICES.keys())


def send_discovery_response(target_device_id: str | None = None) -> None:
    for device_id in device_ids_for_discovery(target_device_id):
        with state_lock:
            device = DEVICES[device_id]
            current_status = device["status"]
            sector = device["sector"]

        discovery = messages_pb2.DiscoveryResponse(
            message_id=f"DISC-{uuid.uuid4().hex[:8]}",
            timestamp=int(time.time()),
            device_id=device_id,
            type=messages_pb2.DEVICE_TYPE_CAMERA,
            ip_address=DEVICE_HOSTNAME,
            control_port=CONTROL_TCP_PORT,
            initial_status=current_status,
            is_controllable=True,
        )

        try:
            send_udp_message(discovery, GATEWAY_DISCOVERY_PORT)
            print(
                f"[sensor_camera] | [Sensor Python:Descoberta] Dispositivo={device_id} | Setor={sector} | "
                f"Status={status_name(current_status)} | Handshake de presença injetado via porta {GATEWAY_DISCOVERY_PORT}."
            )
        except OSError as exc:
            print(f"[sensor_camera] | [Sensor Python:Erro] Falha ao enviar descoberta de {device_id}: {exc}")


def build_traffic_metrics() -> list[messages_pb2.Metric]:
    vehicles_count = random.randint(12, 95)

    rush_factor = 1.0 if vehicles_count < 55 else 1.8
    infraction_rate = 0.025 * rush_factor
    infractions = sum(1 for _ in range(vehicles_count) if random.random() < infraction_rate)

    return [
        messages_pb2.Metric(
            name="vehicles_count",
            value=float(vehicles_count),
            unit="veh/min",
        ),
        messages_pb2.Metric(
            name="infractions",
            value=float(infractions),
            unit="count",
        ),
    ]


def traffic_threshold_reason(metrics: list[messages_pb2.Metric]) -> str | None:
    metric_map = {metric.name: metric.value for metric in metrics}
    reasons = []

    vehicles_count = metric_map.get("vehicles_count", 0.0)
    infractions = metric_map.get("infractions", 0.0)

    if vehicles_count >= TRAFFIC_VEHICLES_THRESHOLD:
        reasons.append(f"vehicles_count={vehicles_count:.0f} >= {TRAFFIC_VEHICLES_THRESHOLD}")
    if infractions >= TRAFFIC_INFRACTIONS_THRESHOLD:
        reasons.append(f"infractions={infractions:.0f} >= {TRAFFIC_INFRACTIONS_THRESHOLD}")

    return "; ".join(reasons) if reasons else None


def prepare_device_for_telemetry(device_id: str) -> tuple[str, str, int] | None:
    now = time.monotonic()

    with state_lock:
        device = DEVICES.get(device_id)
        if not device:
            return None

        if now >= device["manual_until"]:
            device["status"] = random_device_status()

        device["next_send_at"] = now + telemetry_wait_secs(device["frequency_secs"])
        return device["device_id"], device["sector"], device["status"]


def emit_telemetry_payload(
    current_device_id: str,
    sector: str,
    current_status: int,
    metrics: list[messages_pb2.Metric],
    trigger_reason: str | None = None,
) -> None:
    payload = messages_pb2.DataPayload(
        message_id=f"{current_device_id}-{int(time.time())}-{uuid.uuid4().hex[:6]}",
        timestamp=int(time.time()),
        device_id=current_device_id,
        current_status=current_status,
    )
    payload.metrics.extend(metrics)

    try:
        send_udp_message(payload, GATEWAY_TELEMETRY_PORT)
        if payload.metrics:
            vehicles = payload.metrics[0].value
            infractions = payload.metrics[1].value
            event_label = "Evento por limiar" if trigger_reason else "Telemetria injetada"
            print(
                f"[sensor_camera] | [Sensor Python:UDP] {event_label} | "
                f"Dispositivo={current_device_id} | Setor={sector} | Status={status_name(current_status)} | "
                f"Veiculos={vehicles:.0f}/min | Infracoes={infractions:.0f}"
                + (f" | Limiar={trigger_reason}" if trigger_reason else "")
            )
        else:
            print(
                f"[sensor_camera] | [Sensor Python:UDP] Heartbeat operacional | "
                f"Dispositivo={current_device_id} | Setor={sector} | Status={status_name(current_status)}"
            )
    except OSError as exc:
        print(f"[sensor_camera] | [Sensor Python:Erro] Falha ao enviar telemetria de {current_device_id}: {exc}")


def send_telemetry_payload(device_id: str) -> None:
    snapshot = prepare_device_for_telemetry(device_id)
    if snapshot is None:
        return

    current_device_id, sector, current_status = snapshot
    metrics = build_traffic_metrics() if current_status == messages_pb2.STATUS_ON else []
    emit_telemetry_payload(current_device_id, sector, current_status, metrics)


def send_threshold_telemetry_payload(device_id: str) -> None:
    now = time.monotonic()
    with state_lock:
        device = DEVICES.get(device_id)
        if not device or now < device["next_threshold_check_at"]:
            return

        device["next_threshold_check_at"] = now + THRESHOLD_SCAN_INTERVAL_SECS
        if (
            device["status"] != messages_pb2.STATUS_ON
            or now - device["last_threshold_send_at"] < THRESHOLD_EVENT_COOLDOWN_SECS
        ):
            return

        current_device_id = device["device_id"]
        sector = device["sector"]
        current_status = device["status"]

    metrics = build_traffic_metrics()
    trigger_reason = traffic_threshold_reason(metrics)
    if trigger_reason is None:
        return

    with state_lock:
        device = DEVICES.get(device_id)
        if not device or device["status"] != messages_pb2.STATUS_ON:
            return
        current_now = time.monotonic()
        if current_now - device["last_threshold_send_at"] < THRESHOLD_EVENT_COOLDOWN_SECS:
            return
        device["last_threshold_send_at"] = current_now

    emit_telemetry_payload(current_device_id, sector, current_status, metrics, trigger_reason)


def recv_exact(sock: socket.socket, size: int) -> bytes:
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise ConnectionError("conexao encerrada antes do frame completo")
        data.extend(chunk)
    return bytes(data)


def handle_control_client(conn: socket.socket, addr) -> None:
    try:
        conn.settimeout(5.0)

        header = recv_exact(conn, 4)
        frame_size = struct.unpack(">I", header)[0]
        if frame_size <= 0 or frame_size > MAX_FRAME_SIZE:
            raise ValueError(f"tamanho de frame invalido: {frame_size}")

        body = recv_exact(conn, frame_size)
        command = messages_pb2.ConfigCommand()
        command.ParseFromString(body)

        target_device_id = command.target_device_id or DEVICE_ID
        with state_lock:
            device = DEVICES.get(target_device_id)
            if not device:
                raise ValueError(f"dispositivo alvo desconhecido: {target_device_id}")

            if command.update_status:
                device["status"] = command.target_status
                device["manual_until"] = time.monotonic() + MANUAL_OVERRIDE_SECS

            if command.update_frequency and command.new_frequency_secs > 0:
                device["frequency_secs"] = command.new_frequency_secs

            device["next_send_at"] = 0.0
            current_status = device["status"]
            current_frequency = device["frequency_secs"]
            sector = device["sector"]

        print(
            f"[sensor_camera] | [Sensor Python:TCP] Comando {command.command_id or '<sem-id>'} recebido | "
            f"Dispositivo={target_device_id} | Setor={sector} | Status={status_name(current_status)} | Frequencia={current_frequency}s"
        )

        response = messages_pb2.ConfigResponse(
            message_id=f"ACK-{uuid.uuid4().hex[:8]}",
            command_id=command.command_id,
            timestamp=int(time.time()),
            success=True,
            message=f"Camera Python {target_device_id} reconfigurada com sucesso.",
            updated_status=current_status,
            updated_frequency_secs=current_frequency,
        )
        response_bytes = response.SerializeToString()
        conn.sendall(struct.pack(">I", len(response_bytes)) + response_bytes)

        send_discovery_response(target_device_id)

    except Exception as exc:
        print(f"[sensor_camera] | [Sensor Python:Erro] Falha no pipeline TCP: {exc}")
        try:
            response = messages_pb2.ConfigResponse(
                message_id=f"ERR-{uuid.uuid4().hex[:8]}",
                timestamp=int(time.time()),
                success=False,
                message=f"Falha ao aplicar comando na camera: {exc}",
            )
            response_bytes = response.SerializeToString()
            conn.sendall(struct.pack(">I", len(response_bytes)) + response_bytes)
        except OSError:
            pass
    finally:
        conn.close()


def control_server_loop() -> None:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("0.0.0.0", CONTROL_TCP_PORT))
        server.listen()
        server.settimeout(1.0)
        print(f"[sensor_camera] | [Sensor Python:TCP] Interface de controle ativa na porta {CONTROL_TCP_PORT}.")

        while not shutdown_event.is_set():
            try:
                conn, addr = server.accept()
            except socket.timeout:
                continue
            except OSError as exc:
                if not shutdown_event.is_set():
                    print(f"[sensor_camera] | [Sensor Python:Erro] Falha no accept TCP: {exc}")
                continue

            threading.Thread(
                target=handle_control_client,
                args=(conn, addr),
                daemon=True,
            ).start()


def multicast_listener_loop() -> None:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("", MULTICAST_PORT))

        membership = struct.pack(
            "4s4s",
            socket.inet_aton(MULTICAST_GROUP),
            socket.inet_aton("0.0.0.0"),
        )
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membership)
        sock.settimeout(1.0)

        print(
            f"[sensor_camera] | [Sensor Python:Multicast] Escutando probes em "
            f"{MULTICAST_GROUP}:{MULTICAST_PORT}."
        )

        while not shutdown_event.is_set():
            try:
                data, addr = sock.recvfrom(256)
            except socket.timeout:
                continue
            except OSError as exc:
                if not shutdown_event.is_set():
                    print(f"[sensor_camera] | [Sensor Python:Erro] Falha no multicast: {exc}")
                continue

            if data == b"SMARTCITY_DISCOVERY_PROBE":
                delay = discovery_probe_jitter_secs()
                print(
                    f"[sensor_camera] | [Sensor Python:Multicast] Probe recebido de {addr[0]}. "
                    f"Reenviando descoberta da frota em {delay * 1000:.0f} ms."
                )
                if shutdown_event.wait(delay):
                    continue
                send_discovery_response()


def heartbeat_loop() -> None:
    while not shutdown_event.wait(heartbeat_wait_secs()):
        print(
            f"[sensor_camera] | [Sensor Python:Heartbeat] Renovando presença da frota "
            f"via DiscoveryResponse."
        )
        send_discovery_response()


def handle_shutdown_signal(signum, frame) -> None:
    shutdown_event.set()


def due_device_ids() -> list[str]:
    now = time.monotonic()
    with state_lock:
        return [
            device_id
            for device_id, device in DEVICES.items()
            if now >= device["next_send_at"]
        ]


def threshold_due_device_ids() -> list[str]:
    now = time.monotonic()
    with state_lock:
        return [
            device_id
            for device_id, device in DEVICES.items()
            if now >= device["next_threshold_check_at"]
        ]


def main() -> None:
    random.seed(time.time_ns())

    signal.signal(signal.SIGTERM, handle_shutdown_signal)
    signal.signal(signal.SIGINT, handle_shutdown_signal)

    print("============================================================")
    print(f"[sensor_camera] | [Sensor Python] Inicializando frota de {len(DEVICES)} cameras de trafego.")
    for device in DEVICES.values():
        print(f"[sensor_camera] | Dispositivo={device['device_id']} | Setor={device['sector']}")
    print("[sensor_camera] | Metricas: vehicles_count, infractions")
    print("============================================================")

    threading.Thread(target=control_server_loop, daemon=True).start()
    threading.Thread(target=multicast_listener_loop, daemon=True).start()
    threading.Thread(target=heartbeat_loop, daemon=True).start()

    send_discovery_response()

    while not shutdown_event.is_set():
        for device_id in threshold_due_device_ids():
            send_threshold_telemetry_payload(device_id)

        for device_id in due_device_ids():
            send_telemetry_payload(device_id)

        shutdown_event.wait(0.2)

    print("[sensor_camera] | [Sensor Python] Encerramento solicitado. Finalizando processo.")


if __name__ == "__main__":
    main()
