local socket = require("socket")
local pb     = require("pb")

print("============================================================")
print("[Sensor Lua] Inicializando Poste Inteligente (Arquitetura Multiplexada)...")
print("============================================================")

math.randomseed(os.time())
for _ = 1, 3 do math.random() end

assert(pb.loadfile("messages.pb"), "[Sensor Lua:Erro] messages.pb não encontrado no File System.")

-- ====================================================================
-- CONFIGURAÇÕES GERAIS E DE ROTEAMENTO (CAMADA DE TRANSPORTE)
-- ====================================================================

local SECTORS = {
    { name = "Pici",         slug = "pici"         },
    { name = "Benfica",      slug = "benfica"      },
    { name = "Porangabussu", slug = "porangabussu" }
}

local CONTROL_TCP_PORT            = 5006
local GATEWAY_HOST                = "gateway"
local GATEWAY_TELEMETRY_PORT      = 5000
local GATEWAY_UDP_DISCOVERY_PORT  = 5002
local MULTICAST_GROUP             = "239.0.0.1"
local MULTICAST_PORT              = 5005
local UDP_MAX_RETRIES             = 3
local RETRY_BASE_DELAY            = 0.20
local RETRY_MAX_DELAY             = 1.50
local TELEMETRY_JITTER_SECS       = 0.35
local DISCOVERY_PROBE_JITTER_SECS = 2.0
local HEARTBEAT_INTERVAL_SECS     = math.max(1.0, tonumber(os.getenv("SENSOR_HEARTBEAT_INTERVAL_SECS") or "10") or 10.0)
local HEARTBEAT_JITTER_SECS       = math.max(0.0, tonumber(os.getenv("SENSOR_HEARTBEAT_JITTER_SECS")  or "2")  or 2.0)
local MAX_TCP_FRAME_BYTES         = 1024 * 1024
local MANUAL_OVERRIDE_SECS        = 30.0
local THRESHOLD_SCAN_INTERVAL_SECS   = 1.0
local THRESHOLD_EVENT_COOLDOWN_SECS  = 3.0
local LUMINOSITY_LOW_THRESHOLD    = tonumber(os.getenv("LUMINOSITY_LOW_THRESHOLD")    or "80")
local POWER_CONSUMPTION_THRESHOLD = tonumber(os.getenv("POWER_CONSUMPTION_THRESHOLD") or "32")
local DEVICE_COUNT                = tonumber(os.getenv("LUA_DEVICE_COUNT") or tostring(#SECTORS))

-- [Melhoria 5] Timeout TCP configurável e menor (era 5.0s fixo)
-- Reduz o tempo máximo que uma conexão lenta monopoliza o loop principal.
local TCP_READ_TIMEOUT_SECS = math.max(0.5, tonumber(os.getenv("TCP_READ_TIMEOUT_SECS") or "2.0") or 2.0)

-- [Melhoria 1] Parâmetros da fila de retransmissão UDP não-bloqueante
local UDP_RETRY_QUEUE_MAX  = 32  -- descarta novos enfileiramentos quando lotada
local UDP_RETRY_BATCH_SIZE = 4   -- tentativas de reenvio processadas por iteração

-- [Melhoria 2 / 3] Limites de drenagem por iteração
local TCP_ACCEPT_BATCH_SIZE  = 4  -- conexões TCP aceitas por ciclo
local MULTICAST_BATCH_SIZE   = 8  -- datagramas multicast lidos por ciclo

-- [Melhoria 4] Fila de respostas de descoberta pendentes
local MAX_PENDING_DISCOVERIES = 10

local devices      = {}
local device_order = {}

for idx = 1, DEVICE_COUNT do
    local sector         = SECTORS[((idx - 1) % #SECTORS) + 1]
    local sector_ordinal = math.floor((idx - 1) / #SECTORS) + 1
    local device_id      = string.format("poste_%s_%02d", sector.slug, sector_ordinal)

    devices[device_id] = {
        device_id            = device_id,
        sector               = sector.name,
        status               = "STATUS_ON",
        frequency_secs       = 5,
        last_udp_send        = 0,
        next_jitter_secs     = math.random() * TELEMETRY_JITTER_SECS,
        next_threshold_check = 0,
        last_threshold_send  = 0,
        manual_until         = 0
    }
    table.insert(device_order, device_id)
    print(string.format("[Sensor Lua:Identidade] Nó provisionado com ID: %s | Setor: %s",
          device_id, sector.name))
end

local DEFAULT_DEVICE_ID = device_order[1]

-- ====================================================================
-- RESOLUÇÃO DNS SOB DEMANDA
-- ====================================================================

print(string.format(
    "[sensor_posto] | [Sensor Lua:DNS] Gateway '%s' será resolvido durante cada envio UDP.",
    GATEWAY_HOST))

-- ====================================================================
-- INICIALIZAÇÃO DE DESCRITORES DE REDE (SOCKETS POSIX BOUND)
-- ====================================================================

local udp_out = socket.udp()
udp_out:settimeout(0)

local udp_mc = socket.udp()
udp_mc:settimeout(0)
udp_mc:setoption("reuseaddr", true)
assert(udp_mc:setsockname("0.0.0.0", MULTICAST_PORT))
assert(udp_mc:setoption("ip-add-membership", { multiaddr = MULTICAST_GROUP, interface = "0.0.0.0" }))
print(string.format("[Sensor Lua:Multicast] Interface vinculada ao grupo %s:%d",
      MULTICAST_GROUP, MULTICAST_PORT))

local tcp_server = assert(socket.bind("0.0.0.0", CONTROL_TCP_PORT))
tcp_server:settimeout(0)
print(string.format("[Sensor Lua:TCP] Interface de controle provisionada na porta %d.", CONTROL_TCP_PORT))

-- ====================================================================
-- [Melhoria 7] FILA FIFO O(1) — estrutura auxiliar compartilhada
--
-- table.remove(t, 1) é O(n): desloca todos os elementos a cada dequeue.
-- Esta implementação usa ponteiros head/tail sobre uma tabela esparsa,
-- garantindo enqueue e dequeue em O(1) amortizado.
--
-- Nota: os índices numéricos crescem indefinidamente, mas para as filas
-- usadas neste sensor (< 32 entradas simultâneas) o overhead é irrelevante.
-- ====================================================================

local function new_queue()
    return { data = {}, head = 1, tail = 0 }
end

local function enqueue(q, item)
    q.tail = q.tail + 1
    q.data[q.tail] = item
end

local function dequeue(q)
    if q.head > q.tail then return nil end
    local item = q.data[q.head]
    q.data[q.head] = nil   -- libera referência para o GC
    q.head = q.head + 1
    return item
end

local function queue_peek(q)
    return q.data[q.head]
end

local function queue_empty(q)
    return q.head > q.tail
end

local function queue_size(q)
    return math.max(0, q.tail - q.head + 1)
end

-- Filas globais (dependem das helpers acima)
local udp_retry_queue    = new_queue()  -- reenvios UDP pendentes  [Melhoria 1]
local pending_discoveries = new_queue() -- respostas de probe pendentes [Melhoria 4]

-- ====================================================================
-- UTILITÁRIOS DE PROTOCOLO
-- ====================================================================

local function retry_delay(attempt)
    local backoff = math.min(RETRY_MAX_DELAY, RETRY_BASE_DELAY * (2 ^ (attempt - 1)))
    return backoff + (math.random() * RETRY_BASE_DELAY)
end

local function discovery_probe_jitter()
    return math.random() * DISCOVERY_PROBE_JITTER_SECS
end

local function heartbeat_delay()
    return HEARTBEAT_INTERVAL_SECS + (math.random() * HEARTBEAT_JITTER_SECS)
end

local function udp_error_text(err)
    return err and tostring(err) or "falha desconhecida"
end

local function random_device_status()
    local roll = math.random(1, 100)
    if roll <= 78 then return "STATUS_ON"  end
    if roll <= 90 then return "STATUS_OFF" end
    return "STATUS_ERROR"
end

-- ====================================================================
-- [Melhoria 1] ENVIO UDP NÃO-BLOQUEANTE com fila de retransmissão
--
-- PROBLEMA ORIGINAL:
--   send_udp_with_retry() chamava socket.sleep() entre tentativas,
--   congelando telemetria, heartbeat, comandos TCP e multicast enquanto
--   o gateway estava inacessível.
--
-- CORREÇÃO — fila de retransmissão (udp_retry_queue):
--   A primeira tentativa é feita imediatamente. Em caso de falha, o
--   pacote é enfileirado com o timestamp da próxima tentativa. A função
--   retorna sem bloquear. process_udp_retry_queue() drena a fila no
--   início de cada iteração do loop principal, sem afetar os demais
--   canais (TCP, multicast, telemetria periódica).
-- ====================================================================

local function send_udp_nonblocking(bytes, host, port, channel)
    local ok, err = udp_out:sendto(bytes, host, port)
    if ok then return true end

    err = udp_error_text(err)
    local delay = retry_delay(1)

    if queue_size(udp_retry_queue) < UDP_RETRY_QUEUE_MAX then
        enqueue(udp_retry_queue, {
            bytes         = bytes,
            host          = host,
            port          = port,
            channel       = channel,
            attempts      = 1,
            next_retry_at = socket.gettime() + delay
        })
        print(string.format(
            "[sensor_posto] | [Sensor Lua:Retry] Falha UDP porta %d (tentativa 1/%d): %s. Retry em %.2fs.",
            port, UDP_MAX_RETRIES, err, delay))
    else
        print(string.format(
            "[sensor_posto] | [Sensor Lua:Retry] Falha UDP porta %d (tentativa 1/%d): %s. Fila de retry saturada; pacote %s descartado.",
            port, UDP_MAX_RETRIES, err, channel))
    end
    return false, err
end

local function process_udp_retry_queue(current_time)
    local processed = 0
    while not queue_empty(udp_retry_queue) and processed < UDP_RETRY_BATCH_SIZE do
        local entry = queue_peek(udp_retry_queue)
        -- A fila é aproximadamente FIFO por tempo: se o head não está pronto,
        -- entradas posteriores também não estão (mesma base de backoff).
        if current_time < entry.next_retry_at then break end

        dequeue(udp_retry_queue)
        processed = processed + 1
        local attempt_number = entry.attempts + 1

        local ok, err = udp_out:sendto(entry.bytes, entry.host, entry.port)
        if ok then
            print(string.format(
                "[sensor_posto] | [Sensor Lua:Retry] %s retransmitido com sucesso (tentativa %d/%d).",
                entry.channel, attempt_number, UDP_MAX_RETRIES))
        else
            err = udp_error_text(err)
            if attempt_number >= UDP_MAX_RETRIES then
                print(string.format(
                    "[sensor_posto] | [Sensor Lua:Retry] Falha UDP porta %d (tentativa %d/%d): %s. Todas as tentativas esgotadas.",
                    entry.port, attempt_number, UDP_MAX_RETRIES, err))
            else
                local delay = retry_delay(attempt_number)
                entry.attempts      = attempt_number
                entry.next_retry_at = current_time + delay
                if queue_size(udp_retry_queue) < UDP_RETRY_QUEUE_MAX then
                    enqueue(udp_retry_queue, entry)
                    print(string.format(
                        "[sensor_posto] | [Sensor Lua:Retry] Falha UDP porta %d (tentativa %d/%d): %s. Retry em %.2fs.",
                        entry.port, attempt_number, UDP_MAX_RETRIES, err, delay))
                else
                    print(string.format(
                        "[sensor_posto] | [Sensor Lua:Retry] Falha UDP porta %d (tentativa %d/%d): %s. Fila de retry saturada; pacote %s descartado.",
                        entry.port, attempt_number, UDP_MAX_RETRIES, err, entry.channel))
                end
            end
        end
    end
end

-- ====================================================================
-- recv_exact — Leitura TCP com garantia de N bytes (loop acumulador)
-- ====================================================================

local function recv_exact(client, n)
    local chunks   = {}
    local received = 0

    while received < n do
        local remaining           = n - received
        local chunk, err, partial = client:receive(remaining)

        if chunk then
            table.insert(chunks, chunk)
            received = received + #chunk

        elseif partial and #partial > 0 then
            table.insert(chunks, partial)
            received = received + #partial
            if err ~= "timeout" then break end

        else
            return nil, string.format(
                "recv_exact: erro após %d/%d bytes — %s",
                received, n, err or "conexão encerrada")
        end
    end

    if received < n then
        return nil, string.format(
            "recv_exact: frame incompleto — %d/%d bytes recebidos", received, n)
    end

    return table.concat(chunks)
end

-- ====================================================================
-- ROTINAS DE PROTOCOLO E SERIALIZAÇÃO BINÁRIA
-- ====================================================================

local function send_discovery_response(target_device_id)
    local ids = target_device_id and { target_device_id } or device_order

    for _, device_id in ipairs(ids) do
        local device = devices[device_id]
        if device then
            local msg = {
                message_id      = string.format("DISC-%s-%d", device.device_id, os.time()),
                timestamp       = os.time(),
                device_id       = device.device_id,
                type            = "DEVICE_TYPE_LAMP_POST",
                ip_address      = "sensor_posto",
                control_port    = CONTROL_TCP_PORT,
                initial_status  = device.status,
                is_controllable = true
            }
            local bytes = assert(pb.encode("smartcity.DiscoveryResponse", msg))
            local ok, err = send_udp_nonblocking(bytes, GATEWAY_HOST, GATEWAY_UDP_DISCOVERY_PORT, "Descoberta")
            if ok then
                print(string.format(
                    "[Sensor Lua:Descoberta] Dispositivo=%s | Setor=%s | Status=%s | Porta=%d.",
                    device.device_id, device.sector, device.status, GATEWAY_UDP_DISCOVERY_PORT))
            else
                print(string.format(
                    "[sensor_posto] | [Sensor Lua:Erro] Falha ao enviar descoberta de %s: %s",
                    device.device_id, udp_error_text(err)))
            end
        end
    end
end

local function build_lamp_metrics()
    return {
        { name = "luminosity",        value = math.random(75, 100),          unit = "%" },
        { name = "power_consumption", value = 25.0 + (math.random() * 10.0), unit = "W" }
    }
end

local function lamp_threshold_reason(metrics)
    local reasons = {}
    if metrics[1] and metrics[1].value <= LUMINOSITY_LOW_THRESHOLD then
        table.insert(reasons, string.format("luminosity=%.0f <= %.0f",
            metrics[1].value, LUMINOSITY_LOW_THRESHOLD))
    end
    if metrics[2] and metrics[2].value >= POWER_CONSUMPTION_THRESHOLD then
        table.insert(reasons, string.format("power_consumption=%.1f >= %.1f",
            metrics[2].value, POWER_CONSUMPTION_THRESHOLD))
    end
    if #reasons == 0 then return nil end
    return table.concat(reasons, "; ")
end

local function send_metrics_payload(device, trigger_reason, metrics_override)
    if not trigger_reason and socket.gettime() >= device.manual_until then
        device.status = random_device_status()
    end

    local current_time = os.time()
    local msg_id = string.format("%s-%d-%04d",
                                 device.device_id, current_time, math.random(1, 9999))
    local metrics = {}
    if device.status == "STATUS_ON" then
        metrics = metrics_override or build_lamp_metrics()
    end

    local payload = {
        message_id     = msg_id,
        timestamp      = current_time,
        device_id      = device.device_id,
        current_status = device.status,
        metrics        = metrics
    }
    local bytes = assert(pb.encode("smartcity.DataPayload", payload))
    local ok, err = send_udp_nonblocking(bytes, GATEWAY_HOST, GATEWAY_TELEMETRY_PORT, "Telemetria")

    if ok and device.status == "STATUS_ON" then
        local label = trigger_reason and "Evento por limiar" or "Telemetria injetada"
        print(string.format(
            "[Sensor Lua:UDP] %s | Dispositivo=%s | Setor=%s | Luminosidade: %d%% | Consumo: %.1f W%s",
            label, device.device_id, device.sector,
            payload.metrics[1].value, payload.metrics[2].value,
            trigger_reason and (" | Limiar=" .. trigger_reason) or ""))
    elseif ok then
        print(string.format("[Sensor Lua:UDP] Heartbeat | Dispositivo=%s | Status=%s",
              device.device_id, device.status))
    else
        print(string.format(
            "[sensor_posto] | [Sensor Lua:Erro] Falha ao enviar telemetria de %s: %s",
            device.device_id, udp_error_text(err)))
    end
end

local function poll_threshold_events(current_time)
    for _, device_id in ipairs(device_order) do
        local device = devices[device_id]
        if device
           and current_time >= device.next_threshold_check
           and device.status == "STATUS_ON"
           and (current_time - device.last_threshold_send) >= THRESHOLD_EVENT_COOLDOWN_SECS then

            device.next_threshold_check = current_time + THRESHOLD_SCAN_INTERVAL_SECS
            local metrics        = build_lamp_metrics()
            local trigger_reason = lamp_threshold_reason(metrics)
            if trigger_reason then
                device.last_threshold_send = current_time
                send_metrics_payload(device, trigger_reason, metrics)
            end
        elseif device and current_time >= device.next_threshold_check then
            device.next_threshold_check = current_time + THRESHOLD_SCAN_INTERVAL_SECS
        end
    end
end

-- ====================================================================
-- [Melhoria 2] Processamento de múltiplas conexões TCP por ciclo
--
-- PROBLEMA ORIGINAL:
--   Uma única tcp_server:accept() por iteração deixava conexões
--   acumulando no backlog durante ciclos de processamento longos.
--
-- CORREÇÃO:
--   handle_control_command() foi extraída como função independente.
--   poll_control_commands() drena o backlog em loop (até TCP_ACCEPT_BATCH_SIZE
--   conexões por ciclo) — sem socket.sleep, pois accept() retorna nil
--   imediatamente quando não há conexões pendentes (timeout=0).
-- ====================================================================

local function handle_control_command(client)
    local peer_ip, peer_port = client:getpeername()
    print(string.format("[Sensor Lua:TCP] Conexão estabelecida com %s:%s", peer_ip, peer_port))

    -- [Melhoria 5] Timeout reduzido: clientes lentos não monopolizam o loop
    client:settimeout(TCP_READ_TIMEOUT_SECS)

    local header_data, header_err = recv_exact(client, 4)
    if not header_data then
        print(string.format("[Sensor Lua:Erro] Falha no cabeçalho TCP: %s", header_err or "desconhecido"))
        client:close()
        return
    end

    local msg_len = string.unpack(">I4", header_data)
    if msg_len <= 0 or msg_len > MAX_TCP_FRAME_BYTES then
        print(string.format("[Sensor Lua:Erro] Frame TCP rejeitado por tamanho inválido: %d bytes", msg_len))
        client:close()
        return
    end

    local body_data, body_err = recv_exact(client, msg_len)
    if not body_data then
        print(string.format("[Sensor Lua:Erro] Falha no payload TCP (%d bytes esperados): %s",
              msg_len, body_err or "desconhecido"))
        client:close()
        return
    end

    local decode_ok, cmd = pcall(pb.decode, "smartcity.ConfigCommand", body_data)
    if not decode_ok or not cmd then
        print("[Sensor Lua:Erro] Corrupção na desserialização do payload de atuação Protobuf.")
        client:close()
        return
    end

    print(string.format("[Sensor Lua:Controle] Frame ID '%s' desserializado.", cmd.command_id))

    local target_device_id = (cmd.target_device_id and cmd.target_device_id ~= "")
                             and cmd.target_device_id or DEFAULT_DEVICE_ID
    local target = devices[target_device_id]

    if not target then
        print(string.format("[Sensor Lua:Erro] Dispositivo alvo desconhecido: %s", target_device_id))
        local resp_bytes = assert(pb.encode("smartcity.ConfigResponse", {
            command_id = cmd.command_id, success = false,
            message    = "Dispositivo alvo desconhecido no atuador Lua."
        }))
        client:send(string.pack(">I4", #resp_bytes) .. resp_bytes)
        client:close()
        return
    end

    if cmd.update_status and cmd.target_status then
        target.status       = cmd.target_status
        target.manual_until = socket.gettime() + MANUAL_OVERRIDE_SECS
        print("[Sensor Lua:Atuação] -> Status: " .. target.status)
    end

    if cmd.update_frequency and cmd.new_frequency_secs > 0 then
        target.frequency_secs = cmd.new_frequency_secs
        print("[Sensor Lua:Atuação] -> Frequência: " .. target.frequency_secs .. "s")
    end

    target.last_udp_send = 0

    local resp_bytes = assert(pb.encode("smartcity.ConfigResponse", {
        command_id             = cmd.command_id,
        success                = true,
        message                = "Atuador Lua realinhado para " .. target.device_id .. ".",
        updated_status         = target.status,
        updated_frequency_secs = target.frequency_secs
    }))
    client:send(string.pack(">I4", #resp_bytes) .. resp_bytes)
    send_discovery_response(target.device_id)
    client:close()
end

local function poll_control_commands()
    local accepted = 0
    while accepted < TCP_ACCEPT_BATCH_SIZE do
        local client = tcp_server:accept()
        if not client then break end   -- backlog esgotado
        accepted = accepted + 1
        handle_control_command(client)
    end
end

-- ====================================================================
-- [Melhorias 3 e 4] Drenagem completa de probes multicast + fila múltipla
--
-- PROBLEMA ORIGINAL — Melhoria 3:
--   Uma única udp_mc:receivefrom() por ciclo deixava pacotes acumulando
--   no buffer do kernel. Sob rajadas de probes, o backlog crescia
--   indefinidamente entre iterações.
--
-- PROBLEMA ORIGINAL — Melhoria 4:
--   Um único probe_pending_until descartava silenciosamente os probes B
--   e C se chegassem enquanto A estava pendente. Em vez de múltiplas
--   respostas escalonadas no tempo, apenas uma era emitida.
--
-- CORREÇÃO:
--   pending_discoveries é uma fila FIFO de entradas { fire_at }.
--   Cada probe recebido enquanto a fila não estiver saturada gera
--   uma nova entrada com jitter independente. A drenagem do socket
--   multicast é feita em loop (até MULTICAST_BATCH_SIZE por ciclo).
--   Entradas prontas (fire_at <= current_time) são disparadas em ordem.
-- ====================================================================

local function poll_multicast_probes(current_time)
    -- Passo A: disparar descobertas cujo jitter expirou
    while not queue_empty(pending_discoveries) do
        local entry = queue_peek(pending_discoveries)
        if current_time < entry.fire_at then break end  -- head não pronto = fila não pronta
        dequeue(pending_discoveries)
        send_discovery_response()
    end

    -- Passo B: drenar todos os datagramas disponíveis no socket multicast
    local received = 0
    while received < MULTICAST_BATCH_SIZE do
        local data, peer_ip = udp_mc:receivefrom()
        if not data then break end
        received = received + 1

        if data == "SMARTCITY_DISCOVERY_PROBE" then
            if queue_size(pending_discoveries) < MAX_PENDING_DISCOVERIES then
                local delay = discovery_probe_jitter()
                enqueue(pending_discoveries, { fire_at = current_time + delay })
                print(string.format(
                    "[Sensor Lua:Multicast] Probe de %s → descoberta agendada em %.0fms (fila: %d/%d).",
                    peer_ip, delay * 1000,
                    queue_size(pending_discoveries), MAX_PENDING_DISCOVERIES))
            else
                print(string.format(
                    "[Sensor Lua:Multicast] Fila saturada (%d/%d) — probe de %s ignorado.",
                    MAX_PENDING_DISCOVERIES, MAX_PENDING_DISCOVERIES, peer_ip))
            end
        end
    end
end

-- ====================================================================
-- SINAL POSIX — Shutdown gracioso (SIGTERM / SIGINT)
-- ====================================================================

local keep_running = true

local ok_sig, posix_sig = pcall(require, "posix.signal")
if ok_sig then
    local function on_shutdown()
        io.write("\n[Sensor Lua] Sinal POSIX interceptado. Acionando teardown gracioso...\n")
        io.flush()
        keep_running = false
    end
    posix_sig.signal(posix_sig.SIGTERM, on_shutdown)
    posix_sig.signal(posix_sig.SIGINT,  on_shutdown)
    print("[Sensor Lua:Sinal] Handlers SIGTERM/SIGINT registrados via luaposix.")
else
    print(string.format("[Sensor Lua:Aviso] luaposix indisponível: %s", tostring(posix_sig)))
    print("[Sensor Lua:Aviso] SIGTERM não interceptado — docker stop aguardará StopTimeout antes de SIGKILL.")
end

-- ====================================================================
-- KERNEL DE EVENTOS (MAIN EXECUTION ENGINE)
-- ====================================================================

send_discovery_response()
local next_heartbeat_at = socket.gettime() + heartbeat_delay()

while keep_running do
    local current_time = socket.gettime()

    -- 1. Reenvios UDP pendentes (não-bloqueante — fila de retransmissão)
    process_udp_retry_queue(current_time)

    -- 2. Eventos imediatos por limiar
    poll_threshold_events(current_time)

    -- 3. Heartbeat topológico
    -- [Melhoria 6] next_heartbeat_at avança a partir do tempo AGENDADO,
    -- não do tempo atual — elimina drift acumulativo entre disparos.
    if current_time >= next_heartbeat_at then
        print("[Sensor Lua:Heartbeat] Renovando presença via DiscoveryResponse.")
        send_discovery_response()
        next_heartbeat_at = next_heartbeat_at + heartbeat_delay()
        -- Guarda de avanço: se o loop ficou bloqueado por muito tempo,
        -- pula beats perdidos em vez de enviar uma rajada de heartbeats.
        if next_heartbeat_at < current_time then
            next_heartbeat_at = current_time + heartbeat_delay()
        end
    end

    -- 4. Telemetria periódica UDP
    for _, device_id in ipairs(device_order) do
        local device = devices[device_id]
        if (current_time - device.last_udp_send) >= (device.frequency_secs + device.next_jitter_secs) then
            send_metrics_payload(device)
            device.last_udp_send    = current_time
            device.next_jitter_secs = math.random() * TELEMETRY_JITTER_SECS
        end
    end

    -- 5. Drenagem do backlog TCP (múltiplas conexões por ciclo)
    poll_control_commands()

    -- 6. Drenagem do buffer multicast + fila de descobertas pendentes
    poll_multicast_probes(current_time)

    -- 7. Cessão de ciclos ao kernel host
    socket.sleep(0.1 + (math.random() * TELEMETRY_JITTER_SECS / 10))
end

-- ====================================================================
-- TEARDOWN GRACIOSO
-- ====================================================================
print("\n[Sensor Lua] Loop principal encerrado. Iniciando teardown...")

-- Marca todos os nós como offline
for _, device_id in ipairs(device_order) do
    devices[device_id].status = "STATUS_OFF"
end

-- DiscoveryResponse final (pode enfileirar na retry queue se UDP falhar)
print("[Sensor Lua:Teardown] Emitindo DiscoveryResponse final com STATUS_OFF...")
send_discovery_response()

-- Tenta drenar a fila de retransmissão antes de fechar os sockets
-- (máximo 3 tentativas × 50ms = 150ms de atraso adicional no teardown)
for _ = 1, 3 do
    if queue_empty(udp_retry_queue) then break end
    process_udp_retry_queue(socket.gettime())
    socket.sleep(0.05)
end

-- Fecha sockets em ordem inversa de dependência
-- (udp_out por último: send_udp_nonblocking e process_udp_retry_queue dependem dele)
tcp_server:close()
udp_mc:close()
udp_out:close()

print("[Sensor Lua:Teardown] Sockets encerrados. Processo finalizado com sucesso.")
