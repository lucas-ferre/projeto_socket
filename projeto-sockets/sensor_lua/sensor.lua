local socket = require("socket")
local pb = require("pb")

print("============================================================")
print("[Sensor Lua] Inicializando Poste Inteligente (Arquitetura Multiplexada)...")
print("============================================================")

-- 1. Inicializa a entropia do gerador pseudoaleatório com o epoch atual do sistema
math.randomseed(os.time())
-- Executa descartes iniciais (dummy pops) para dispersar o estado interno do PRNG
for _ = 1, 3 do math.random() end

-- 2. Carregamento do descritor binário Protobuf compilado no Dockerfile
assert(pb.loadfile("messages.pb"), "[Sensor Lua:Erro] Arquivo binário compilado messages.pb não encontrado no File System.")

-- ====================================================================
-- CONFIGURAÇÕES GERAIS E DE ROTEAMENTO (CAMADA DE TRANSPORTE)
-- ====================================================================

local SECTORS = {
    { name = "Pici",         slug = "pici"         },
    { name = "Benfica",      slug = "benfica"      },
    { name = "Porangabussu", slug = "porangabussu" }
}

local CONTROL_TCP_PORT       = 5002
local GATEWAY_HOST           = "gateway"
local GATEWAY_TELEMETRY_PORT = 5000
local GATEWAY_UDP_DISCOVERY_PORT = 5002
local MULTICAST_GROUP        = "239.0.0.1"
local MULTICAST_PORT         = 5005
local UDP_MAX_RETRIES        = 3
local RETRY_BASE_DELAY       = 0.20
local RETRY_MAX_DELAY        = 1.50
local TELEMETRY_JITTER_SECS  = 0.35
local DISCOVERY_PROBE_JITTER_SECS = 2.0
local HEARTBEAT_INTERVAL_SECS = math.max(1.0, tonumber(os.getenv("SENSOR_HEARTBEAT_INTERVAL_SECS") or "10") or 10.0)
local HEARTBEAT_JITTER_SECS   = math.max(0.0, tonumber(os.getenv("SENSOR_HEARTBEAT_JITTER_SECS") or "2") or 2.0)
local MAX_TCP_FRAME_BYTES    = 1024 * 1024
local MANUAL_OVERRIDE_SECS   = 30.0
local THRESHOLD_SCAN_INTERVAL_SECS = 1.0
local THRESHOLD_EVENT_COOLDOWN_SECS = 3.0
local LUMINOSITY_LOW_THRESHOLD = tonumber(os.getenv("LUMINOSITY_LOW_THRESHOLD") or "80")
local POWER_CONSUMPTION_THRESHOLD = tonumber(os.getenv("POWER_CONSUMPTION_THRESHOLD") or "32")
local DEVICE_COUNT           = tonumber(os.getenv("LUA_DEVICE_COUNT") or tostring(#SECTORS))

local devices      = {}
local device_order = {}

for idx = 1, DEVICE_COUNT do
    local sector    = SECTORS[((idx - 1) % #SECTORS) + 1]
    local sector_ordinal = math.floor((idx - 1) / #SECTORS) + 1
    local device_id = string.format("poste_%s_%02d", sector.slug, sector_ordinal)

    devices[device_id] = {
        device_id        = device_id,
        sector           = sector.name,
        status           = "STATUS_ON",
        frequency_secs   = 5,
        last_udp_send    = 0,
        next_jitter_secs = math.random() * TELEMETRY_JITTER_SECS,
        next_threshold_check = 0,
        last_threshold_send = 0,
        manual_until     = 0
    }
    table.insert(device_order, device_id)
    print(string.format("[Sensor Lua:Identidade] Nó provisionado com ID: %s | Setor: %s",
          device_id, sector.name))
end

local DEFAULT_DEVICE_ID = device_order[1]

-- ====================================================================
-- RESOLUÇÃO DNS — executada UMA ÚNICA VEZ no boot e cacheada
--
-- Motivação: get_gateway_ip() era invocada a cada pacote UDP emitido
-- (telemetria + descoberta), repetindo a resolução DNS a cada ciclo.
-- Em Docker, o DNS interno é estável — resolver no startup e reutilizar
-- o IP eliminam overhead de lookup desnecessário por pacote.
-- ====================================================================

local function get_gateway_ip()
    for attempt = 1, UDP_MAX_RETRIES do
        local ip = socket.dns.toip(GATEWAY_HOST)
        if ip then return ip end

        if attempt < UDP_MAX_RETRIES then
            local delay = math.min(RETRY_MAX_DELAY, RETRY_BASE_DELAY * (2 ^ (attempt - 1)))
                          + (math.random() * RETRY_BASE_DELAY)
            print(string.format("[Sensor Lua:Retry] DNS do Gateway falhou (tentativa %d/%d). Retry em %.2fs.",
                  attempt, UDP_MAX_RETRIES, delay))
            socket.sleep(delay)
        end
    end

    print("[Sensor Lua:Aviso] DNS indisponível após retries. Fallback para 127.0.0.1.")
    return "127.0.0.1"
end

-- IP resolvido uma vez no boot — reutilizado em todas as emissões UDP
local GATEWAY_IP = get_gateway_ip()
print(string.format("[Sensor Lua:DNS] Gateway '%s' resolvido para %s — IP cacheado para o ciclo de vida do processo.",
      GATEWAY_HOST, GATEWAY_IP))

-- ====================================================================
-- INICIALIZAÇÃO DE DESCRITORES DE REDE (SOCKETS POSIX BOUND)
-- ====================================================================

-- A. Socket UDP Primário: emissor unicast para telemetria e handshake
local udp_out = socket.udp()
udp_out:settimeout(0)

-- B. Socket UDP Secundário: listener multicast para probes de redescoberta
local udp_mc = socket.udp()
udp_mc:settimeout(0)
udp_mc:setoption("reuseaddr", true)
assert(udp_mc:setsockname("0.0.0.0", MULTICAST_PORT))
assert(udp_mc:setoption("ip-add-membership", { multiaddr = MULTICAST_GROUP, interface = "0.0.0.0" }))
print(string.format("[Sensor Lua:Multicast] Interface vinculada ao grupo de resiliência %s:%d",
      MULTICAST_GROUP, MULTICAST_PORT))

-- C. Socket TCP Servidor: aceita conexões de controle (atuador)
local tcp_server = assert(socket.bind("0.0.0.0", CONTROL_TCP_PORT))
tcp_server:settimeout(0)
print(string.format("[Sensor Lua:TCP] Interface de controle provisionada na porta %d.", CONTROL_TCP_PORT))

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

local function random_device_status()
    local roll = math.random(1, 100)
    if roll <= 78 then return "STATUS_ON"    end
    if roll <= 90 then return "STATUS_OFF"   end
    return "STATUS_ERROR"
end

local function send_udp_with_retry(bytes, host, port, channel)
    local last_err = "desconhecido"
    for attempt = 1, UDP_MAX_RETRIES do
        local ok, err = udp_out:sendto(bytes, host, port)
        if ok then return true end
        last_err = err or last_err
        if attempt < UDP_MAX_RETRIES then
            local delay = retry_delay(attempt)
            print(string.format("[Sensor Lua:Retry] UDP %s falhou (tentativa %d/%d): %s. Retry em %.2fs.",
                  channel, attempt, UDP_MAX_RETRIES, last_err, delay))
            socket.sleep(delay)
        end
    end
    return false, last_err
end

-- ====================================================================
-- recv_exact — Leitura TCP com garantia de N bytes (loop acumulador)
--
-- Motivação: LuaSocket's client:receive(n) pode retornar menos de n
-- bytes quando o SO entrega o segmento TCP em partes — especialmente
-- em redes com latência ou jitter, ou quando settimeout() expira antes
-- de todos os bytes chegarem. Nesse caso, receive() retorna:
--   nil, "timeout", partial_data
-- onde partial_data contém os bytes recebidos até o timeout.
--
-- Sem loop, a leitura parcial causa parse error silencioso no Protobuf.
-- Esta função acumula chunks até completar exatamente n bytes,
-- análoga ao recv_exact() do Python e read_n_bytes() do C.
-- ====================================================================

local function recv_exact(client, n)
    local chunks   = {}   -- Acumulador de segmentos parciais
    local received = 0    -- Total de bytes acumulados até o momento

    while received < n do
        local remaining      = n - received
        local chunk, err, partial = client:receive(remaining)

        if chunk then
            -- Caso nominal: receive() entregou exatamente 'remaining' bytes
            table.insert(chunks, chunk)
            received = received + #chunk

        elseif partial and #partial > 0 then
            -- Caso de entrega parcial: timeout com dados incompletos
            -- Acumula o que chegou e continua tentando se for apenas timeout
            table.insert(chunks, partial)
            received = received + #partial

            if err ~= "timeout" then
                -- Erro real (closed, reset) após dados parciais — aborta
                break
            end
            -- "timeout" com partial: continua o loop para buscar o restante

        else
            -- Nenhum dado e nenhum parcial: erro fatal (conexão fechada, etc.)
            return nil, string.format(
                "recv_exact: erro após %d/%d bytes — %s",
                received, n, err or "conexão encerrada"
            )
        end
    end

    if received < n then
        return nil, string.format(
            "recv_exact: frame incompleto — %d/%d bytes recebidos", received, n
        )
    end

    return table.concat(chunks)  -- Concatenação única ao final (eficiente para N segmentos)
end

-- ====================================================================
-- ROTINAS DE PROTOCOLO E SERIALIZAÇÃO BINÁRIA
-- ====================================================================

-- Rotina 1: transmite o vetor de anúncio topológico no duto de descoberta
local function send_discovery_response(target_device_id)
    local ids = target_device_id and { target_device_id } or device_order

    for _, device_id in ipairs(ids) do
        local device = devices[device_id]
        if device then
            local msg = {
                device_id       = device.device_id,
                type            = "DEVICE_TYPE_LAMP_POST",
                ip_address      = "sensor_posto",
                control_port    = CONTROL_TCP_PORT,
                initial_status  = device.status,
                is_controllable = true
            }
            local bytes = assert(pb.encode("smartcity.DiscoveryResponse", msg))
            -- GATEWAY_IP: variável resolvida no boot — sem chamada DNS por pacote
            local ok, err = send_udp_with_retry(bytes, GATEWAY_IP, GATEWAY_UDP_DISCOVERY_PORT, "Descoberta")
            if ok then
                print(string.format(
                    "[Sensor Lua:Descoberta] Dispositivo=%s | Setor=%s | Status=%s | Vetor topológico despachado via porta %d.",
                    device.device_id, device.sector, device.status, GATEWAY_UDP_DISCOVERY_PORT))
            else
                print(string.format("[Sensor Lua:Erro] Descoberta descartada após retries: %s", err))
            end
        end
    end
end

-- Rotina 2: empacota e transmite os DataPayloads periódicos no duto de métricas
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

    -- GATEWAY_IP: variável resolvida no boot — sem chamada DNS por pacote
    local ok, err = send_udp_with_retry(bytes, GATEWAY_IP, GATEWAY_TELEMETRY_PORT, "Telemetria")
    if ok and device.status == "STATUS_ON" then
        local event_label = trigger_reason and "Evento por limiar" or "Telemetria injetada"
        print(string.format(
            "[Sensor Lua:UDP] %s | Dispositivo=%s | Setor=%s | Luminosidade: %d%% | Consumo: %.1f W%s",
            event_label,
            device.device_id, device.sector,
            payload.metrics[1].value, payload.metrics[2].value,
            trigger_reason and (" | Limiar=" .. trigger_reason) or ""))
    elseif ok then
        print(string.format(
            "[Sensor Lua:UDP] Heartbeat operacional | Dispositivo=%s | Setor=%s | Status=%s",
            device.device_id, device.sector, device.status))
    else
        print(string.format("[Sensor Lua:Erro] Telemetria descartada após retries: %s", err))
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
            local metrics = build_lamp_metrics()
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

-- Rotina 3: processa fluxos de atuação com Length-Prefix Framing robusto
local function poll_control_commands()
    local client = tcp_server:accept()
    if not client then return end

    local peer_ip, peer_port = client:getpeername()
    print(string.format("[Sensor Lua:TCP] Conexão estabelecida com %s:%s", peer_ip, peer_port))

    client:settimeout(5.0)

    -- Passo A: leitura exata do cabeçalho de 4 bytes via recv_exact
    -- Garante o prefixo completo mesmo que o SO entregue em segmentos parciais
    local header_data, header_err = recv_exact(client, 4)

    if not header_data then
        print(string.format("[Sensor Lua:Erro] Falha na leitura do cabeçalho TCP: %s",
              header_err or "desconhecido"))
        client:close()
        return
    end

    local msg_len = string.unpack(">I4", header_data)
    if msg_len <= 0 or msg_len > MAX_TCP_FRAME_BYTES then
        print(string.format("[Sensor Lua:Erro] Frame TCP rejeitado por tamanho inválido: %d bytes", msg_len))
        client:close()
        return
    end

    -- Passo B: leitura exata do payload via recv_exact
    -- Evita truncamento silencioso em segmentos TCP fragmentados
    local body_data, body_err = recv_exact(client, msg_len)

    if not body_data then
        print(string.format("[Sensor Lua:Erro] Falha na leitura do payload TCP (%d bytes esperados): %s",
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
        local resp       = { command_id = cmd.command_id, success = false,
                             message    = "Dispositivo alvo desconhecido no atuador Lua." }
        local resp_bytes = assert(pb.encode("smartcity.ConfigResponse", resp))
        client:send(string.pack(">I4", #resp_bytes) .. resp_bytes)
        client:close()
        return
    end

    -- Processamento de mutações de estado
    if cmd.update_status and cmd.target_status then
        target.status       = cmd.target_status
        target.manual_until = socket.gettime() + MANUAL_OVERRIDE_SECS
        print("[Sensor Lua:Atuação] -> Mutação de estado operacional: " .. target.status)
    end

    if cmd.update_frequency and cmd.new_frequency_secs > 0 then
        target.frequency_secs = cmd.new_frequency_secs
        print("[Sensor Lua:Atuação] -> Reconfiguração temporal (Amostragem): "
              .. target.frequency_secs .. "s")
    end

    target.last_udp_send = 0

    -- Passo C: resposta com framing de saída
    local resp = {
        command_id            = cmd.command_id,
        success               = true,
        message               = "Atuador Lua realinhado operacionalmente para " .. target.device_id .. ".",
        updated_status        = target.status,
        updated_frequency_secs = target.frequency_secs
    }
    local resp_bytes = assert(pb.encode("smartcity.ConfigResponse", resp))
    client:send(string.pack(">I4", #resp_bytes) .. resp_bytes)

    send_discovery_response(target.device_id)
    client:close()
end

-- Rotina 4: listener passivo para re-sincronização via probe multicast
local function poll_multicast_probes()
    local data, peer_ip = udp_mc:receivefrom()
    if data and data == "SMARTCITY_DISCOVERY_PROBE" then
        local delay = discovery_probe_jitter()
        print(string.format(
            "[Sensor Lua:Multicast] Probe rastreado via %s. Injetando sincronização em %.0f ms!",
            peer_ip, delay * 1000))
        socket.sleep(delay)
        send_discovery_response()
    end
end

-- ====================================================================
-- KERNEL DE EVENTOS (MAIN EXECUTION ENGINE)
-- ====================================================================

send_discovery_response()
local next_heartbeat_at = socket.gettime() + heartbeat_delay()

while true do
    local current_time = socket.gettime()

    -- 1. Eventos imediatos por limiar sem deslocar a cadência periódica
    poll_threshold_events(current_time)

    -- 2. Heartbeat topológico independente da cadência de métricas
    if current_time >= next_heartbeat_at then
        print("[Sensor Lua:Heartbeat] Renovando presença da frota via DiscoveryResponse.")
        send_discovery_response()
        next_heartbeat_at = socket.gettime() + heartbeat_delay()
    end

    -- 3. Despacho sequencial de telemetria UDP
    for _, device_id in ipairs(device_order) do
        local device = devices[device_id]
        if (current_time - device.last_udp_send) >= (device.frequency_secs + device.next_jitter_secs) then
            send_metrics_payload(device)
            device.last_udp_send    = current_time
            device.next_jitter_secs = math.random() * TELEMETRY_JITTER_SECS
        end
    end

    -- 4. Inspeção da fila TCP para requisições de controle
    poll_control_commands()

    -- 5. Inspeção da interface multicast para probes de disaster recovery
    poll_multicast_probes()

    -- 6. Cessão de ciclos de CPU ao kernel host
    socket.sleep(0.1 + (math.random() * TELEMETRY_JITTER_SECS / 10))
end
