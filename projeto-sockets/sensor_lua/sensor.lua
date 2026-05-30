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
    { name = "Pici", slug = "pici" },
    { name = "Benfica", slug = "benfica" },
    { name = "Porangabussu", slug = "porangabussu" }
}

local CONTROL_PORT = 5002
local GATEWAY_HOST = "gateway"

-- Portas segregadas para evitar colisão de desserialização no nó central
local GATEWAY_TELEMETRY_PORT = 5000
local GATEWAY_DISCOVERY_PORT = 5002

local MULTICAST_GROUP = "239.0.0.1"
local MULTICAST_PORT = 5000
local UDP_MAX_RETRIES = 3
local RETRY_BASE_DELAY = 0.20
local RETRY_MAX_DELAY = 1.50
local TELEMETRY_JITTER_SECS = 0.35
local MAX_TCP_FRAME_BYTES = 1024 * 1024
local MANUAL_OVERRIDE_SECS = 30.0
local DEVICE_COUNT = tonumber(os.getenv("LUA_DEVICE_COUNT") or tostring(#SECTORS))

local devices = {}
local device_order = {}

for idx = 1, DEVICE_COUNT do
    local sector = SECTORS[((idx - 1) % #SECTORS) + 1]
    local suffix = string.format("%04x", math.random(0, 65535))
    local device_id = "poste_" .. sector.slug .. "_" .. suffix

    devices[device_id] = {
        device_id = device_id,
        sector = sector.name,
        status = "STATUS_ON",
        frequency_secs = 5,
        last_udp_send = 0,
        next_jitter_secs = math.random() * TELEMETRY_JITTER_SECS,
        manual_until = 0
    }
    table.insert(device_order, device_id)
    print(string.format("[Sensor Lua:Identidade] Nó provisionado com ID: %s | Setor: %s", device_id, sector.name))
end

local DEFAULT_DEVICE_ID = device_order[1]

-- Função auxiliar para resolução estrita de DNS do Hub Coordenador com fallback de loopback
local function get_gateway_ip()
    for attempt = 1, UDP_MAX_RETRIES do
        local ip = socket.dns.toip(GATEWAY_HOST)
        if ip then return ip end

        if attempt < UDP_MAX_RETRIES then
            local delay = math.min(RETRY_MAX_DELAY, RETRY_BASE_DELAY * (2 ^ (attempt - 1))) + (math.random() * RETRY_BASE_DELAY)
            print(string.format("[Sensor Lua:Retry] DNS do Gateway falhou (tentativa %d/%d). Retry em %.2fs.", attempt, UDP_MAX_RETRIES, delay))
            socket.sleep(delay)
        end
    end

    print("[Sensor Lua:Aviso] DNS indisponível. Fallback para 127.0.0.1.")
    return "127.0.0.1"
end

-- ====================================================================
-- INICIALIZAÇÃO DE DESCRITORES DE REDE (SOCKETS POSIX BOUND)
-- ====================================================================

-- A. Socket UDP Primário: Descritor não-bloqueante para emissão Unicast (Telemetria/Handshake)
local udp_out = socket.udp()
udp_out:settimeout(0)

-- B. Socket UDP Secundário: Listener dedicado à interceptação de tráfego IGMP/Multicast
local udp_mc = socket.udp()
udp_mc:settimeout(0)
udp_mc:setoption("reuseaddr", true) -- SO_REUSEADDR para concorrência de nós
assert(udp_mc:setsockname("0.0.0.0", MULTICAST_PORT))
assert(udp_mc:setoption("ip-add-membership", { multiaddr = MULTICAST_GROUP, interface = "0.0.0.0" }))
print(string.format("[Sensor Lua:Multicast] Interface vinculada ao grupo de resiliência %s:%d", MULTICAST_GROUP, MULTICAST_PORT))

-- C. Socket TCP Servidor: Aceitação de sockets para reconfiguração de estado (Atuador)
local tcp_server = assert(socket.bind("0.0.0.0", CONTROL_PORT))
tcp_server:settimeout(0) -- Operação assíncrona orientada ao event-loop
print(string.format("[Sensor Lua:TCP] Interface de controle provisionada na porta %d.", CONTROL_PORT))

-- ====================================================================
-- ROTINAS DE PROTOCOLO E SERIALIZAÇÃO BINÁRIA
-- ====================================================================

local function retry_delay(attempt)
    local backoff = math.min(RETRY_MAX_DELAY, RETRY_BASE_DELAY * (2 ^ (attempt - 1)))
    return backoff + (math.random() * RETRY_BASE_DELAY)
end

local function random_device_status()
    local roll = math.random(1, 100)
    if roll <= 78 then return "STATUS_ON" end
    if roll <= 90 then return "STATUS_OFF" end
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

-- Rotina 1: Transmite o vetor estruturado de anúncio topológico no duto segregado
local function send_discovery_response(target_device_id)
    local ids = target_device_id and { target_device_id } or device_order

    for _, device_id in ipairs(ids) do
        local device = devices[device_id]
        if device then
            local msg = {
                device_id = device.device_id,
                type = "DEVICE_TYPE_LAMP_POST",
                ip_address = "sensor_posto",
                control_port = CONTROL_PORT,
                initial_status = device.status,
                is_controllable = true
            }
            local bytes = assert(pb.encode("smartcity.DiscoveryResponse", msg))
            local ok, err = send_udp_with_retry(bytes, get_gateway_ip(), GATEWAY_DISCOVERY_PORT, "Descoberta")
            if ok then
                print(string.format("[Sensor Lua:Descoberta] Dispositivo=%s | Setor=%s | Status=%s | Vetor topológico despachado via porta %d.",
                      device.device_id, device.sector, device.status, GATEWAY_DISCOVERY_PORT))
            else
                print(string.format("[Sensor Lua:Erro] Descoberta descartada após retries: %s", err))
            end
        end
    end
end

-- Rotina 2: Empacota e transmite os DataPayloads periódicos no duto de métricas
local function send_metrics_payload(device)
    if socket.gettime() >= device.manual_until then
        device.status = random_device_status()
    end

    local current_time = os.time()
    local msg_id = string.format("%s-%d-%04d", device.device_id, current_time, math.random(1, 9999))
    local metrics = {}
    if device.status == "STATUS_ON" then
        metrics = {
            { name = "luminosity", value = math.random(75, 100), unit = "%" },
            { name = "power_consumption", value = 25.0 + (math.random() * 10.0), unit = "W" }
        }
    end

    local payload = {
        message_id = msg_id,
        timestamp = current_time,
        device_id = device.device_id,
        current_status = device.status,
        metrics = metrics
    }
    local bytes = assert(pb.encode("smartcity.DataPayload", payload))
    
    -- Injeção estrita no pipeline analítico/telemetria (Porta 5000)
    local ok, err = send_udp_with_retry(bytes, get_gateway_ip(), GATEWAY_TELEMETRY_PORT, "Telemetria")
    if ok and device.status == "STATUS_ON" then
        print(string.format("[Sensor Lua:UDP] Telemetria injetada | Dispositivo=%s | Setor=%s | Luminosidade: %d%% | Consumo: %.1f W", 
              device.device_id, device.sector, payload.metrics[1].value, payload.metrics[2].value))
    elseif ok then
        print(string.format("[Sensor Lua:UDP] Heartbeat operacional | Dispositivo=%s | Setor=%s | Status=%s",
              device.device_id, device.sector, device.status))
    else
        print(string.format("[Sensor Lua:Erro] Telemetria descartada após retries: %s", err))
    end
end

-- Rotina 3: Processa fluxos de atuação executando Length-Prefix Framing na camada de aplicação
local function poll_control_commands()
    local client = tcp_server:accept()
    if not client then return end

    local peer_ip, peer_port = client:getpeername()
    print(string.format("[Sensor Lua:TCP] Conexão física estabelecida com %s:%s", peer_ip, peer_port))

    -- Janela de bloqueio restrita para mitigar overhead de I/O parcial
    client:settimeout(5.0)

    -- Passo A: Extração atômica do cabeçalho de Framing (4 Bytes Big-Endian)
    local header_data, err = client:receive(4)
    
    if not err and header_data then
        -- Casting do prefixo binário para inteiro (unsigned de 32 bits)
        local msg_len = string.unpack(">I4", header_data)
        if msg_len <= 0 or msg_len > MAX_TCP_FRAME_BYTES then
            print(string.format("[Sensor Lua:Erro] Frame TCP rejeitado por tamanho inválido: %d bytes", msg_len))
            client:close()
            return
        end
        
        -- Passo B: Extração estrita do payload baseada na restrição do cabeçalho
        local body_data, body_err = client:receive(msg_len)
        
        if not body_err and body_data then
            local decode_ok, cmd = pcall(pb.decode, "smartcity.ConfigCommand", body_data)
            if decode_ok and cmd then
                print(string.format("[Sensor Lua:Controle] Frame ID '%s' desserializado nativamente.", cmd.command_id))
                local target_device_id = cmd.target_device_id or DEFAULT_DEVICE_ID
                if target_device_id == "" then target_device_id = DEFAULT_DEVICE_ID end
                local target = devices[target_device_id]

                if not target then
                    print(string.format("[Sensor Lua:Erro] Dispositivo alvo desconhecido: %s", target_device_id))
                    local resp = {
                        command_id = cmd.command_id,
                        success = false,
                        message = "Dispositivo alvo desconhecido no atuador Lua."
                    }
                    local resp_bytes = assert(pb.encode("smartcity.ConfigResponse", resp))
                    client:send(string.pack(">I4", #resp_bytes) .. resp_bytes)
                    client:close()
                    return
                end
                
                -- Processamento de mutações no estado interno
                if cmd.update_status and cmd.target_status then
                    target.status = cmd.target_status
                    target.manual_until = socket.gettime() + MANUAL_OVERRIDE_SECS
                    print("[Sensor Lua:Atuação] -> Mutação de estado operacional: " .. target.status)
                end
                
                if cmd.update_frequency and cmd.new_frequency_secs > 0 then
                    target.frequency_secs = cmd.new_frequency_secs
                    print("[Sensor Lua:Atuação] -> Reconfiguração temporal (Amostragem): " .. target.frequency_secs .. "s")
                end

                target.last_udp_send = 0

                -- Passo C: Orquestração do vetor de resposta sintética (ACK)
                local resp = {
                    command_id = cmd.command_id,
                    success = true,
                    message = "Atuador Lua realinhado operacionalmente para " .. target.device_id .. ".",
                    updated_status = target.status,
                    updated_frequency_secs = target.frequency_secs
                }
                local resp_bytes = assert(pb.encode("smartcity.ConfigResponse", resp))
                
                -- Passo D: Aplicação do Framing de saída (Concatenação de Buffer)
                local resp_header = string.pack(">I4", #resp_bytes)
                client:send(resp_header .. resp_bytes)
                send_discovery_response(target.device_id)
            else
                print("[Sensor Lua:Erro] Corrupção na desserialização do payload de atuação Protobuf.")
            end
        else
            print(string.format("[Sensor Lua:Erro] Truncamento de frame de rede I/O TCP: %s", body_err or "desconhecido"))
        end
    else
        print(string.format("[Sensor Lua:Erro] Queda na extração de cabeçalho Framing TCP: %s", err or "desconhecido"))
    end

    client:close()
end

-- Rotina 4: Listener passivo para mitigação de falhas e re-sincronização do Gateway
local function poll_multicast_probes()
    local data, peer_ip = udp_mc:receivefrom()
    if data and data == "SMARTCITY_DISCOVERY_PROBE" then
        print(string.format("[Sensor Lua:Multicast] Probe analítico rastreado via %s. Injetando sincronização!", peer_ip))
        send_discovery_response()
    end
end

-- ====================================================================
-- KERNEL DE EVENTOS (MAIN EXECUTION ENGINE)
-- ====================================================================

-- Engatilha handshake topológico primário de boot
send_discovery_response()

while true do
    local current_time = socket.gettime()

    -- 1. Avaliação temporal para despacho sequencial da telemetria UDP
    for _, device_id in ipairs(device_order) do
        local device = devices[device_id]
        if (current_time - device.last_udp_send) >= (device.frequency_secs + device.next_jitter_secs) then
            send_metrics_payload(device)
            device.last_udp_send = current_time
            device.next_jitter_secs = math.random() * TELEMETRY_JITTER_SECS
        end
    end

    -- 2. Inspeção de fila TCP para requisições de controle (Atuação RPC)
    poll_control_commands()

    -- 3. Inspeciona a interface IGMP/Multicast avaliando Probes de Disaster Recovery
    poll_multicast_probes()

    -- 4. Cessão voluntária de ciclos de CPU ao Kernel Host via delay nominal
    socket.sleep(0.1 + (math.random() * TELEMETRY_JITTER_SECS / 10))
end
