import streamlit as st
import socket
import time
import struct
import uuid
import datetime
import pandas as pd
from collections import defaultdict

# Importa as classes do Protobuf geradas no momento do build
import messages_pb2 # pyright: ignore[reportMissingImports]

# ====================================================================
# CONFIGURAÇÕES DE REDE E TRANSPORTE TCP
# ====================================================================

GATEWAY_HOST = "gateway"
GATEWAY_PORT = 5001

def recv_all(sock, n):
    """Função auxiliar para garantir a leitura de exatamente n bytes."""
    data = bytearray()
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet: return None
        data.extend(packet)
    return data

def send_tcp_request(request: messages_pb2.ClientRequest) -> messages_pb2.ClientResponse:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((GATEWAY_HOST, GATEWAY_PORT))
            
            # 1. Envia requisição com prefixo de tamanho
            msg_data = request.SerializeToString()
            s.sendall(struct.pack('>I', len(msg_data)) + msg_data)
            
            # 2. Lê o cabeçalho da resposta (4 bytes)
            header = recv_all(s, 4)
            if not header: return None
            resp_len = struct.unpack('>I', header)[0]
            
            # 3. Lê o corpo da resposta baseado no tamanho recebido
            data = recv_all(s, resp_len)
            if data:
                response = messages_pb2.ClientResponse()
                response.ParseFromString(data)
                return response
    except Exception as e:
        st.error(f"Erro de Framing TCP: {e}")
        return None

def infer_sector_from_device_id(device_id: str) -> str:
    sector_map = {
        "pici": "Pici",
        "benfica": "Benfica",
        "porangabussu": "Porangabussu",
    }

    for slug, label in sector_map.items():
        if f"_{slug}_" in device_id.lower():
            return label
    return "N/A"

# ====================================================================
# INTERFACE GRÁFICA (DASHBOARD STREAMLIT)
# ====================================================================

st.set_page_config(
    page_title="Smart City Analytics Client",
    page_icon="🏙️",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("🏙️ Centro de Controle Analítico - Smart City")
st.markdown("Monitoramento distribuído, controle operacional e agregação estatística via Sockets TCP/Protobuf.")

# Sidebar com informações de status geral
with st.sidebar:
    st.subheader("📊 Status do Sistema")
    col1, col2, col3 = st.columns(3)
    col1.metric("Gateway", "🟢 Ativo")
    col2.metric("Sensores", "N/A", help="Atualizar na aba Descoberta")
    col3.metric("Hora UTC", datetime.datetime.utcnow().strftime('%H:%M:%S'))
    st.markdown("---")
    st.info("💡 Selecione uma aba para começar o monitoramento.")

st.divider()

# Armazenamento de estado da sessão para histórico
if 'device_history' not in st.session_state:
    st.session_state.device_history = []
if 'command_history' not in st.session_state:
    st.session_state.command_history = []

# Abas de navegação mapeando os requisitos da especificação
tab1, tab2, tab3 = st.tabs([
    "📡 Fontes de Dados (Descoberta)", 
    "⚙️ Painel de Atuação (Controle)", 
    "📊 Consultas Analíticas (OLAP)"
])

# --------------------------------------------------------------------
# ABA 1: Consulta de Dispositivos Conectados
# --------------------------------------------------------------------
with tab1:
    st.subheader("📡 Nós Operacionais Registrados no Gateway")
    st.markdown("Consulte a topologia da rede inteligente em tempo real.")
    
    col_btn, col_refresh = st.columns([3, 1])
    with col_btn:
        if st.button("Atualizar Topologia de Rede", type="primary", width="stretch"):
            req = messages_pb2.ClientRequest()
            req.type = messages_pb2.REQUEST_TYPE_LIST_DEVICES
            
            with st.spinner("Consultando Gateway via TCP..."):
                resp = send_tcp_request(req)
                
            if resp and resp.success:
                st.session_state.device_history = resp.devices
                if not resp.devices:
                    st.info("✓ Nenhum dispositivo descoberto pelo Gateway até o momento.")
                else:
                    st.success(f"✓ {len(resp.devices)} dispositivo(s) encontrado(s)")
            elif resp:
                st.warning(f"⚠️ {resp.message}")
            else:
                st.error("❌ Falha na comunicação com Gateway")
    
    if st.session_state.device_history:
        # Preparar dados para visualização
        device_data = []
        type_map = {
            messages_pb2.DEVICE_TYPE_TRAFFIC_LIGHT: "🚦 Semáforo",
            messages_pb2.DEVICE_TYPE_LAMP_POST: "💡 Poste Inteligente",
            messages_pb2.DEVICE_TYPE_WEATHER_STATION: "🌦️ Estação Met.",
            messages_pb2.DEVICE_TYPE_CAMERA: "📹 Câmera de Tráfego",
            messages_pb2.DEVICE_TYPE_AIR_QUALITY: "💨 Qualidade do Ar"
        }
        
        status_map = {
            messages_pb2.STATUS_ON: "🟢 ONLINE",
            messages_pb2.STATUS_OFF: "⚪ OFFLINE",
            messages_pb2.STATUS_ERROR: "🔴 FALHA"
        }
        
        # Criar DataFrame
        for d in st.session_state.device_history:
            device_data.append({
                "ID": d.device_id,
                "Setor": infer_sector_from_device_id(d.device_id),
                "Tipo": type_map.get(d.type, "Desconhecido"),
                "Status": status_map.get(d.status, "Desconhecido"),
                "IP:Porta": f"{d.ip_address}:{d.control_port}" if d.is_controllable else f"{d.ip_address} (UDP)",
                "Controlável": "✓" if d.is_controllable else "✗",
                "Último Visto": datetime.datetime.fromtimestamp(d.last_seen_timestamp).strftime('%H:%M:%S')
            })
        
        df = pd.DataFrame(device_data)
        st.dataframe(df, width="stretch", hide_index=True)
        
        # Gráfico de status dos dispositivos
        st.divider()
        col_graph, col_stats = st.columns([2, 1])
        
        with col_graph:
            status_counts = defaultdict(int)
            for d in st.session_state.device_history:
                status_label = status_map.get(d.status, "Desconhecido")
                status_counts[status_label] += 1
            
            if status_counts:
                st.bar_chart(pd.DataFrame([
                    {"Status": status, "Quantidade": count}
                    for status, count in status_counts.items()
                ]).set_index("Status"))
        
        with col_stats:
            st.metric("Total de Dispositivos", len(st.session_state.device_history))
            online_count = sum(1 for d in st.session_state.device_history if d.status == messages_pb2.STATUS_ON)
            st.metric("Dispositivos Online", online_count)
            controllable_count = sum(1 for d in st.session_state.device_history if d.is_controllable)
            st.metric("Controláveis", controllable_count)

# --------------------------------------------------------------------
# ABA 2: Envio de Comandos de Controle
# --------------------------------------------------------------------
with tab2:
    st.subheader("⚙️ Console de Atuação Contextual")
    
    if not st.session_state.device_history:
        st.info("💡 Por favor, atualize a topologia na aba 'Fontes de Dados' para listar os dispositivos controláveis.")
    else:
        # 1. Seleção Inteligente de Dispositivo
        # Filtramos apenas os que permitem controle para evitar erros de I/O
        controllable_devices = [d for d in st.session_state.device_history if d.is_controllable]
        device_ids = [d.device_id for d in controllable_devices]
        
        col_sel, col_det = st.columns([1, 1])
        
        with col_sel:
            target_id = st.selectbox("🎯 Selecione o Dispositivo Alvo", options=device_ids)
            
            # Recupera o objeto do dispositivo selecionado para lógica contextual
            selected_device = next((d for d in controllable_devices if d.device_id == target_id), None)
        
        if selected_device:
            with col_det:
                # Exibe metadados técnicos do nó selecionado
                st.markdown(f"""
                **Detalhes do Nó:**
                - **Tipo:** {type_map.get(selected_device.type, "Desconhecido")}
                - **Endereço:** `{selected_device.ip_address}:{selected_device.control_port}`
                - **Estado Atual:** {status_map.get(selected_device.status, "Desconhecido")}
                """)

            st.divider()
            
            # 2. Formulário de Comando Adaptativo
            with st.form("form_controle_avancado"):
                st.write(f"### Configuração Operacional: {target_id}")
                
                c1, c2 = st.columns(2)
                
                with c1:
                    alterar_status = st.checkbox("Alterar Estado", value=False)
                    
                    # Rótulos dinâmicos baseados no tipo de sensor
                    if selected_device.type == messages_pb2.DEVICE_TYPE_TRAFFIC_LIGHT:
                        status_options = ["Ligar (ONLINE/VERDE)", "Desligar (OFFLINE)", "Emergência (ERROR/PISCANTE)"]
                    elif selected_device.type == messages_pb2.DEVICE_TYPE_LAMP_POST:
                        status_options = ["Acender (ONLINE)", "Apagar (OFFLINE)", "Manutenção (ERROR)"]
                    elif selected_device.type == messages_pb2.DEVICE_TYPE_CAMERA: # <-- ADICIONADO
                        status_options = ["Iniciar Gravação (ONLINE)", "Pausar Gravação (OFFLINE)", "Modo Diagnóstico (ERROR)"]
                    else:
                        status_options = ["Ativar (ONLINE)", "Desativar (OFFLINE)", "Falha (ERROR)"]
                        
                    novo_status_label = st.radio("Selecione o Novo Modo", status_options, disabled=not alterar_status)
                
                with c2:
                    alterar_freq = st.checkbox("Alterar Ciclo de Telemetria", value=False)
                    
                    # Descrição dinâmica da frequência
                    freq_help = "Define o intervalo de envio dos pacotes UDP para o Gateway."
                    nova_freq = st.slider("Segundos entre envios", 1, 60, 5, help=freq_help, disabled=not alterar_freq)

                st.markdown("---")
                submitted = st.form_submit_button("Transmitir Frame de Configuração", width="stretch")

                if submitted:
                    if not alterar_status and not alterar_freq:
                        st.warning("⚠️ Nenhuma alteração solicitada. Marque os campos que deseja modificar.")
                    else:
                        # Construção do Protobuf ClientRequest
                        req = messages_pb2.ClientRequest()
                        req.type = messages_pb2.REQUEST_TYPE_SEND_COMMAND
                        req.target_device_id = target_id
                        
                        cmd = req.command_payload
                        cmd.command_id = f"CMD-{uuid.uuid4().hex[:6].upper()}"
                        
                        if alterar_status:
                            cmd.update_status = True
                            if "Ligar" in novo_status_label or "Acender" in novo_status_label or "Ativar" in novo_status_label or "Iniciar" in novo_status_label:
                                cmd.target_status = messages_pb2.STATUS_ON
                            elif "Desligar" in novo_status_label or "Apagar" in novo_status_label or "Desativar" in novo_status_label or "Pausar" in novo_status_label:
                                cmd.target_status = messages_pb2.STATUS_OFF
                            else:
                                cmd.target_status = messages_pb2.STATUS_ERROR
                                
                        if alterar_freq:
                            cmd.update_frequency = True
                            cmd.new_frequency_secs = int(nova_freq)
                        
                        with st.spinner(f"Transmitindo comando via TCP para o Proxy Gateway..."):
                            resp = send_tcp_request(req)
                            
                        if resp:
                            # Log de histórico
                            ts = datetime.datetime.now().strftime('%H:%M:%S')
                            st.session_state.command_history.append({
                                "timestamp": ts,
                                "device": target_id,
                                "command_id": cmd.command_id,
                                "status": "✓ Sucesso" if resp.success else "✗ Falha",
                                "message": resp.message
                            })
                            
                            if resp.success:
                                st.success(f"**Comando Aplicado!** {resp.message}")
                                st.toast("Configuração atualizada com sucesso!", icon="✅")
                            else:
                                st.error(f"**Falha na Atuação:** {resp.message}")

# --------------------------------------------------------------------
# ABA 3: Consultas Analíticas Agregadas
# --------------------------------------------------------------------
with tab3:
    st.subheader("📊 Processamento Estatístico Centralizado (Gateway OLAP)")
    st.markdown("O processamento pesado ocorre no Gateway. O cliente recebe apenas o escalar resultante.")
    
    c_op, c_met, c_time = st.columns(3)
    
    with c_op:
        operacao = st.selectbox("Métrica de Agregação", [
            ("📈 Média Aritmética", messages_pb2.OP_AVERAGE),
            ("📊 Desvio Padrão", messages_pb2.OP_STD_DEV)
        ], format_func=lambda x: x[0])
        
    with c_met:
        metrica_alvo = st.selectbox("Métrica Alvo", [
            # --- Estação Ambiental (Sensor C) ---
            ("🌡️ Temperatura (°C)",              "temperature"),
            ("💧 Umidade Relativa (%)",            "humidity"),
            ("🌿 CO₂ (ppm)",                      "co2"),
            ("🌫️ PM2.5 — Part. Finas (µg/m³)",   "pm25"),
            ("💨 PM10 — Part. Grossas (µg/m³)",   "pm10"),
            ("🏭 AQI — Índice de Qualidade do Ar", "aqi"),
            # --- Poste Inteligente (Sensor Lua) ---
            ("💡 Luminosidade (%)",                "luminosity"),
            ("⚡ Consumo de Energia (W)",          "power_consumption"),
            # --- Semáforo (Sensor Java) ---
            ("🚦 Estado Operacional",              "state"),
            # --- Câmera de Tráfego (Sensor Python) ---
            ("🚗 Contagem de Veículos (veh/min)", "vehicles_count"),
            ("📸 Infrações Registradas", "infractions"),
        ], format_func=lambda x: x[0])
        
    with c_time:
        janela_horas = st.slider("Janela Temporal (Últimas X horas)", min_value=1, max_value=24, value=1)

    if st.button("Executar Consulta Analítica", type="primary", width="stretch"):
        req = messages_pb2.ClientRequest()
        req.type = messages_pb2.REQUEST_TYPE_ANALYTICS_QUERY
        req.query_op = operacao[1]
        req.query_metric = metrica_alvo[1]
        
        # Calcula a janela temporal em Epoch Timestamps
        agora = int(time.time())
        req.end_timestamp = agora
        req.start_timestamp = agora - (janela_horas * 3600)
        
        with st.spinner(f"⏳ Computando {operacao[0].lower()} no Gateway..."):
            resp = send_tcp_request(req)
            
        if resp:
            if resp.success:
                st.divider()
                
                # Exibir resultado em cards
                col_result, col_metadata = st.columns([2, 1])
                
                with col_result:
                    metric_icons = {
                        "temperature":        "🌡️",
                        "humidity":           "💧",
                        "co2":                "🌿",
                        "pm25":               "🌫️",
                        "pm10":               "💨",
                        "aqi":                "🏭",
                        "luminosity":         "💡",
                        "power_consumption":  "⚡",
                        "state":              "🚦",
                        "vehicles_count":     "🚗",
                        "infractions":        "📸",
                    }
                    metric_units = {
                        "temperature": "°C",   "humidity": "%",
                        "co2": "ppm",          "pm25": "µg/m³",
                        "pm10": "µg/m³",       "aqi": "",
                        "luminosity": "%",     "power_consumption": "W",
                        "state": "",
                        "vehicles_count": "veh/min",
                        "infractions": "count",
                    }

                    def aqi_category(v):
                        if v <= 50:   return "🟢 Bom"
                        if v <= 100:  return "🟡 Moderado"
                        if v <= 150:  return "🟠 Insalubre (sensíveis)"
                        if v <= 200:  return "🔴 Insalubre"
                        if v <= 300:  return "🟣 Muito Insalubre"
                        return "⚫ Perigoso"

                    icon  = metric_icons.get(metrica_alvo[1], "📊")
                    unit  = metric_units.get(metrica_alvo[1], "")
                    value = resp.analytics_result
                    label = f"{icon} {operacao[0].split()[-1]} — {metrica_alvo[0]}"

                    st.metric(label=label,
                              value=f"{value:.2f} {unit}".strip(),
                              delta=None)

                    if metrica_alvo[1] == "aqi":
                        st.info(f"Categoria: **{aqi_category(value)}**  \n"
                                "AQI ≤ 50 Bom · ≤ 100 Moderado · ≤ 150 Sensíveis · "
                                "≤ 200 Insalubre · ≤ 300 Muito Insalubre · > 300 Perigoso")
                    
                    result_chart_data = pd.DataFrame({
                        "Métrica": [operacao[0]],
                        "Valor":   [value]
                    }).set_index("Métrica")
                    st.bar_chart(result_chart_data)
                
                with col_metadata:
                    st.info(f"📋 **Metadados**\n\n{resp.result_metadata}\n\n**Intervalo:**\n{janela_horas}h")
                    st.caption(f"**ID da Resposta:** {resp.message_id}")
                    st.caption(f"**Timestamp:** {datetime.datetime.fromtimestamp(resp.timestamp).strftime('%H:%M:%S')}")
            else:
                st.warning(f"⚠️ Aviso do Gateway: {resp.message}")
                st.info("Tente expandir a janela temporal ou verifique se há dados disponíveis.")
        else:
            st.error("Falha na conexão com Gateway")
    
    # Informações sobre o processamento
    st.divider()
    with st.expander("Como funciona o OLAP no Gateway?"):
        st.markdown("""
        O Gateway implementa **Online Analytical Processing (OLAP)** para análises rápidas:

        - **Média Aritmética**: média simples dos valores da série temporal
        - **Desvio Padrão**: variação dos dados em relação à média

        Os dados são agregados no servidor e apenas o escalar resultante é transmitido ao cliente.

        **Métricas disponíveis por sensor:**
        | Sensor | Métricas |
        |--------|----------|
        | 🌡️ Estação Ambiental (C) | `temperature` · `humidity` · `co2` · `pm25` · `pm10` · `aqi` |
        | 💡 Poste Inteligente (Lua) | `luminosity` · `power_consumption` |
        | 🚦 Semáforo (Java) | `state` |
        | 📹 Câmera de Tráfego (Python) | `vehicles_count` · `infractions` |

        **Referência de qualidade do ar (AQI — EPA):**
        `0–50` Bom · `51–100` Moderado · `101–150` Insalubre (sensíveis) ·
        `151–200` Insalubre · `201–300` Muito insalubre · `>300` Perigoso
        """)
