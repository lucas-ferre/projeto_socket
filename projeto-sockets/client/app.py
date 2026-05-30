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
    """Função auxiliar para garantir a leitura de exatamente n bytes do buffer."""
    data = bytearray()
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet: return None
        data.extend(packet)
    return data

def send_tcp_request(request: messages_pb2.ClientRequest) -> messages_pb2.ClientResponse:
    """Encapsula a requisição TCP com protocolo de Framing e Timeout explícito."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            # FIX 1: Impede vazamento de conexão e travamento da UI (Timeout de 5s)
            s.settimeout(5.0)  
            s.connect((GATEWAY_HOST, GATEWAY_PORT))
            
            # 1. Envia requisição com prefixo de tamanho (Length-Prefix Framing)
            msg_data = request.SerializeToString()
            s.sendall(struct.pack('>I', len(msg_data)) + msg_data)
            
            # 2. Lê o cabeçalho da resposta (4 bytes Big-Endian)
            header = recv_all(s, 4)
            if not header: return None
            resp_len = struct.unpack('>I', header)[0]
            
            # 3. Lê o corpo binário exato da resposta e desserializa
            data = recv_all(s, resp_len)
            if data:
                response = messages_pb2.ClientResponse()
                response.ParseFromString(data)
                return response
    except Exception as e:
        st.error(f"Erro crítico no fluxo TCP/Framing: {e}")
        return None

def check_gateway_status() -> bool:
    """Realiza um probe rápido (2s) para atestar a vitalidade do nó central."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(2.0)
            s.connect((GATEWAY_HOST, GATEWAY_PORT))
            return True
    except Exception:
        return False

def infer_sector_from_device_id(device_id: str) -> str:
    """Realiza análise em substring para mapear IDs heterogêneos aos setores físicos."""
    sector_map = {
        "pici": "Pici",
        "benfica": "Benfica",
        "porangabussu": "Porangabussu",
    }
    
    # FIX 2: Busca relaxada para não quebrar com nomes como "CameraPici01"
    for slug, label in sector_map.items():
        if slug in device_id.lower():
            return label
    return "N/A"

def get_metric_reference_range(metric_key: str) -> dict:
    """Retorna dicionário com topologia de faixas críticas para a UI."""
    ranges = {
        "temperature": {
            "safe": (18, 26), "warning": (15, 32), "critical": (-float('inf'), 15),
            "description": "Faixa recomendada: 18-26°C"
        },
        "humidity": {
            "safe": (40, 60), "warning": (30, 70),
            "description": "Faixa recomendada: 40-60%"
        },
        "co2": {
            "safe": (0, 800), "warning": (800, 1200), "critical": (1200, float('inf')),
            "description": "Nível seguro: < 800 ppm"
        },
        "pm25": {
            "safe": (0, 12), "warning": (12, 35), "critical": (35, float('inf')),
            "description": "Limite seguro: ≤ 12 µg/m³"
        },
        "pm10": {
            "safe": (0, 54), "warning": (54, 154), "critical": (154, float('inf')),
            "description": "Limite seguro: ≤ 54 µg/m³"
        },
        "luminosity": {
            "safe": (30, 80), "warning": (20, 100),
            "description": "Nível recomendado: 30-80%"
        },
        "power_consumption": {
            "safe": (0, 100), "warning": (100, 200), "critical": (200, float('inf')),
            "description": "Consumo normal: 0-100W"
        }
    }
    return ranges.get(metric_key, {
        "description": f"Métrica contínua: {metric_key}"
    })

def aqi_category(v: float) -> str:
    """Traduz valor numérico do AQI para categoria de impacto ambiental."""
    if v <= 50:   return "🟢 Bom"
    if v <= 100:  return "🟡 Moderado"
    if v <= 150:  return "🟠 Insalubre (sensíveis)"
    if v <= 200:  return "🔴 Insalubre"
    if v <= 300:  return "🟣 Muito Insalubre"
    return "⚫ Perigoso"

# ====================================================================
# MAPAS DE CONFIGURAÇÃO DE DOMÍNIO
# ====================================================================

TYPE_MAP = {
    messages_pb2.DEVICE_TYPE_TRAFFIC_LIGHT: "🚦 Semáforo",
    messages_pb2.DEVICE_TYPE_LAMP_POST: "💡 Poste Inteligente",
    messages_pb2.DEVICE_TYPE_WEATHER_STATION: "🌦️ Estação Met.",
    messages_pb2.DEVICE_TYPE_CAMERA: "📹 Câmera de Tráfego",
    messages_pb2.DEVICE_TYPE_AIR_QUALITY: "💨 Qualidade do Ar"
}

STATUS_MAP = {
    messages_pb2.STATUS_ON: "🟢 ONLINE",
    messages_pb2.STATUS_OFF: "⚪ OFFLINE",
    messages_pb2.STATUS_ERROR: "🔴 FALHA"
}

METRIC_ICONS = {
    "temperature": "🌡️", "humidity": "💧", "co2": "🌿",
    "pm25": "🌫️", "pm10": "💨", "aqi": "🏭",
    "luminosity": "💡", "power_consumption": "⚡", "state": "🚦",
    "vehicles_count": "🚗", "infractions": "📸",
}

METRIC_UNITS = {
    "temperature": "°C", "humidity": "%", "co2": "ppm",
    "pm25": "µg/m³", "pm10": "µg/m³", "aqi": "",
    "luminosity": "%", "power_consumption": "W", "state": "",
    "vehicles_count": "veh/min", "infractions": "count",
}

# ====================================================================
# INICIALIZAÇÃO DA INTERFACE STREAMLIT
# ====================================================================

st.set_page_config(
    page_title="Smart City Analytics Client",
    page_icon="🏙️",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("🏙️ Centro de Controle Analítico - Smart City")
st.markdown("Monitoramento distribuído, controle operacional e agregação estatística via Sockets TCP/Protobuf.")

with st.sidebar:
    st.subheader("📊 Status do Sistema")
    col1, col2, col3 = st.columns(3)
    gateway_status = "🟢 Ativo" if check_gateway_status() else "🔴 Inativo"
    col1.metric("Gateway", gateway_status)
    sensor_count = len(st.session_state.device_history) if 'device_history' in st.session_state else "N/A"
    col2.metric("Sensores", sensor_count, help="Atualizar na aba Descoberta")
    col3.metric("Hora UTC", datetime.datetime.utcnow().strftime('%H:%M:%S'))
    st.markdown("---")
    st.info("💡 Selecione uma aba para iniciar operações na rede.")

st.divider()

# ====================================================================
# GERENCIAMENTO DE ESTADO EM MEMÓRIA
# ====================================================================

if 'device_history' not in st.session_state:
    st.session_state.device_history = []
if 'command_history' not in st.session_state:
    st.session_state.command_history = []
if 'last_cmd_result' not in st.session_state:
    st.session_state.last_cmd_result = None
if 'selected_device_id' not in st.session_state:
    st.session_state.selected_device_id = None

tab1, tab2, tab3 = st.tabs([
    "📡 Fontes de Dados (Descoberta)", 
    "⚙️ Painel de Atuação (Controle)", 
    "📊 Consultas Analíticas (OLAP)"
])

# --------------------------------------------------------------------
# ABA 1: Topologia de Descoberta
# --------------------------------------------------------------------
with tab1:
    st.subheader("📡 Nós Operacionais Registrados no Gateway")
    
    col_btn, col_refresh = st.columns([3, 1])
    with col_btn:
        if st.button("Atualizar Topologia de Rede", type="primary", use_container_width=True):
            req = messages_pb2.ClientRequest()
            req.type = messages_pb2.REQUEST_TYPE_LIST_DEVICES
            
            with st.spinner("Sincronizando inventário via Socket TCP..."):
                resp = send_tcp_request(req)
                
            if resp and resp.success:
                st.session_state.device_history = resp.devices
                if not resp.devices:
                    st.info("✓ Nenhum dispositivo descoberto pelo Gateway até o momento.")
                else:
                    st.success(f"✓ {len(resp.devices)} nó(s) identificado(s) com sucesso.")
            elif resp:
                st.warning(f"⚠️ Alerta do Barramento: {resp.message}")
            else:
                st.error("❌ Link TCP inativo. Falha de comunicação com o Gateway.")
    
    if st.session_state.device_history:
        device_data = []
        for d in st.session_state.device_history:
            device_data.append({
                "ID": d.device_id,
                "Setor Geográfico": infer_sector_from_device_id(d.device_id),
                "Classe do Dispositivo": TYPE_MAP.get(d.type, "Desconhecido"),
                "Status Atual": STATUS_MAP.get(d.status, "Desconhecido"),
                "Ponto de Entrada": f"{d.ip_address}:{d.control_port}" if d.is_controllable else f"{d.ip_address} (Somente UDP)",
                "Permite Controle": "✓ Sim" if d.is_controllable else "✗ Não",
                "Último ACK": datetime.datetime.fromtimestamp(d.last_seen_timestamp).strftime('%H:%M:%S')
            })
        
        df = pd.DataFrame(device_data)
        st.dataframe(df, use_container_width=True, hide_index=True)
        
        st.divider()
        col_graph, col_stats = st.columns([2, 1])
        
        with col_graph:
            status_counts = defaultdict(int)
            for d in st.session_state.device_history:
                status_label = STATUS_MAP.get(d.status, "Desconhecido")
                status_counts[status_label] += 1
            
            if status_counts:
                st.bar_chart(pd.DataFrame([
                    {"Status da Frota": status, "Nós": count}
                    for status, count in status_counts.items()
                ]).set_index("Status da Frota"))
        
        with col_stats:
            st.metric("Total de Nós Indexados", len(st.session_state.device_history))
            online_count = sum(1 for d in st.session_state.device_history if d.status == messages_pb2.STATUS_ON)
            st.metric("Instâncias Operacionais (ONLINE)", online_count)
            controllable_count = sum(1 for d in st.session_state.device_history if d.is_controllable)
            st.metric("Interfaces de Atuação Disponíveis", controllable_count)

# --------------------------------------------------------------------
# ABA 2: Comandos de Controle (RPC)
# --------------------------------------------------------------------
with tab2:
    st.subheader("⚙️ Console de Atuação Contextual")
    
    if not st.session_state.device_history:
        st.info("Topologia desconhecida. Sincronize a rede na aba 'Fontes de Dados'.")
    else:
        controllable_devices = [d for d in st.session_state.device_history if d.is_controllable]
        device_ids = [d.device_id for d in controllable_devices]
        
        col_sel, col_det = st.columns([1, 1])
        
        with col_sel:
            if st.session_state.selected_device_id not in device_ids and device_ids:
                st.session_state.selected_device_id = device_ids[0]
            
            target_id = st.selectbox(
                "Selecione o Nó Alvo de Atuação", 
                options=device_ids,
                index=device_ids.index(st.session_state.selected_device_id) if st.session_state.selected_device_id in device_ids else 0,
                key="selected_device_id"
            )
            
            selected_device = next((d for d in controllable_devices if d.device_id == target_id), None)
        
        if selected_device:
            with col_det:
                st.markdown(f"""
                **Tabela de Assinatura do Nó:**
                - **Classificação:** `{TYPE_MAP.get(selected_device.type, "Desconhecido")}`
                - **Socket Escuta:** `{selected_device.ip_address}:{selected_device.control_port}`
                - **Estado Local:** `{STATUS_MAP.get(selected_device.status, "Desconhecido")}`
                """)

            st.divider()
            
            if st.session_state.last_cmd_result:
                c_msg, c_clr = st.columns([9, 1])
                with c_msg:
                    res_type = st.session_state.last_cmd_result["type"]
                    msg = st.session_state.last_cmd_result["message"]
                    if res_type == "success": st.success(f"**Confirmação Positiva:** {msg}")
                    elif res_type == "error": st.error(f"**Falha de I/O:** {msg}")
                    elif res_type == "warning": st.warning(f"⚠️ {msg}")
                with c_clr:
                    if st.button("✕", key="clear_msg"):
                        st.session_state.last_cmd_result = None
            
            # FIX 3: Estrutura completamente reativa. st.form foi removido.
            st.write(f"### Parâmetros de Intervenção: `{target_id}`")
            
            c1, c2 = st.columns(2)
            
            with c1:
                alterar_status = st.checkbox("Engatilhar Mutação de Estado", value=False)
                
                if selected_device.type == messages_pb2.DEVICE_TYPE_TRAFFIC_LIGHT:
                    status_options = ["Ligar (VERDE)", "Desligar (OFF)", "Emergência (PISCANTE)"]
                elif selected_device.type == messages_pb2.DEVICE_TYPE_LAMP_POST:
                    status_options = ["Acender Relé (ON)", "Cortar Relé (OFF)", "Modo Manutenção (ERR)"]
                elif selected_device.type == messages_pb2.DEVICE_TYPE_CAMERA: 
                    status_options = ["Gravar Stream (ON)", "Pausar Stream (OFF)", "Diagnóstico Binário (ERR)"]
                else:
                    status_options = ["Ativar", "Desativar", "Provocar Falha"]
                    
                novo_status_label = st.radio("Seletor de Estado Desejado", status_options, disabled=not alterar_status)
            
            with c2:
                alterar_freq = st.checkbox("Substituir Relógio de Telemetria", value=False)
                nova_freq = st.slider("Duty Cycle (Segundos/Datagrama)", 1, 60, 5, 
                                      help="Altera a agressividade com que o nó dispara pacotes UDP.", 
                                      disabled=not alterar_freq)

            st.markdown("---")
            
            # Submissão direta e avaliação reativa dos valores correntes no dashboard
            if st.button("Transmitir Payload de Controle (TCP)", type="primary", use_container_width=True):
                if not alterar_status and not alterar_freq:
                    st.session_state.last_cmd_result = {"type": "warning", "message": "Nenhum parâmetro selecionado para sobreposição."}
                    st.rerun()
                else:
                    req = messages_pb2.ClientRequest()
                    req.type = messages_pb2.REQUEST_TYPE_SEND_COMMAND
                    req.target_device_id = target_id
                    
                    cmd = req.command_payload
                    cmd.command_id = f"CMD-{uuid.uuid4().hex[:6].upper()}"
                    
                    if alterar_status:
                        cmd.update_status = True
                        if any(x in novo_status_label for x in ["Ligar", "Acender", "Gravar", "Ativar"]):
                            cmd.target_status = messages_pb2.STATUS_ON
                        elif any(x in novo_status_label for x in ["Desligar", "Cortar", "Pausar", "Desativar"]):
                            cmd.target_status = messages_pb2.STATUS_OFF
                        else:
                            cmd.target_status = messages_pb2.STATUS_ERROR
                            
                    if alterar_freq:
                        cmd.update_frequency = True
                        cmd.new_frequency_secs = int(nova_freq)
                    
                    with st.spinner(f"Bloqueando para I/O: Enviando frame ao Gateway e aguardando ACK do sensor..."):
                        resp = send_tcp_request(req)
                        
                    if resp:
                        ts = datetime.datetime.now().strftime('%H:%M:%S')
                        st.session_state.command_history.append({
                            "timestamp": ts,
                            "device": target_id,
                            "command_id": cmd.command_id,
                            "status": "✓ Aprovado" if resp.success else "✗ Recusado",
                            "message": resp.message
                        })
                        
                        if resp.success:
                            # Reconciliação do estado no buffer da UI para não exigir novo "Atualizar Topologia"
                            for device in st.session_state.device_history:
                                if device.device_id == target_id:
                                    if cmd.update_status:
                                        device.status = cmd.target_status
                                    device.last_seen_timestamp = int(time.time())
                                    break
                            
                            st.session_state.last_cmd_result = {"type": "success", "message": resp.message}
                            st.rerun()
                        else:
                            st.session_state.last_cmd_result = {"type": "error", "message": resp.message}
                            st.rerun()
            
            st.divider()
            st.subheader("📜 Auditoria de Atuação Recente")
            
            if st.session_state.command_history:
                history_df = pd.DataFrame(st.session_state.command_history[-10:])
                history_df = history_df[['timestamp', 'device', 'command_id', 'status', 'message']]
                history_df.columns = ['Ocorrência', 'Endereço Lógico', 'Hash do Comando', '✓ Resultado', 'Retorno I/O']
                st.dataframe(history_df.iloc[::-1], use_container_width=True, hide_index=True)
            else:
                st.info("📋 Tabela de auditoria vazia. Os comandos executados nesta sessão aparecerão aqui.")

# --------------------------------------------------------------------
# ABA 3: Análise Multidimensional (OLAP)
# --------------------------------------------------------------------
with tab3:
    st.subheader("📊 Agregação Analítica Servidor-Lado (SQLite WAL)")
    
    c_op, c_met, c_time = st.columns(3)
    
    with c_op:
        operacao = st.selectbox("Função de Avaliação Numérica", [
            ("📈 Média Aritmética Amostral", messages_pb2.OP_AVERAGE),
            ("📊 Desvio Padrão Populacional", messages_pb2.OP_STD_DEV),
            ("🔀 Cálculo de Maior Variação Geográfica", messages_pb2.OP_MAX_VARIATION),
        ], format_func=lambda x: x[0])
        
    with c_met:
        metrica_alvo = st.selectbox("Vetor de Telemetria", [
            ("🌡️ Temperatura (°C)",              "temperature"),
            ("💧 Umidade Relativa (%)",            "humidity"),
            ("🌿 CO₂ (ppm)",                      "co2"),
            ("🌫️ PM2.5 — Partículas Finas",      "pm25"),
            ("💨 PM10 — Partículas Grossas",     "pm10"),
            ("🏭 AQI — Índice Base EPA",         "aqi"),
            ("💡 Luxmetria Resultante (%)",      "luminosity"),
            ("⚡ Drenagem Energética (W)",       "power_consumption"),
            ("🚗 Fluxo Veicular Direto",         "vehicles_count"),
            ("📸 Taxa de Infrações Corrente",    "infractions"),
        ], format_func=lambda x: x[0])
        
    with c_time:
        janela_horas = st.slider("Fatia Temporal Histórica (Horas passadas)", min_value=1, max_value=24, value=1)

    if st.button("Disparar Query ao Gateway", type="primary", use_container_width=True):
        req = messages_pb2.ClientRequest()
        req.type = messages_pb2.REQUEST_TYPE_ANALYTICS_QUERY
        req.query_op = operacao[1]
        req.query_metric = metrica_alvo[1]
        
        # Resolução vetorial do tempo via UNIX Epochs
        agora = int(time.time())
        req.end_timestamp = agora
        req.start_timestamp = agora - (janela_horas * 3600)
        
        with st.spinner(f"⏳ Processando {operacao[0].lower()} e extraindo grafos..."):
            resp = send_tcp_request(req)
            
        if resp:
            if resp.success:
                st.divider()
                
                col_result, col_metadata = st.columns([2, 1])
                
                with col_result:
                    icon  = METRIC_ICONS.get(metrica_alvo[1], "📊")
                    unit  = METRIC_UNITS.get(metrica_alvo[1], "")
                    value = resp.analytics_result
                    
                    label_desc = operacao[0].split(" ")[1] if "Maior" not in operacao[0] else "Variação"
                    st.metric(label=f"{icon} {label_desc} Agregada — {metrica_alvo[0]}",
                              value=f"{value:.2f} {unit}".strip())

                    # Contextualização Paramétrica
                    ref_range = get_metric_reference_range(metrica_alvo[1])
                    if metrica_alvo[1] == "aqi":
                        st.info(f"**Detecção Automática AQI:** {aqi_category(value)}  \n"
                                "(Base EPA): 0-50 Bom · 51-100 Moderado · 101-150 Sensíveis · 151-200 Insalubre")
                    elif "safe" in ref_range and "warning" in ref_range:
                        safe_min, safe_max = ref_range["safe"]
                        warn_min, warn_max = ref_range["warning"]
                        
                        if safe_min <= value <= safe_max:
                            st.success(f"**🟢 Estável: Parâmetro operando no envelope seguro.**\n\n{ref_range.get('description', '')}")
                        elif warn_min <= value <= warn_max:
                            st.warning(f"**🟡 Atenção: Desvio tolerável do ideal.**\n\n{ref_range.get('description', '')}")
                        else:
                            st.error(f"**🔴 Crítico: Integridade térmica/física comprometida.**\n\n{ref_range.get('description', '')}")
                    
                    # FIX 4: Desenho da Linha Temporal Nativa (Isolado de sessões fantasma)
                    st.subheader(f"📈 Extração do Histórico Contínuo ({janela_horas}h)")
                    
                    if len(resp.graph_points) > 0:
                        chart_data = pd.DataFrame([
                            {
                                "Eixo X": datetime.datetime.fromtimestamp(pt.timestamp).strftime("%H:%M:%S"),
                                "Grandeza Física": pt.value,
                                "MAC Address / ID": pt.device_id
                            }
                            for pt in resp.graph_points
                        ])
                        
                        # Pivotagem protegida contra colisões atômicas do log UDP (agrupamento aritmético do milissegundo)
                        chart_pivot = pd.pivot_table(
                            chart_data, 
                            index="Eixo X", 
                            columns="MAC Address / ID", 
                            values="Grandeza Física",
                            aggfunc="mean"
                        )
                        
                        st.line_chart(chart_pivot, use_container_width=True, height=360)
                    else:
                        st.info("📋 Base de dados desprovida de medições brutas no intervalo de amostragem requisitado.")
                
                with col_metadata:
                    st.info(f"📋 **Estatística da Extração SQL:**\n\n{resp.result_metadata}\n\n**Escopo Analítico:**\n{janela_horas} hora(s)")
                    st.caption(f"**Token de Assinatura (ID):** {resp.message_id}")
                    st.caption(f"**Geração do Report:** {datetime.datetime.fromtimestamp(resp.timestamp).strftime('%H:%M:%S')}")
            else:
                st.warning(f"⚠️ Restrição Computacional do Gateway: {resp.message}")
        else:
            st.error("A requisição OLAP foi rompida por perda de Framing ou Timeout do Kernel TCP.")

    # Informações sobre o processamento
    st.divider()
    with st.expander("Como funciona o OLAP no Gateway?"):
        st.markdown("""
        O Gateway implementa **Online Analytical Processing (OLAP)** para análises rápidas:

        - **Média Aritmética**: média simples dos valores da série temporal
        - **Desvio Padrão**: variação dos dados em relação à média
        - **Maior Variação por Dispositivo**: identifica o sensor com maior amplitude (máx − mín) na janela selecionada

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