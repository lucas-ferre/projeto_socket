# Smart City — Sistema Distribuído de Monitoramento Urbano

Plataforma de telemetria e controle para dispositivos urbanos distribuídos.
Sensores heterogêneos (C, Lua, Java, Python) comunicam-se com um gateway central via
**UDP/TCP + Protocol Buffers**, com persistência em SQLite e dashboard analítico em Streamlit.

---

## Sumário

1. [Arquitetura](#arquitetura)
2. [Stack Tecnológico](#stack-tecnológico)
3. [Quick Start](#quick-start)
4. [Serviços e Portas](#serviços-e-portas)
5. [Variáveis de Ambiente](#variáveis-de-ambiente)
6. [Catálogo de Métricas](#catálogo-de-métricas)
7. [Protocolo de Comunicação](#protocolo-de-comunicação)
8. [Frota Multi-Dispositivo](#frota-multi-dispositivo)
9. [Operações do Dashboard](#operações-do-dashboard)
10. [Resiliência de Rede](#resiliência-de-rede)
11. [Estrutura do Projeto](#estrutura-do-projeto)

---

## Arquitetura

O sistema segue o modelo **Hub-and-Spoke**: todos os sensores se comunicam exclusivamente com o Gateway central.

```
                         Rede Compose: smart_city_net
┌────────────────────────────────────────────────────────────────────┐
│                                                                    │
│  ┌──────────────────┐   UDP :5000 (Telemetria)                    │
│  │  sensor_clima    │──────────────────────────────┐              │
│  │  C · 3 Estações  │   UDP :5002 (Descoberta)     │              │
│  │  Pici/Benf/Poran.│──────────────────────────────┤              │
│  └──────────────────┘                              ▼              │
│                                          ┌──────────────────────┐ │
│  ┌──────────────────┐   UDP :5000 ──────▶│      gateway         │ │
│  │  sensor_posto    │   UDP :5002 ──────▶│  Python / asyncio    │ │
│  │  Lua · 3 Postes  │◀── TCP :5002 ──────│                      │ │
│  └──────────────────┘                    │  SQLite WAL          │ │
│                                          │  Pool aiosqlite      │ │
│  ┌──────────────────┐   UDP :5000 ──────▶│  Framing TCP         │ │
│  │  sensor_semaforo │   UDP :5002 ──────▶│                      │ │
│  │  Java · 3 Semáf. │◀── TCP :5003 ──────│  :5000/UDP ingestão  │ │
│  └──────────────────┘                    │  :5002/UDP descoberta│ │
│                                          │  :5001/TCP cliente   │ │
│  ┌──────────────────┐   UDP :5000 ──────▶└──────────────────────┘ │
│  │  sensor_camera   │   UDP :5002 ──────▶          ▲              │
│  │  Python · 3 Câm. │◀── TCP :5004 ──────          │ TCP :5001    │
│  └──────────────────┘                    ┌──────────────────────┐ │
│                                          │     dashboard        │ │
│  ←── Multicast 239.0.0.1 ────────────────│  Streamlit :8501     │ │
│       Recovery Probes (UDP :5005)        └──────────────────────┘ │
└────────────────────────────────────────────────────────────────────┘
                                                    │ :8501
                                                    ▼
                                              Navegador Web
```

### Fluxo de dados

| Fase | Protocolo | Descrição |
|------|-----------|-----------|
| **Registro** | UDP :5002 | Sensor envia `DiscoveryResponse` → Gateway persiste dispositivo no SQLite |
| **Telemetria** | UDP :5000 | Sensor envia `DataPayload` com métricas → Gateway valida, desduplicando e persiste |
| **Controle** | TCP :500x | Dashboard envia `ConfigCommand` via Gateway → Gateway faz proxy para sensor alvo |
| **Analítica** | TCP :5001 | Dashboard requisita agregação OLAP → Gateway processa e retorna escalar |
| **Recuperação** | Multicast | Gateway transmite probe → Sensores re-enviam `DiscoveryResponse` |

---

## Stack Tecnológico

| Componente | Linguagem | Runtime | Biblioteca principal |
|-----------|-----------|---------|---------------------|
| Gateway | Python 3.11 | asyncio | `aiosqlite`, `protobuf` |
| Dashboard | Python 3.11 | Streamlit | `protobuf`, `pandas` |
| Sensor Clima | C (C11) | POSIX/pthreads | `protobuf-c` |
| Sensor Poste | Lua 5.4 | LuaSocket | `lua-protobuf` |
| Sensor Semáforo | Java 17 | JVM | `protobuf-java` |
| Sensor Câmera | Python 3.11 | threading | `protobuf` |
| Serialização | — | — | Protocol Buffers 3 |
| Persistência | — | — | SQLite 3 (WAL mode) |
| Orquestração | — | Docker Compose ou Podman Compose | — |

---

## Quick Start

### Pré-requisitos

Escolha uma das opções:

- [Docker Engine](https://docs.docker.com/engine/install/) >= 24 com [Docker Compose](https://docs.docker.com/compose/install/) plugin v2
- Podman instalado e acessível no PATH, com suporte a Compose (`podman compose`) ou o binário `podman-compose`

### Executar o sistema completo

Com Docker:

```bash
# Build e inicialização de todos os serviços
docker compose up --build

# Modo background
docker compose up --build -d

# Acompanhar logs em tempo real
docker compose logs -f gateway sensor_clima sensor_posto

# Parar tudo
docker compose down
```

Com Podman:

```bash
# Build e inicialização de todos os serviços
podman compose up --build

# Modo background
podman compose up --build -d

# Acompanhar logs em tempo real
podman compose logs -f gateway sensor_clima sensor_posto

# Parar tudo
podman compose down
```

Se a instalação expuser apenas `podman-compose`, use `podman-compose` no lugar de `podman compose`. No Windows, confira `podman compose version`: se a saída informar que está executando `docker-compose.exe` como provider externo, instale/use `podman-compose` ou configure `PODMAN_COMPOSE_PROVIDER=podman-compose` para evitar depender do Docker Compose. O arquivo `docker-compose.yml` é mantido como fonte única para os dois runtimes.

No PowerShell, execute os comandos a partir da pasta que contém o `docker-compose.yml`:

```powershell
cd .\projeto-sockets
podman compose up --build
```

Se o terminal ainda não reconhecer `podman` após a instalação, ou se você estiver na pasta acima do projeto, use o helper:

```powershell
.\projeto-sockets\scripts\podman-compose.ps1 up --build
```

### Acessar o dashboard

Após a inicialização (aguarde os healthchecks dos serviços ficarem saudáveis):

```
http://localhost:8501
```

### Inspecionar o banco de dados

```bash
# Dispositivos registrados
docker exec gateway sqlite3 db/smartcity_gateway.db \
  "SELECT device_id, type, status, last_seen FROM devices;"

# Métricas mais recentes
docker exec gateway sqlite3 db/smartcity_gateway.db \
  "SELECT device_id, metric_name, value, unit FROM metrics ORDER BY id DESC LIMIT 20;"
```

Com Podman:

```bash
podman exec gateway sqlite3 db/smartcity_gateway.db \
  "SELECT device_id, type, status, last_seen FROM devices;"

podman exec gateway sqlite3 db/smartcity_gateway.db \
  "SELECT device_id, metric_name, value, unit FROM metrics ORDER BY id DESC LIMIT 20;"
```

---

## Serviços e Portas

### Expostas ao host

| Serviço | Porta | Protocolo | Descrição |
|---------|-------|-----------|-----------|
| `gateway` | **5000** | UDP | Ingestão de telemetria contínua |
| `gateway` | **5001** | TCP | Interface cliente (dashboard) |
| `dashboard` | **8501** | TCP/HTTP | Interface web Streamlit |

### Internas (rede Compose)

| Serviço | Porta | Protocolo | Descrição |
|---------|-------|-----------|-----------|
| `gateway` | 5002 | UDP | Recepção de handshakes de descoberta |
| `sensor_posto` | 5002 | TCP | Servidor de controle (Lua) |
| `sensor_semaforo` | 5003 | TCP | Servidor de controle (Java) |
| `sensor_camera` | 5004 | TCP | Servidor de controle (Python) |
| Multicast | 5005 | UDP | Grupo 239.0.0.1 — probes de recovery |

> O sensor C não possui porta TCP — opera exclusivamente como emissor UDP.

---

## Variáveis de Ambiente

Configure no `docker-compose.yml` sob a chave `environment:` de cada serviço.

### gateway

| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `DISCOVERY_PROBE_INTERVAL_SECS` | `15` | Intervalo dos probes multicast que pedem aos sensores para reanunciar a topologia |
| `MULTICAST_TTL` | `1` | TTL dos pacotes multicast de recuperação |
| `DEVICE_OFFLINE_TIMEOUT_SECS` | `45` | Tempo máximo sem `last_seen` antes de marcar o dispositivo como offline |
| `DEVICE_OFFLINE_CHECK_INTERVAL_SECS` | `5` | Intervalo da varredura de presença no SQLite |
| `TCP_CLIENT_READ_TIMEOUT` | `10` | Timeout para ler cabeçalho/payload de um frame TCP já iniciado |
| `TCP_CLIENT_IDLE_TIMEOUT` | `60` | Tempo que uma conexão TCP persistente pode ficar ociosa antes de ser fechada |
| `TCP_MAX_FRAME_BYTES` | `1048576` | Tamanho máximo aceito para frames TCP do cliente |
| `DB_POOL_SIZE` | `4` | Tamanho do pool de conexões SQLite |

### sensor_clima (C)

| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `C_DEVICE_COUNT` | `3` | Número de estações ambientais simuladas |

### sensor_posto (Lua)

| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `LUA_DEVICE_COUNT` | `3` | Número de postes simulados |
| `LUMINOSITY_LOW_THRESHOLD` | `80` | Limiar inferior que dispara evento imediato de luminosidade |
| `POWER_CONSUMPTION_THRESHOLD` | `32` | Limiar superior que dispara evento imediato de consumo |

### sensor_semaforo (Java)

| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `JAVA_DEVICE_COUNT` | `3` | Número de semáforos simulados |
| `TRAFFIC_QUEUE_THRESHOLD` | `35` | Limiar de fila veicular que dispara evento imediato |

### sensor_camera (Python)

| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `CAMERA_DEVICE_COUNT` | `3` | Número de câmeras simuladas |
| `TRAFFIC_VEHICLES_THRESHOLD` | `80` | Limiar de veículos por minuto que dispara evento imediato |
| `TRAFFIC_INFRACTIONS_THRESHOLD` | `3` | Limiar de infrações que dispara evento imediato |

---

## Catálogo de Métricas

Todas as métricas são transmitidas como campos `Metric { name, value, unit }` dentro do `DataPayload`.

### Sensor Clima — sensor_clima (C)

Simula 3 estações ambientais nos setores Pici, Benfica e Porangabussu.

| Métrica | Unidade | Faixa simulada | Descrição |
|---------|---------|---------------|-----------|
| `temperature` | °C | 25 – 35 | Temperatura do ar |
| `humidity` | % | 55 – 90 | Umidade relativa |
| `co2` | ppm | 400 – 600 | Concentração de CO₂ |
| `pm25` | µg/m³ | 5 – 45 | Material particulado fino (PM2.5) |
| `pm10` | µg/m³ | pm25 + 5–25 | Material particulado grosso (PM10) |
| `aqi` | índice | 0 – 500 | Índice de Qualidade do Ar (padrão EPA) |

**Referência AQI (EPA):**
`0–50` Bom · `51–100` Moderado · `101–150` Insalubre (sensíveis) · `151–200` Insalubre · `201–300` Muito insalubre · `>300` Perigoso

### Sensor Poste — sensor_posto (Lua)

Simula 3 postes inteligentes com controle de luminosidade.

| Métrica | Unidade | Faixa simulada | Descrição |
|---------|---------|---------------|-----------|
| `luminosity` | % | 75 – 100 | Intensidade da iluminação |
| `power_consumption` | W | 25 – 35 | Consumo elétrico instantâneo |

### Sensor Semáforo — sensor_semaforo (Java)

Simula 3 semáforos com ciclo operacional configurável.

| Métrica | Unidade | Valor | Descrição |
|---------|---------|-------|-----------|
| `state` | code | `1` | Estado do ciclo (emitido apenas quando STATUS_ON) |
| `queue_length` | vehicles | 5 – 50 | Fila simulada no cruzamento, usada para disparo por limiar |

### Sensor Câmera — sensor_camera (Python)

Simula 3 câmeras de tráfego com detecção de infrações.

| Métrica | Unidade | Faixa simulada | Descrição |
|---------|---------|---------------|-----------|
| `vehicles_count` | veh/min | 12 – 95 | Veículos detectados por minuto |
| `infractions` | count | 0 – ~5 | Infrações registradas no intervalo |

> Taxa de infrações: ~2.5% por veículo (fator 1.8× em pico de tráfego acima de 55 veh/min).

---

## Protocolo de Comunicação

### Serialização

Todos os pacotes usam **Protocol Buffers 3** definidos em `common/messages.proto`.

Mensagens principais:

| Mensagem | Direção | Canal |
|----------|---------|-------|
| `DiscoveryResponse` | Sensor → Gateway | UDP :5002 |
| `DataPayload` | Sensor → Gateway | UDP :5000 |
| `ConfigCommand` | Gateway → Sensor | TCP :500x |
| `ConfigResponse` | Sensor → Gateway | TCP :500x |
| `ClientRequest` | Dashboard → Gateway | TCP :5001 |
| `ClientResponse` | Gateway → Dashboard | TCP :5001 |

### Framing TCP (Length-Prefix)

Toda comunicação TCP usa prefixo de 4 bytes Big-Endian:

```
┌────────────────┬──────────────────────────────┐
│  4 bytes (>I)  │  N bytes (Protobuf payload)  │
│  uint32 BE     │                              │
└────────────────┴──────────────────────────────┘
```

### Idempotência

Cada mensagem carrega `message_id` único. O gateway detecta e descarta:
- **Mensagens duplicadas** — mesmo `device_id` + `timestamp` + `message_id`
- **Mensagens atrasadas** — `timestamp` anterior ao último processado do mesmo dispositivo

---

## Frota Multi-Dispositivo

Cada nó sensor pode simular múltiplos dispositivos independentes no mesmo container,
distribuídos pelos 3 setores disponíveis: **Pici**, **Benfica** e **Porangabussu**.

```yaml
# Exemplo: escalar para 6 câmeras (2 por setor)
sensor_camera:
  environment:
    - CAMERA_DEVICE_COUNT=6
```

Cada dispositivo da frota:
- Recebe `device_id` estável por tipo/setor/ordinal (ex.: `camera_pici_01`)
- Mantém estado independente (status, frequência de envio)
- É registrado individualmente no Gateway
- Pode ser controlado individualmente pelo dashboard

**Ciclo de status automático:**

| Status | Probabilidade |
|--------|--------------|
| `STATUS_ON` | 78% |
| `STATUS_OFF` | 12% |
| `STATUS_ERROR` | 10% |

Após receber um comando manual, o status fica **bloqueado por 30 segundos** antes de retomar a variação automática.

---

## Operações do Dashboard

### Aba 1 — Fontes de Dados

Consulta todos os dispositivos registrados no gateway.

Exibe: ID, setor, tipo, status, endereço de controle, controlável, último contato.

### Aba 2 — Painel de Atuação

Envia comandos de configuração para dispositivos controláveis.

| Campo | Opções |
|-------|--------|
| Dispositivo alvo | Selecionado entre os controláveis registrados |
| Novo status | `STATUS_ON`, `STATUS_OFF`, `STATUS_ERROR` (rótulos contextuais por tipo) |
| Frequência | Intervalo entre envios UDP (1–60 segundos) |

Fluxo: `Dashboard → ClientRequest(SEND_COMMAND) → Gateway (TCP :5001) → Sensor alvo (TCP :500x) → ConfigResponse`

### Aba 3 — Consultas Analíticas (OLAP)

O processamento estatístico ocorre inteiramente no gateway. O cliente recebe apenas o escalar resultante.

| Operação | Enum | Descrição |
|----------|------|-----------|
| Média Aritmética | `OP_AVERAGE` | Média simples sobre a janela temporal |
| Desvio Padrão | `OP_STD_DEV` | Dispersão em relação à média |

Parâmetros:
- **Métrica alvo** — 12 disponíveis (ver catálogo acima)
- **Janela temporal** — últimas 1 a 24 horas

---

## Resiliência de Rede

### Retry com backoff exponencial + jitter

Todos os sensores implementam retentativas com atraso crescente em UDP e DNS:

```
Tentativa 1 →  200 ms + jitter aleatório
Tentativa 2 →  400 ms + jitter aleatório
Tentativa 3 →  800 ms + jitter aleatório  (máx. 1500 ms)
```

### Redescoberta automática (Multicast Recovery)

O gateway transmite `SMARTCITY_DISCOVERY_PROBE` via multicast `239.0.0.1:5005` a cada 15 segundos por padrão.
Todos os sensores escutam o grupo e re-enviam `DiscoveryResponse`, garantindo recuperação após reinicialização do gateway sem intervenção manual. A porta multicast é dedicada para não misturar probes de recovery com telemetria `DataPayload` em `5000/UDP`.

### Jitter de telemetria

Cada ciclo de envio inclui atraso aleatório (até ±350 ms) para evitar sincronização de envios em frotas grandes e reduzir colisões UDP.

### Eventos imediatos por limiar

Além do envio periódico, os sensores simulam amostras intermediárias e emitem um `DataPayload` extra quando um valor crítico cruza o limiar definido. O envio por limiar usa cooldown de 3 segundos para evitar rajadas e não altera a próxima janela periódica.

| Sensor | Limiar de evento |
|--------|------------------|
| C / Estação ambiental | `temperature >= 32°C`, `pm25 >= 35 µg/m³` ou `aqi >= 100` |
| Lua / Poste | `luminosity <= 80%` ou `power_consumption >= 32 W` |
| Java / Semáforo | `queue_length >= 35 veículos` |
| Python / Câmera | `vehicles_count >= 80` ou `infractions >= 3` |

### Detecção automática de offline

O gateway marca dispositivos sem pacotes recentes como `STATUS_OFF` usando `DEVICE_OFFLINE_TIMEOUT_SECS`. Como os IDs agora são estáveis, reiniciar um sensor atualiza o mesmo registro no SQLite em vez de criar uma nova chave fantasma.

### Graceful Shutdown

| Sensor | Mecanismo |
|--------|-----------|
| C | `sigaction(SIGTERM/SIGINT)` → cancela thread POSIX → libera sockets e DNS |
| Python | `signal.signal` → `threading.Event` → threads daemon encerram com o processo |
| Java | threads separadas; encerramento natural no `System.exit` |
| Lua | event-loop síncrono; sem shutdown explícito necessário |

---

## Estrutura do Projeto

```
projeto-sockets/
│
├── common/
│   └── messages.proto          # Contrato Protobuf compartilhado entre todos os serviços
│
├── gateway/
│   ├── main.py                 # Hub central asyncio + aiosqlite + pool de conexões
│   └── Dockerfile
│
├── client/
│   ├── app.py                  # Dashboard Streamlit (descoberta, controle, OLAP)
│   └── Dockerfile
│
├── sensor_c/
│   ├── sensor.c                # Estação ambiental POSIX/pthreads — 6 métricas + AQI (EPA)
│   └── Dockerfile
│
├── sensor_lua/
│   ├── sensor.lua              # Poste inteligente — event-loop cooperativo multi-dispositivo
│   └── Dockerfile              # Compila messages.pb via protoc no build
│
├── sensor_java/
│   ├── sensor.java             # Semáforo JVM — threads separadas para TCP, multicast e telemetria
│   └── Dockerfile              # Build multi-estágio JDK 17 → JRE slim
│
├── sensor_python/
│   ├── sensor.py               # Câmera de tráfego — threading + shutdown via Event
│   └── Dockerfile
│
├── Dockerfile                  # Fallback multi-stage usado por Podman Compose no Windows
└── docker-compose.yml          # Orquestração compatível com Docker Compose e Podman Compose
```

---

## Notas de Implementação

- **SQLite WAL mode** ativado no boot para melhorar concorrência de leituras simultâneas
- **Pool de conexões** (`SQLiteConnectionPool`) com fila asyncio evita bloqueio do event loop em picos de telemetria
- **Sensor C multi-frota** registra e envia telemetria para N dispositivos no mesmo processo, com `pthread` dedicada ao listener multicast
- **Sensor Lua** implementa scheduling cooperativo manual (sem threads) via `socket.sleep` e timestamps de controle
- **Sensor Java** usa `volatile` nos campos de estado para segurança entre threads sem overhead de `synchronized` completo
- **Healthchecks** verificam o TCP :5001 do gateway, as portas TCP dos sensores controláveis, a porta web do dashboard e o processo do sensor C; os probes multicast periódicos permitem re-sincronização caso algum runtime Compose inicie serviços fora da ordem esperada
- **Dockerfile raiz** replica os builds dos serviços como estágios nomeados para contornar providers Podman Compose que ignoram `build.dockerfile`
