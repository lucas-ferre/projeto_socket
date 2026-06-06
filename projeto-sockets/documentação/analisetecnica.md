# Análise Técnica Unificada do Sistema Distribuído Smart City

## 1. Visão Geral

O projeto implementa uma plataforma distribuída de monitoramento urbano no modelo Hub-and-Spoke. Sensores heterogêneos enviam telemetria e descoberta para um gateway central, que persiste os dados em SQLite e atende o dashboard Streamlit por TCP.

Componentes principais:

* Gateway Python com `asyncio`, UDP, TCP persistente e SQLite WAL;
* sensores em C, Lua, Java e Python;
* dashboard Streamlit com descoberta, controle, OLAP e inspeção individual;
* contrato Protocol Buffers compartilhado;
* orquestração por Docker Compose ou Podman Compose.

## 2. Arquitetura de Rede Atual

| Função | Transporte | Porta | Observação |
|---|---:|---:|---|
| Telemetria | UDP | 5000 | Exclusiva para `DataPayload` dos sensores para o gateway |
| Cliente/Gateway | TCP | 5001 | Dashboard envia `ClientRequest` e recebe `ClientResponse` |
| Descoberta | UDP | 5002 | Sensores enviam `DiscoveryResponse` ao gateway |
| Controle Lua | TCP | 5002 | Gateway encaminha `ConfigCommand` para o sensor Lua |
| Controle Java | TCP | 5003 | Gateway encaminha `ConfigCommand` para o sensor Java |
| Controle Python | TCP | 5004 | Gateway encaminha `ConfigCommand` para o sensor Python |
| Recovery multicast | UDP | 5005 | Gateway envia `SMARTCITY_DISCOVERY_PROBE` para `239.0.0.1` |
| Dashboard web | HTTP/TCP | 8501 | Interface Streamlit exposta ao host |

A porta `5000/UDP` ficou dedicada à telemetria. O multicast de recovery usa `5005/UDP`, evitando mistura entre probes e métricas.

## 3. Contrato Protocol Buffers

Mensagens operacionais:

* `DiscoveryResponse`: anúncio de topologia e presença.
* `DataPayload`: telemetria, status corrente e métricas.
* `ConfigCommand`: comando remoto do gateway para sensores controláveis.
* `ConfigResponse`: confirmação de atuação.
* `ClientRequest` e `ClientResponse`: comunicação dashboard/gateway.

O antigo ramo `SensorData/sequence` foi removido. A telemetria real do sistema é `DataPayload`; a idempotência no gateway usa `device_id`, `timestamp` e `message_id`.

## 4. Gateway

O gateway concentra:

* ingestão UDP assíncrona de telemetria;
* descoberta UDP;
* servidor TCP persistente para o dashboard;
* proxy TCP para sensores controláveis;
* persistência SQLite com WAL;
* pool assíncrono de conexões;
* monitor de offline automático;
* probes multicast periódicos;
* checkpoint periódico do WAL.

O servidor TCP do cliente aceita múltiplos frames na mesma conexão. Isso reduz handshakes TCP, evita thrashing de sockets e permite keep-alive controlado por `TCP_CLIENT_IDLE_TIMEOUT`.

## 5. Persistência

O banco `smartcity_gateway.db` possui duas tabelas principais:

* `devices`: inventário, tipo, status, IP, porta de controle, controlabilidade e `last_seen`.
* `metrics`: séries temporais de métricas por dispositivo.

O índice `idx_metrics(metric_name, timestamp)` acelera consultas OLAP por janela temporal.

## 6. Presença e Offline Automático

O gateway atualiza `last_seen` quando recebe descoberta ou telemetria. Uma task periódica marca dispositivos como `STATUS_OFF` quando ficam mais de `DEVICE_OFFLINE_TIMEOUT_SECS` sem pacote recente.

Os sensores usam IDs estáveis por tipo, setor e ordinal, por exemplo:

* `estacao_pici_01`;
* `poste_benfica_01`;
* `semaforo_porangabussu_01`;
* `camera_pici_01`.

Isso evita dispositivos fantasmas no SQLite após reboot do container.

## 7. Sensores

| Sensor | Linguagem | Tipo | Controle | Métricas |
|---|---|---|---|---|
| `sensor_clima` | C | Estação ambiental | Não | `temperature`, `humidity`, `co2`, `pm25`, `pm10`, `aqi` |
| `sensor_posto` | Lua | Poste inteligente | TCP 5002 | `luminosity`, `power_consumption` |
| `sensor_semaforo` | Java | Semáforo | TCP 5003 | `state`, `queue_length` |
| `sensor_camera` | Python | Câmera de tráfego | TCP 5004 | `vehicles_count`, `infractions` |

Todos enviam `DiscoveryResponse` em `5002/UDP` e `DataPayload` em `5000/UDP`.

## 8. Envio Periódico e Eventos por Limiar

Além da telemetria periódica com jitter, os sensores simulam amostras intermediárias e enviam imediatamente quando um limiar relevante é cruzado.

| Sensor | Limiar de evento |
|---|---|
| C / Estação ambiental | `temperature >= 32`, `pm25 >= 35` ou `aqi >= 100` |
| Lua / Poste | `luminosity <= 80` ou `power_consumption >= 32` |
| Java / Semáforo | `queue_length >= 35` |
| Python / Câmera | `vehicles_count >= 80` ou `infractions >= 3` |

Os envios por limiar usam cooldown de 3 segundos para evitar rajadas.

## 9. Multicast Recovery e Mitigação de Thundering Herd

O gateway envia `SMARTCITY_DISCOVERY_PROBE` para `239.0.0.1:5005`. Ao receber o probe, cada sensor aguarda jitter aleatório de 0 a 2000 ms antes de reenviar `DiscoveryResponse`.

Esse atraso randômico evita que todos os sensores reanunciem simultaneamente e reduz risco de saturação do buffer UDP do gateway.

## 10. Dashboard

O dashboard Streamlit oferece:

* descoberta de dispositivos;
* painel de atuação contextual;
* consultas OLAP;
* inspeção individual por sensor;
* histórico de comandos;
* reuso de conexão TCP persistente com o gateway.

O cliente diferencia falhas como timeout, conexão recusada, frame inválido, erro Protobuf e erro de socket. A aba de comandos envia requisições em background para evitar congelamento da UI.

## 11. Healthchecks

Todos os serviços possuem healthchecks no Compose:

* gateway: TCP `5001`;
* sensor Lua: TCP `5002`;
* sensor Java: TCP `5003`;
* sensor Python: TCP `5004`;
* dashboard: TCP `8501`;
* sensor C: processo `sensor_c` vivo.

## 12. Limitações Atuais

O sistema ainda não implementa:

* autenticação;
* autorização;
* TLS;
* assinatura de mensagens;
* criptografia;
* replicação do gateway;
* banco distribuído;
* fila persistente de eventos.

O gateway ainda é ponto único de falha, e SQLite é adequado para o escopo acadêmico/local, mas não para alta escala.

## 13. Conclusão Técnica

O sistema atual apresenta uma arquitetura coerente para simulação de IoT urbana distribuída. As evoluções recentes corrigiram pontos importantes: remoção de código morto, IDs estáveis, offline automático, eventos por limiar, porta multicast dedicada, jitter de recovery, conexão TCP persistente, healthchecks e inspeção individual no dashboard.
