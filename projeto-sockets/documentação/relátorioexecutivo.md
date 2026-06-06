# Relatório Executivo — Sistema Distribuído Smart City

## 1. Visão Geral

O projeto simula uma infraestrutura urbana inteligente com sensores distribuídos, gateway central, banco SQLite e dashboard web. A plataforma coleta telemetria, registra dispositivos, permite comandos remotos e executa consultas analíticas.

Fluxo principal:

```text
Sensores -> Gateway -> SQLite -> Dashboard
```

O sistema demonstra conceitos de sistemas distribuídos, IoT, sockets UDP/TCP, Protocol Buffers, concorrência e interoperabilidade multi-linguagem.

## 2. Componentes

| Componente | Função |
|---|---|
| Gateway | Ingestão, descoberta, persistência, proxy de comandos e analytics |
| Dashboard | Visualização, atuação remota, OLAP e inspeção individual |
| Sensor C | Estações ambientais |
| Sensor Lua | Postes inteligentes controláveis |
| Sensor Java | Semáforos controláveis |
| Sensor Python | Câmeras de tráfego controláveis |

## 3. Comunicação

| Canal | Porta | Uso |
|---|---:|---|
| UDP | 5000 | Telemetria `DataPayload` |
| TCP | 5001 | Dashboard/Gateway |
| UDP | 5002 | Descoberta `DiscoveryResponse` |
| TCP | 5002 | Controle do sensor Lua |
| TCP | 5003 | Controle do sensor Java |
| TCP | 5004 | Controle do sensor Python |
| UDP multicast | 5005 | Recovery probes `SMARTCITY_DISCOVERY_PROBE` |
| HTTP/TCP | 8501 | Dashboard Streamlit |

O multicast foi separado da telemetria para evitar mistura de mensagens na porta `5000/UDP`.

## 4. Protocolo de Dados

O contrato é definido em `common/messages.proto`.

Mensagens principais:

* `DiscoveryResponse`: presença e topologia;
* `DataPayload`: status e métricas;
* `ConfigCommand` e `ConfigResponse`: atuação remota;
* `ClientRequest` e `ClientResponse`: comunicação do dashboard com o gateway.

O gateway não usa mais `SensorData` nem controle de sequência. A consistência operacional é feita por `message_id`, `timestamp` e `device_id`.

## 5. Recursos Implementados

O sistema atual possui:

* IDs estáveis por tipo/setor/ordinal;
* detecção automática de offline;
* envio periódico com jitter;
* envio imediato por limiar;
* retry UDP com backoff exponencial;
* jitter de 0 a 2000 ms nas respostas multicast;
* TCP persistente entre dashboard e gateway;
* classificação de erros TCP no cliente;
* healthchecks para todos os containers;
* dashboard com inspeção individual de sensores.

## 6. Métricas

| Sensor | Métricas |
|---|---|
| C / Estação ambiental | `temperature`, `humidity`, `co2`, `pm25`, `pm10`, `aqi` |
| Lua / Poste | `luminosity`, `power_consumption` |
| Java / Semáforo | `state`, `queue_length` |
| Python / Câmera | `vehicles_count`, `infractions` |

## 7. Resiliência

O gateway registra `last_seen` e marca dispositivos como `STATUS_OFF` quando ultrapassam o timeout configurado. Probes multicast periódicos pedem aos sensores que reanunciem a topologia, e o jitter de resposta evita thundering herd.

O Compose usa healthchecks para coordenar inicialização e indicar falhas operacionais.

## 8. Limitações

Ainda não há:

* autenticação;
* TLS;
* autorização por perfil;
* replicação;
* failover;
* banco de séries temporais dedicado.

O gateway continua sendo ponto único de falha, mas a arquitetura está adequada ao objetivo acadêmico de simular comunicação distribuída heterogênea.

## 9. Conclusão

A versão atual está mais robusta que a versão inicial: reduz código morto, evita dispositivos fantasmas, separa portas por finalidade, adiciona recovery com jitter, melhora o cliente TCP e amplia observabilidade por healthchecks e inspeção individual.
