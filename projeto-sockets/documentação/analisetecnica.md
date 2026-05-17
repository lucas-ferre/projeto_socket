# Análise Técnica Unificada do Sistema Distribuído Smart City 

# 1. Visão Geral

O sistema implementa uma arquitetura distribuída orientada a eventos voltada para monitoramento e controle de dispositivos em um cenário de Smart City.

A plataforma é composta por:

* Gateway Central;
* Sensores distribuídos multi-linguagem;
* Dashboard analítico em Streamlit;
* Comunicação híbrida UDP/TCP;
* Persistência SQLite;
* Serialização via Protocol Buffers;
* Infraestrutura conteinerizada com Docker.

A solução demonstra integração entre:

* redes distribuídas;
* sistemas concorrentes;
* protocolos de comunicação;
* telemetria em tempo real;# Análise Técnica Unificada do Sistema Distribuído Smart City

# 1. Visão Geral

O sistema implementa uma arquitetura distribuída orientada a eventos voltada para monitoramento e controle de dispositivos em um cenário de Smart City.

A plataforma é composta por:

* Gateway Central;
* Sensores distribuídos multi-linguagem;
* Dashboard analítico em Streamlit;
* Comunicação híbrida UDP/TCP;
* Persistência SQLite;
* Serialização via Protocol Buffers;
* Infraestrutura conteinerizada com Docker.

A solução demonstra integração entre:

* redes distribuídas;
* sistemas concorrentes;
* protocolos de comunicação;
* telemetria em tempo real;
* persistência de dados;
* interoperabilidade heterogênea.

---

# 2. Arquitetura Geral

A arquitetura segue um modelo centralizado do tipo Hub-and-Spoke:

```text
Sensores -> Gateway -> Dashboard
```

O Gateway atua como núcleo operacional da plataforma.

Ele é responsável por:

* descoberta de dispositivos;
* recepção de telemetria;
* roteamento de comandos;
* persistência;
* agregações analíticas;
* coordenação geral do sistema.

A centralização reduz:

* complexidade distribuída;
* acoplamento entre sensores;
* sincronização peer-to-peer.

Entretanto, introduz:

* ponto único de falha;
* gargalo centralizado;
* dependência operacional do Gateway.

---

# 3. Camada de Comunicação

## 3.1 Segmentação de Protocolos

O sistema separa responsabilidades de rede utilizando protocolos distintos:

| Função     | Transporte | Porta |
| ---------- | ---------- | ----- |
| Telemetria | UDP        | 5000  |
| Controle   | TCP        | 5001  |
| Descoberta | UDP        | 5002  |
| Dashboard  | HTTP       | 8501  |

Essa divisão é tecnicamente adequada.

UDP foi corretamente utilizado para:

* telemetria contínua;
* baixa latência;
* redução de overhead;
* comunicação não-crítica.

TCP foi reservado para:

* comandos críticos;
* consistência de entrega;
* streams confiáveis.

---

# 4. Serialização com Protocol Buffers

O sistema utiliza Protocol Buffers como padrão de serialização binária.

Essa decisão proporciona:

* interoperabilidade multi-linguagem;
* payloads compactos;
* alta eficiência de rede;
* contratos rígidos de comunicação.

A solução permite que sensores implementados em linguagens diferentes compartilhem o mesmo protocolo.

---

# 5. Gateway Central

## 5.1 Responsabilidades

O Gateway representa o componente mais complexo da arquitetura.

Ele implementa:

* servidores UDP assíncronos;
* servidor TCP;
* processamento de telemetria;
* descoberta de dispositivos;
* persistência SQLite;
* consultas analíticas;
* controle distribuído.

Arquiteturalmente, atua simultaneamente como:

* coordinator;
* message broker;
* ingestion hub;
* analytics node.

---

# 6. Modelo de Concorrência

## 6.1 Uso de asyncio

O Gateway utiliza programação assíncrona baseada em asyncio.

Essa abordagem é adequada porque o sistema é predominantemente I/O-bound.

Os principais mecanismos utilizados incluem:

* asyncio.start_server;
* create_datagram_endpoint;
* background tasks;
* aiosqlite.

A arquitetura evita:

* excesso de threads;
* overhead de context switching;
* bloqueios desnecessários.

---

# 7. Persistência de Dados

## 7.1 SQLite com WAL

O banco SQLite opera em modo WAL:

```sql
PRAGMA journal_mode=WAL;
```

Essa configuração melhora:

* concorrência de leitura;
* throughput;
* coexistência entre leitores e escritores.

---

## 7.2 Pool Assíncrono de Conexões

O sistema implementa pool manual de conexões SQLite.

Características:

* reutilização de conexões;
* queue assíncrona;
* rollback automático;
* foreign keys habilitadas;
* busy_timeout configurado.

A implementação demonstra preocupação com:

* contenção;
* estabilidade;
* gerenciamento transacional.

---

## 7.3 Modelagem

### devices

Tabela responsável por:

* inventário;
* status;
* descoberta;
* controle.

### metrics

Tabela responsável por:

* séries temporais;
* armazenamento de telemetria;
* consultas analíticas.

---

## 7.4 Indexação

O sistema utiliza índice:

```sql
CREATE INDEX idx_metrics
ON metrics(metric_name, timestamp)
```

Isso reduz:

* full scans;
* degradação temporal;
* custo de agregações.

---

# 8. Pipeline de Telemetria

Fluxo operacional:

```text
Sensor -> UDP -> Gateway -> SQLite -> Dashboard
```

O Gateway executa:

1. recepção do datagrama;
2. desserialização;
3. validação;
4. detecção de duplicidade;
5. controle de sequência;
6. persistência assíncrona.

---

## 8.1 Controle de Duplicidade

O sistema mantém rastreamento de:

* timestamps;
* message_id.

Isso evita:

* replay;
* duplicação;
* inconsistências temporais.

---

## 8.2 Controle de Sequência

O Gateway detecta perdas utilizando controle de sequência.

A lógica compara:

```text
sequência esperada vs sequência recebida
```

Isso permite:

* detecção de packet loss;
* auditoria de integridade;
* monitoramento da qualidade da rede.

---

# 9. Descoberta de Dispositivos

Os sensores anunciam presença via UDP.

O Gateway registra:

* IP;
* porta;
* tipo;
* status;
* capacidade de controle.

O sistema também utiliza multicast:

```text
239.0.0.1
```

Esse mecanismo permite:

* descoberta dinâmica;
* reanúncio;
* sincronização da topologia.

---

# 10. Sensor Python

## 10.1 Características Gerais

O sensor Python é o componente mais sofisticado da plataforma.

Ele implementa:

* múltiplos dispositivos virtuais;
* multicast;
* controle TCP;
* telemetria dinâmica;
* jitter;
* retry exponencial;
* override manual;
* threads concorrentes.

---

## 10.2 Retry Exponencial

O sensor implementa backoff exponencial.

Isso reduz:

* tempestades de retransmissão;
* saturação de rede;
* loops agressivos de falha.

---

## 10.3 Jitter Aleatório

O jitter evita:

* sincronização artificial;
* bursts simultâneos;
* congestionamentos periódicos.

---

## 10.4 Controle TCP

O sensor utiliza:

* framing binário;
* readexact;
* validação de tamanho;
* timeout.

Isso evita:

* partial reads;
* corrupção de stream;
* fragmentação incorreta.

---

# 11. Sensor em C

## 11.1 Características Gerais

O sensor em C representa o componente mais próximo de um ambiente embarcado real.

A implementação demonstra:

* manipulação direta de sockets;
* controle explícito de memória;
* serialização binária;
* baixo overhead operacional.

---

## 11.2 Comunicação de Rede

O sensor utiliza sockets BSD tradicionais.

Isso exige:

* gerenciamento manual de buffers;
* controle explícito de recv/send;
* manipulação direta de sockaddr.

A implementação demonstra domínio de programação de rede em baixo nível.

---

## 11.3 Retry e Resiliência

O sensor implementa:

* retry automático;
* backoff exponencial;
* jitter aleatório;
* retry de DNS;
* retry de envio UDP.

A lógica inclui:

```c
retry_delay_usec()
```

O sistema também utiliza:

```c
send_udp_with_retry()
```

Essa estratégia melhora significativamente a tolerância a falhas transitórias.

---

## 11.4 Eficiência

Entre todos os sensores, o módulo em C possui:

* menor footprint;
* menor latência;
* maior eficiência computacional.

Ele é particularmente adequado para:

* edge computing;
* dispositivos embarcados;
* ambientes restritos.

---

# 12. Sensor Java

## 12.1 Estrutura Arquitetural

O sensor Java apresenta uma arquitetura mais orientada a objetos.

Características:

* encapsulamento;
* abstração;
* separação de responsabilidades;
* modularização.

---

## 12.2 Modelo de Execução

A JVM fornece:

* garbage collection;
* gerenciamento automático de memória;
* abstrações robustas de socket.

Isso reduz:

* vazamentos;
* corrupção de memória;
* falhas estruturais.

Por outro lado, aumenta:

* consumo de RAM;
* overhead de runtime.

---

## 12.3 Robustez

O sensor Java é um dos componentes mais robustos em:

* estabilidade;
* tratamento de exceções;
* manutenção;
* extensibilidade.

---

# 13. Sensor Lua

## 13.1 Natureza do Sensor

O sensor Lua representa o componente mais leve em termos de scripting.

Lua é adequada para:

* automação;
* edge scripting;
* sistemas IoT;
* integração embarcada.

---

## 13.2 Footprint

O runtime Lua possui:

* footprint reduzido;
* inicialização rápida;
* baixo consumo de memória.

---

## 13.3 Limitações

Comparado aos outros sensores, o módulo Lua possui:

* menor estruturação arquitetural;
* menor tipagem;
* menor verificabilidade estática.

Isso reduz:

* segurança estrutural;
* escalabilidade de manutenção.

---

# 14. Dashboard Streamlit

## 14.1 Papel Operacional

O dashboard Streamlit atua como:

* console operacional;
* painel analítico;
* centro de monitoramento;
* interface de controle.

---

## 14.2 Sessão Persistente

O uso de:

```python
st.session_state
```

permite:

* persistência de estado;
* cache local;
* histórico operacional.

---

## 14.3 Interface Contextual

A interface adapta:

* comandos;
* labels;
* status;
* controles.

conforme o tipo de dispositivo.

---

# 15. Consultas Analíticas

O sistema suporta:

* média;
* desvio padrão;
* filtragem temporal.

As consultas são executadas diretamente no SQLite.

---

## 15.1 Limitações Analíticas

O subsistema analítico ainda possui limitações.

Ausências importantes:

* percentis;
* mediana;
* histogramas;
* séries temporais avançadas;
* detecção de anomalias;
* janelas deslizantes.

---

# 16. Dockerização

O sistema utiliza:

* Docker;
* Docker Compose;
* bridge network;
* healthchecks.

Isso melhora:

* isolamento;
* reprodutibilidade;
* portabilidade;
* orquestração.

---

# 17. Avaliação Multi-Linguagem

A coexistência entre:

* Python;
* C;
* Java;
* Lua.

é um dos aspectos mais sofisticados do projeto.

Isso demonstra:

* independência de linguagem;
* desacoplamento arquitetural;
* padronização protocolar;
* interoperabilidade real.

---

# 18. Segurança

O sistema não implementa:

* autenticação;
* autorização;
* TLS;
* assinatura de mensagens;
* criptografia.

Isso representa uma limitação crítica para ambientes reais.

---

# 19. Escalabilidade

SQLite limita:

* throughput;
* concorrência massiva;
* escalabilidade horizontal.

Em produção, seriam recomendados:

* PostgreSQL;
* TimescaleDB;
* InfluxDB;
* Kafka.

---

# 20. Tolerância a Falhas

Atualmente não existem:

* failover;
* replicação;
* consenso distribuído;
* persistência distribuída.

O Gateway permanece como ponto único de falha.

---

# 21. Complexidade Computacional

## Ingestão UDP

```text
O(1)
```

por pacote.

---

## Inserção no Banco

```text
O(log n)
```

considerando indexação.

---

## Consultas Analíticas

```text
O(k)
```

onde:

```text
k = quantidade de registros filtrados
```

---

# 22. Principais Melhorias

## Curto Prazo

* autenticação;
* TLS;
* heartbeat dedicado;
* retries inteligentes;
* monitoramento de saúde.

---

## Médio Prazo

* PostgreSQL/TimescaleDB;
* Prometheus;
* Grafana;
* filas Kafka/RabbitMQ.

---

## Longo Prazo

* clusterização do Gateway;
* replicação;
* consenso distribuído;
* stream analytics.

---

# 23. Conclusão Técnica

O sistema implementa uma plataforma distribuída de Smart City.

A solução demonstra domínio de:

* sistemas distribuídos;
* redes;
* telemetria;
* comunicação binária;
* concorrência;
* interoperabilidade multi-linguagem;
* persistência;
* engenharia de protocolos.

Os componentes mais sofisticados do projeto incluem:

* Gateway assíncrono;
* framing TCP;
* controle de sequência;
* retry exponencial;
* descoberta multicast;
* interoperabilidade heterogênea.

Apesar das limitações relacionadas a:

* segurança;
* escalabilidade;
* tolerância a falhas;
* persistência distribuída;

o sistema já apresenta características compatíveis com arquiteturas reais de edge computing e IoT distribuída.