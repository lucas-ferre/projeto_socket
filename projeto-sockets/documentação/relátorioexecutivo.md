# Relatório Executivo — Funcionamento, Tecnologias e Mecânicas do Sistema Distribuído Smart City

# 1. Visão Geral do Sistema

O projeto consiste em uma plataforma distribuída de monitoramento e controle voltada para cenários de Smart City.

A ideia principal do sistema é simular uma infraestrutura urbana inteligente composta por diversos sensores espalhados pela cidade, cada um responsável por coletar informações específicas e enviar esses dados para um Gateway Central. Ele recebe informações dos sensores, processa os dados, armazena as métricas em banco de dados e disponibiliza tudo para um dashboard web interativo.

Na prática, o sistema implementa um fluxo contínuo de:

```text
Coleta -> Transmissão -> Processamento -> Persistência -> Visualização
```

A arquitetura foi construída para demonstrar conceitos importantes de:

* sistemas distribuídos;
* Internet das Coisas (IoT);
* comunicação em rede;
* concorrência;
* interoperabilidade multi-linguagem;
* telemetria em tempo real.

Os sensores simulam dispositivos urbanos inteligentes, como:

* câmeras de trânsito;
* sensores ambientais;
* dispositivos de monitoramento;
* controladores urbanos.

Cada sensor opera de forma independente e envia continuamente informações para o Gateway.

O sistema também permite que o Gateway envie comandos de volta para os sensores, criando um modelo bidirecional de comunicação.

---

# 2. Principais Tecnologias Utilizadas

# 2.1 Python

Python foi utilizado como principal linguagem do Gateway Central e de parte dos sensores.

Principais aplicações:

* servidor assíncrono;
* processamento de telemetria;
* gerenciamento de dispositivos;
* controle distribuído;
* dashboard analítico;
* persistência de dados.

Bibliotecas utilizadas:

* asyncio;
* socket;
* sqlite3;
* aiosqlite;
* Streamlit;
* protobuf.

---

# 2.2 Linguagem C

O sensor em C foi desenvolvido com foco em:

* baixo overhead;
* eficiência computacional;
* proximidade com hardware;
* controle explícito de memória.

Principais características:

* sockets BSD;
* comunicação UDP;
* serialização binária;
* retry exponencial;
* jitter aleatório.

---

# 2.3 Java

O sensor Java foi utilizado para demonstrar:

* interoperabilidade;
* orientação a objetos;
* modularização;
* robustez estrutural.

Características principais:

* encapsulamento;
* gerenciamento automático de memória;
* abstração de sockets;
* integração com Protocol Buffers.

---

# 2.4 Lua

Lua foi utilizada como alternativa leve para scripting distribuído.

Características:

* baixo consumo de memória;
* inicialização rápida;
* simplicidade operacional;
* integração eficiente com sistemas IoT.

---

# 2.5 Protocol Buffers

Protocol Buffers foi utilizado como padrão de serialização binária.

Benefícios:

* interoperabilidade multi-linguagem;
* redução de payload;
* eficiência de transmissão;
* contratos estruturados de comunicação.

O protocolo permite que sensores escritos em diferentes linguagens compartilhem o mesmo formato de mensagens.

---

# 2.6 SQLite

SQLite foi utilizado como banco de dados principal do sistema.

Características implementadas:

* modo WAL;
* indexação;
* persistência local;
* consultas analíticas.

O banco armazena:

* dispositivos registrados;
* métricas de telemetria;
* séries temporais.

---

# 2.7 Streamlit

Streamlit foi utilizado para desenvolvimento do dashboard operacional.

Funcionalidades:

* visualização em tempo real;
* monitoramento de sensores;
* envio de comandos;
* análise estatística;
* interface web interativa.

---

# 2.8 Docker

Docker e Docker Compose foram utilizados para:

* conteinerização;
* isolamento de serviços;
* orquestração local;
* padronização de ambiente.

A infraestrutura conteinerizada facilita:

* execução distribuída;
* portabilidade;
* reprodutibilidade.

---

# 3. Como o Sistema Funciona na Prática

# 3.1 Arquitetura Geral

Toda a arquitetura foi organizada em torno de um modelo centralizado:

```text
Sensores -> Gateway -> Dashboard
```

O objetivo dessa arquitetura é simplificar a coordenação do sistema.

Ao invés dos sensores se comunicarem diretamente entre si, todos os dispositivos enviam informações para o Gateway.

Isso faz com que o Gateway se torne responsável por:

* receber telemetria;
* registrar dispositivos;
* armazenar métricas;
* distribuir comandos;
* controlar o estado geral da plataforma.

Essa abordagem reduz significativamente a complexidade da comunicação distribuída.

---

# 3.2 Comunicação entre os Componentes

O sistema utiliza dois protocolos diferentes de comunicação: UDP e TCP.

Cada um foi escolhido para resolver um problema específico.

## UDP — Telemetria

UDP é utilizado para envio contínuo de métricas e descoberta de dispositivos.

Os sensores enviam pacotes UDP constantemente para o Gateway contendo:

* leituras;
* estados;
* métricas;
* informações operacionais.

UDP foi escolhido porque possui:

* baixa latência;
* menor overhead;
* maior velocidade.

Como telemetria é enviada continuamente, perder alguns pacotes ocasionais não compromete o funcionamento geral.

Isso torna UDP ideal para monitoramento em tempo real.

---

## TCP — Controle

TCP é utilizado para operações críticas de controle.

Quando o dashboard ou o Gateway precisam alterar o estado de um sensor, a comunicação ocorre via TCP.

Isso garante:

* entrega confiável;
* integridade dos comandos;
* confirmação de transmissão.

Esse modelo híbrido permite que o sistema combine desempenho com confiabilidade.

---

## TCP

Utilizado para:

* comandos críticos;
* controle remoto;
* comunicação confiável.

Vantagens:

* garantia de entrega;
* integridade de stream;
* confiabilidade.

---

# 3.3 Descoberta Automática de Sensores

Quando um sensor é iniciado, ele automaticamente anuncia sua presença para o Gateway.

Esse processo ocorre via UDP.

O objetivo é permitir que novos dispositivos sejam adicionados dinamicamente sem necessidade de configuração manual.

Ao receber o anúncio, o Gateway registra:

* endereço IP;
* porta;
* tipo do dispositivo;
* status;
* capacidade de controle.

O sistema também utiliza multicast para permitir redescoberta de sensores.

Isso facilita:

* reinicializações;
* reconexões;
* atualização dinâmica da topologia.

---

# 3.4 Fluxo Completo da Telemetria

O fluxo principal do sistema funciona da seguinte forma:

```text
Sensor -> UDP -> Gateway -> Banco de Dados -> Dashboard
```

Primeiramente, o sensor gera uma métrica.

Essa métrica é serializada utilizando Protocol Buffers.

Em seguida:

1. o sensor envia os dados via UDP;
2. o Gateway recebe o datagrama;
3. o pacote é desserializado;
4. os dados são validados;
5. a métrica é armazenada no SQLite;
6. o dashboard consulta o banco;
7. as informações são exibidas em tempo real.

Todo esse processo ocorre continuamente e de forma concorrente.

---

# 3.5 Controle de Sequência e Integridade

Como UDP não garante entrega, o sistema implementa mecanismos próprios de validação.

Cada mensagem enviada pelos sensores possui:

* identificador;
* sequência;
* timestamp.

O Gateway monitora essas informações para detectar:

* perda de pacotes;
* duplicação;
* mensagens fora de ordem.

Isso aumenta a confiabilidade geral da plataforma.

---

# 3.6 Retry Exponencial e Resiliência

Os sensores possuem mecanismos automáticos de retry.

Quando ocorre uma falha de comunicação, o sensor tenta reenviar os dados utilizando backoff exponencial.

Na prática, isso significa que:

* a primeira tentativa ocorre rapidamente;
* as próximas tentativas aumentam gradualmente o tempo de espera.

Além disso, o sistema adiciona jitter aleatório.

O jitter evita que todos os sensores tentem retransmitir ao mesmo tempo.

Isso reduz:

* congestionamento;
* sobrecarga do Gateway;
* tempestades de retransmissão.

---

# 3.7 Processamento Concorrente

O Gateway foi desenvolvido utilizando asyncio.

Isso permite que múltiplos sensores enviem informações simultaneamente sem bloquear o sistema.

O Gateway consegue:

* receber múltiplos pacotes ao mesmo tempo;
* aceitar conexões TCP concorrentes;
* persistir dados em paralelo;
* responder comandos simultaneamente.

Esse modelo reduz:

* consumo excessivo de threads;
* overhead de troca de contexto;
* gargalos de I/O.

---

# 3.8 Persistência e Análise

O sistema armazena:

* dispositivos;
* métricas;
* séries temporais.

O dashboard executa:

* média;
* desvio padrão;
* filtragem temporal;
* monitoramento em tempo real.

---

# 4. Características Técnicas Relevantes

## Interoperabilidade Multi-Linguagem

O sistema suporta sensores desenvolvidos em:

* Python;
* C;
* Java;
* Lua.

Isso demonstra independência tecnológica e padronização protocolar.

---

## Modularização

Os componentes são desacoplados.

Cada sensor opera de forma independente.

---

## Escalabilidade Estrutural

Novos sensores podem ser adicionados sem necessidade de alteração significativa no Gateway.

---

## Conteinerização

Toda a infraestrutura pode ser executada via Docker Compose.

---

# 5. Limitações Atuais

O sistema ainda não implementa:

* autenticação;
* criptografia TLS;
* replicação;
* failover;
* persistência distribuída;
* balanceamento de carga.

O Gateway permanece como ponto único de falha.

---

# 6. Conclusão Executiva

O sistema desenvolvido apresenta uma arquitetura distribuída robusta voltada para monitoramento de ambientes Smart City.

A solução demonstra integração entre:

* sistemas distribuídos;
* redes;
* concorrência;
* telemetria;
* persistência;
* interoperabilidade multi-linguagem.

Os principais diferenciais técnicos incluem:

* comunicação híbrida UDP/TCP;
* serialização binária via Protocol Buffers;
* descoberta dinâmica de dispositivos;
* retry exponencial;
* processamento assíncrono;
* integração multi-linguagem;
* infraestrutura conteinerizada.