# Guia de Execução Docker — Sistema Distribuído Smart City

# 1. Visão Geral

O sistema foi desenvolvido para ser executado utilizando Docker e Docker Compose.

A utilização de containers permite:

* isolamento dos serviços;
* facilidade de execução;
* padronização do ambiente;
* eliminação de dependências locais;
* inicialização simplificada.

Toda a infraestrutura do projeto é iniciada automaticamente através do Docker Compose.

Os containers incluem:

* Gateway Central;
* sensores distribuídos;
* dashboard Streamlit.

---

# 2. Pré-Requisitos

Antes de executar o sistema, é necessário possuir:

* Docker instalado;
* Docker Compose habilitado.

---

## 2.1 Verificar Instalação do Docker

Execute:

```bash
docker --version
```

Esse comando verifica se o Docker está instalado corretamente.

Exemplo de saída:

```bash
Docker version 27.x.x
```

---

## 2.2 Verificar Docker Compose

Execute:

```bash
docker compose version
```

Esse comando verifica se o Docker Compose está disponível.

---

# 3. Estrutura Geral do Ambiente

O sistema utiliza múltiplos containers interconectados.

Arquitetura:

```text
+----------------+
| Dashboard      |
| Streamlit      |
+--------+-------+
         |
         v
+----------------+
| Gateway        |
| Central        |
+--------+-------+
         |
 -----------------------------
 |            |             |
 v            v             v
Sensor C   Sensor Java   Sensor Lua
                 |
                 v
           Sensor Python
```

Todos os containers se comunicam através da rede interna criada automaticamente pelo Docker Compose.

---

# 4. Inicialização do Sistema

## 4.1 Acessar Diretório do Projeto

Primeiramente, abra o terminal na pasta raiz do projeto.

Exemplo:

```bash
cd smart-city-system
```

---

## 4.2 Construir os Containers

Execute:

```bash
docker-compose build 
```

Função do comando:

* cria as imagens Docker;
* instala dependências;
* executa os Dockerfiles;
* prepara o ambiente dos serviços.

Esse processo pode levar alguns minutos na primeira execução.

---

## 4.3 Iniciar Todos os Serviços

Execute:

```bash
docker-compose up
```

Função do comando:

* inicia todos os containers;
* cria a rede interna;
* executa sensores;
* inicia o Gateway;
* sobe o dashboard.

Durante a execução, os logs de todos os serviços serão exibidos no terminal.

---

## 4.4 Executar em Background

Para executar o sistema em segundo plano:

```bash
docker compose up -d
```

Função:

* executa os containers sem bloquear o terminal;
* mantém os serviços rodando em background.

---

# 5. Verificar Containers em Execução

Execute:

```bash
docker ps
```

Função:

* listar containers ativos;
* verificar status dos serviços;
* identificar portas abertas.

Exemplo esperado:

```text
gateway
sensor-c
sensor-java
sensor-lua
sensor-python
dashboard
```

---

# 6. Acessar o Dashboard

Após iniciar os containers, o dashboard pode ser acessado via navegador.

URL:

```text
http://localhost:8501
```

O dashboard permite:

* monitorar sensores;
* visualizar métricas;
* acompanhar telemetria;
* enviar comandos;
* analisar dados.

---

# 7. Fluxo de Inicialização Interno

Quando o sistema inicia:

1. o Docker cria a rede interna;
2. o Gateway Central é iniciado;
3. o banco SQLite é preparado;
4. os sensores iniciam;
5. os sensores anunciam presença via UDP;
6. o Gateway registra os dispositivos;
7. os sensores começam a enviar telemetria;
8. o dashboard consulta os dados.

Todo o processo ocorre automaticamente.

---

# 8. Monitoramento de Logs

## 8.1 Logs Gerais

Para visualizar logs de todos os serviços:

```bash
docker compose logs
```

---

## 8.2 Logs em Tempo Real

Para acompanhar logs continuamente:

```bash
docker compose logs -f
```

A opção:

```bash
-f
```

significa:

```text
follow
```

permitindo monitoramento em tempo real.

---

## 8.3 Logs de um Serviço Específico

Exemplo:

```bash
docker compose logs gateway
```

ou:

```bash
docker compose logs sensor-python
```

Função:

* visualizar erros;
* acompanhar telemetria;
* depurar execução.

---

# 9. Reinicialização dos Containers

Para reiniciar todos os serviços:

```bash
docker compose restart
```

Função:

* reinicializar containers sem reconstruir imagens.

---

# 10. Parar o Sistema

## 10.1 Parar Containers

Execute:

```bash
docker compose down
```

Função:

* parar todos os containers;
* remover a rede interna;
* encerrar os serviços.

---

## 10.2 Parar Sem Remover Containers

Execute:

```bash
docker compose stop
```

Função:

* interromper containers;
* preservar estado para reinicialização futura.

---

# 11. Reconstrução Completa

Caso alterações sejam realizadas no código:

```bash
docker compose down
```

Depois:

```bash
docker compose build --no-cache
```

Em seguida:

```bash
docker compose up
```

Função:

* reconstruir imagens do zero;
* evitar cache antigo;
* aplicar mudanças recentes.

---

# 12. Execução Individual de Serviços

Também é possível iniciar apenas um serviço específico.

Exemplo:

```bash
docker compose up gateway
```

ou:

```bash
docker compose up sensor-python
```

Isso é útil para:

* depuração;
* testes isolados;
* desenvolvimento incremental.

---

# 13. Rede Interna do Docker

O Docker Compose cria automaticamente uma bridge network.

Isso permite que os containers se comuniquem utilizando:

* nomes dos serviços;
* IPs internos;
* portas privadas.

Exemplo:

```text
gateway:5000
```

Os sensores utilizam essa rede para enviar telemetria ao Gateway.

---

# 14. Persistência de Dados

O banco SQLite permanece dentro do ambiente do container.

Dependendo da configuração do Docker Compose, volumes podem ser utilizados para:

* persistência;
* backup;
* retenção de métricas.

---

# 15. Comandos Mais Importantes

| Comando                   | Função                       |
| ------------------------- | ---------------------------- |
| docker compose build      | Construir imagens            |
| docker compose up         | Iniciar sistema              |
| docker compose up --build | Construir e inicia o sistema |
| docker compose up -d      | Executar em background       |
| docker compose logs       | Visualizar logs              |
| docker compose logs -f    | Monitorar logs em tempo real |
| docker compose restart    | Reiniciar serviços           |
| docker compose stop       | Parar containers             |
| docker compose down       | Encerrar ambiente            |
| docker ps                 | Ver containers ativos        |

---

# 16. Processo Completo de Execução

Fluxo recomendado:

## 1. Construir imagens

```bash
docker compose build
```

---

## 2. Iniciar containers

```bash
docker compose up -d
```

---

## 3. Verificar containers

```bash
docker ps
```

---

## 4. Acessar dashboard

```text
http://localhost:8501
```

---

## 5. Monitorar logs

```bash
docker compose logs -f
```

---

## 6. Encerrar sistema

```bash
docker compose down
```

---

# 17. Considerações Finais

A conteinerização permite:

* execução reproduzível;
* isolamento entre componentes;
* simplificação de configuração;
* facilidade de distribuição;
* inicialização automatizada do ambiente distribuído.

