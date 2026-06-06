# Guia de Execução Docker ou Podman — Smart City

## 1. Visão Geral

O sistema é executado por Docker Compose ou Podman Compose a partir do arquivo `docker-compose.yml`.

Serviços atuais:

* `gateway`;
* `sensor_clima`;
* `sensor_posto`;
* `sensor_java`;
* `sensor_camera`;
* `dashboard`.

## 2. Pré-Requisitos

Use uma das opções:

* Docker Engine com Docker Compose v2;
* Podman com `podman compose` ou `podman-compose`.

No Windows, execute os comandos a partir da pasta raiz do projeto, onde está o `docker-compose.yml`.

## 3. Portas

| Serviço | Porta | Protocolo | Uso |
|---|---:|---|---|
| gateway | 5000 | UDP | Telemetria `DataPayload` |
| gateway | 5001 | TCP | Dashboard/Gateway |
| gateway | 5002 | UDP | Descoberta/heartbeat `DiscoveryResponse` |
| sensor_posto | 5002 | TCP | Controle do sensor Lua |
| sensor_java | 5003 | TCP | Controle do sensor Java |
| sensor_camera | 5004 | TCP | Controle do sensor Python |
| multicast interno | 5005 | UDP | Recovery probes |
| dashboard | 8501 | TCP/HTTP | Interface web |

O sensor C não expõe porta TCP; ele opera como emissor UDP.

## 4. Build

Com Docker:

```bash
docker compose build
```

Com Podman:

```bash
podman compose build
```

## 5. Inicialização

Com Docker:

```bash
docker compose up --build
```

Em background:

```bash
docker compose up --build -d
```

Com Podman:

```bash
podman compose up --build
podman compose up --build -d
```

Se o ambiente possuir apenas `podman-compose`, use:

```bash
podman-compose up --build
```

No PowerShell, também há um helper:

```powershell
.\scripts\podman-compose.ps1 up --build
```

## 6. Dashboard

Após os healthchecks ficarem saudáveis, acesse:

```text
http://localhost:8501
```

## 7. Healthchecks

Todos os serviços possuem healthcheck:

| Serviço | Verificação |
|---|---|
| gateway | TCP `127.0.0.1:5001` |
| sensor_clima | processo `sensor_c` vivo |
| sensor_posto | TCP `127.0.0.1:5002` |
| sensor_java | TCP `127.0.0.1:5003` |
| sensor_camera | TCP `127.0.0.1:5004` |
| dashboard | TCP `127.0.0.1:8501` |

Para ver o estado:

```bash
docker compose ps
```

ou:

```bash
podman compose ps
```

Os sensores renovam presença automaticamente com `DiscoveryResponse` a cada 10 segundos mais jitter de até 2 segundos. Para ajustar essa cadência, defina `SENSOR_HEARTBEAT_INTERVAL_SECS` e `SENSOR_HEARTBEAT_JITTER_SECS` no serviço desejado.

O gateway também aceita variáveis para gestão de dados, como `TELEMETRY_BATCH_MAX_ROWS`, `METRICS_RAW_RETENTION_SECS`, `ROLLUP_1M_RETENTION_SECS`, `ROLLUP_5M_RETENTION_SECS`, `ROLLUP_1H_RETENTION_SECS` e os limites `OLAP_RAW_MAX_WINDOW_SECS`, `OLAP_1M_MAX_WINDOW_SECS`, `OLAP_5M_MAX_WINDOW_SECS`.

## 8. Logs

Todos os serviços:

```bash
docker compose logs -f
```

Serviços específicos:

```bash
docker compose logs -f gateway sensor_clima sensor_posto sensor_java sensor_camera dashboard
```

Com Podman:

```bash
podman compose logs -f gateway sensor_clima sensor_posto sensor_java sensor_camera dashboard
```

## 9. Inspeção do Banco

Com Docker:

```bash
docker exec gateway sqlite3 db/smartcity_gateway.db \
  "SELECT device_id, type, status, last_seen FROM devices;"
```

```bash
docker exec gateway sqlite3 db/smartcity_gateway.db \
  "SELECT device_id, metric_name, value, unit FROM metrics ORDER BY id DESC LIMIT 20;"
```

```bash
docker exec gateway sqlite3 db/smartcity_gateway.db \
  "SELECT metric_name, COUNT(*) FROM metrics_rollup_1m GROUP BY metric_name;"
```

Com Podman:

```bash
podman exec gateway sqlite3 db/smartcity_gateway.db \
  "SELECT device_id, type, status, last_seen FROM devices;"
```

```bash
podman exec gateway sqlite3 db/smartcity_gateway.db \
  "SELECT device_id, metric_name, value, unit FROM metrics ORDER BY id DESC LIMIT 20;"
```

```bash
podman exec gateway sqlite3 db/smartcity_gateway.db \
  "SELECT metric_name, COUNT(*) FROM metrics_rollup_1m GROUP BY metric_name;"
```

## 10. Execução Parcial

Subir apenas o gateway:

```bash
docker compose up gateway
```

Subir um sensor:

```bash
docker compose up sensor_camera
```

Com Podman, substitua `docker compose` por `podman compose` ou `podman-compose`.

## 11. Reinicialização

```bash
docker compose restart
```

ou:

```bash
podman compose restart
```

## 12. Encerramento

```bash
docker compose down
```

ou:

```bash
podman compose down
```

## 13. Reconstrução Limpa

```bash
docker compose down
docker compose build --no-cache
docker compose up
```

Com Podman:

```bash
podman compose down
podman compose build --no-cache
podman compose up
```

## 14. Observações de Compatibilidade

O arquivo `Dockerfile` da raiz existe como fallback multi-stage para ambientes Podman Compose no Windows que ignoram `build.dockerfile`. O `docker-compose.yml` permanece como fonte principal de orquestração.

O multicast de recovery usa `239.0.0.1:5005` internamente na rede Compose e não precisa ser publicado no host.
