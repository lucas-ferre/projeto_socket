# Configuração do Ambiente de Desenvolvimento

Este documento descreve a matriz de dependências necessárias para o desenvolvimento, depuração local e suporte ao *Language Server Protocol* (LSP) nas IDEs para o ecossistema distribuído.

## 1. Orquestração e Contêineres 

O motor de virtualização é a única dependência estritamente necessária para rodar o projeto.
- **Docker Engine:** `>= 24.0.0`
- **Docker Compose (Plugin v2):** `>= 2.20.0`
- **Validação:** `docker compose version`

## 2. Matriz de Linguagens

### 2.1. Hub Central e Cliente Analítico (Python)
Utilizado nos módulos `gateway`, `client` e `sensor_camera`.
- **Interpretador:** Python `>= 3.11`
- **Bibliotecas:** As bibliotecas nativas da linguagem (como socket, struct, asyncio, uuid, sqlite3, threading e statistics) já vêm embutidas no Python 3.11 e não precisam ser declaradas.
- Core e Serialização (Usado no Gateway, Cliente e Sensor Python) | protobuf>=4.25.0
- Gateway Central (I/O assíncrono para SQLite) | aiosqlite>=0.19.0
- Cliente Analítico / Dashboard | streamlit>=1.30.0 | pandas>=2.1.0

### 2.2. Estação Meteorológica (C / POSIX)
Utilizado no módulo sensor_c. Requer ferramentas de compilação C nativas e a implementação C do Protobuf.
- **Toolchain:** build-essential, make
- **Bibliotecas:** libprotobuf-c-dev, protobuf-c-compiler

### 2.3. Semáforo Inteligente (Java)
Utilizado no módulo sensor_semaforo. Requer o Kit de Desenvolvimento Java.
- **Máquina Virtual:** JDK 17 ou JDK 21 (Eclipse Temurin / OpenJDK recomendado)
- **Dependência Manual (Protobuf Runtime):** protobuf-java-3.25.1.jar (Gerenciado automaticamente via Dockerfile no deploy).
- **Validação:** javac -version

### 2.4. Poste Inteligente (Lua)
Utilizado no módulo sensor_lua. Requer o runtime Lua e o gerenciador de pacotes LuaRocks.
- **Interpretador:** Lua 5.4 e liblua5.4-dev
- **Gerenciador de Pacotes:** LuaRocks
- **Dependências Nativas Lua:** luarocks --lua-version=5.4 install lua-protobuf | luarocks --lua-version=5.4 install luasocket

