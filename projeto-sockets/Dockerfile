ARG TARGET_SERVICE=gateway

FROM python:3.11-slim AS gateway
RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
RUN pip install --no-cache-dir protobuf aiosqlite
COPY ./gateway ./gateway
COPY ./common ./common
RUN protoc -I=./common --python_out=./gateway ./common/messages.proto
WORKDIR /app/gateway
RUN mkdir -p /app/gateway/db
CMD ["python", "main.py"]

FROM ubuntu:24.04 AS sensor_clima
RUN apt-get update && apt-get install -y \
    build-essential \
    libprotobuf-c-dev \
    protobuf-c-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY ./sensor_c .
COPY ./common/messages.proto .
RUN protoc-c --c_out=. messages.proto
RUN gcc -o sensor_c sensor.c messages.pb-c.c -lprotobuf-c
CMD ["./sensor_c"]

FROM ubuntu:24.04 AS sensor_posto
RUN apt-get update && apt-get install -y \
    lua5.4 \
    liblua5.4-dev \
    luarocks \
    build-essential \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
RUN luarocks --lua-version=5.4 install lua-protobuf && \
    luarocks --lua-version=5.4 install luasocket
WORKDIR /app
COPY ./sensor_lua/sensor.lua .
COPY ./common/messages.proto .
RUN protoc --descriptor_set_out=messages.pb messages.proto
CMD ["sh", "-c", "eval $(luarocks --lua-version=5.4 path) && lua5.4 sensor.lua"]

FROM eclipse-temurin:21-jdk AS sensor_java_builder
RUN apt-get update && apt-get install -y protobuf-compiler wget && rm -rf /var/lib/apt/lists/*
WORKDIR /build
RUN wget -q https://repo1.maven.org/maven2/com/google/protobuf/protobuf-java/3.25.1/protobuf-java-3.25.1.jar -O protobuf.jar
COPY ./common ./common
COPY ./sensor_java/sensor.java .
RUN protoc -I=./common --java_out=. ./common/messages.proto
RUN javac -cp protobuf.jar sensor.java smartcity/Messages.java

FROM eclipse-temurin:21-jre AS sensor_java
WORKDIR /app
COPY --from=sensor_java_builder /build/protobuf.jar .
COPY --from=sensor_java_builder /build/*.class .
COPY --from=sensor_java_builder /build/smartcity ./smartcity
CMD ["java", "-cp", ".:protobuf.jar", "sensor"]

FROM python:3.11-slim AS sensor_camera
RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
RUN pip install --no-cache-dir protobuf
COPY ./sensor_python ./sensor_python
COPY ./common ./common
RUN protoc -I=./common --python_out=./sensor_python ./common/messages.proto
WORKDIR /app/sensor_python
CMD ["python", "sensor.py"]

FROM python:3.11-slim AS dashboard
RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
RUN pip install --no-cache-dir streamlit protobuf
COPY ./client ./client
COPY ./common ./common
RUN protoc -I=./common --python_out=./client ./common/messages.proto
WORKDIR /app/client
EXPOSE 8501
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]

FROM ${TARGET_SERVICE} AS final
