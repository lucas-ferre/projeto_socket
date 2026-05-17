FROM ubuntu:24.04

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