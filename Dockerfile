# syntax=docker/dockerfile:1.7
FROM rust:1.92-bookworm AS builder

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY vendor ./vendor
COPY fixtures/ddl ./fixtures/ddl
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    cargo build --release && \
    cp target/release/mariadb-mysql-cdc /usr/local/bin/mariadb-mysql-cdc

FROM ubuntu:24.04@sha256:561618e2c15bf2397621dd04f96926663a3b5616c189cf7e38db7e82f5c538ea

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get upgrade -y \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates \
        libc6 \
        libgcc-s1 \
        libssl3t64 \
        zlib1g \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder --chown=65532:65532 --chmod=0755 \
    /usr/local/bin/mariadb-mysql-cdc \
    /usr/local/bin/mariadb-mysql-cdc

USER 65532:65532
ENTRYPOINT ["mariadb-mysql-cdc"]
