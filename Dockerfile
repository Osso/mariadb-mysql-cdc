# syntax=docker/dockerfile:1.7
ARG BASE_IMAGE
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

FROM ${BASE_IMAGE}

COPY --from=builder /usr/local/bin/mariadb-mysql-cdc /usr/local/bin/mariadb-mysql-cdc

ENTRYPOINT ["mariadb-mysql-cdc"]
