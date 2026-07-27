FROM rust:1.92-bookworm AS builder

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY vendor ./vendor
COPY src ./src
RUN cargo build --release

FROM registry.digitalocean.com/globalcomix/mariadb:2025.07.10

COPY --from=builder /src/target/release/mariadb-mysql-cdc /usr/local/bin/mariadb-mysql-cdc

ENTRYPOINT ["mariadb-mysql-cdc"]
