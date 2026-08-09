ARG BASE_IMAGE
FROM rust:1.92-bookworm AS builder

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY vendor ./vendor
COPY fixtures/ddl ./fixtures/ddl
COPY src ./src
RUN cargo build --release

FROM ${BASE_IMAGE}

COPY --from=builder /src/target/release/mariadb-mysql-cdc /usr/local/bin/mariadb-mysql-cdc

ENTRYPOINT ["mariadb-mysql-cdc"]
