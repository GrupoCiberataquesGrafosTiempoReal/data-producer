# Build
FROM rust:1.95-bullseye AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y cmake \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release

# Runtime
FROM debian:bullseye-slim

WORKDIR /opt

COPY --from=builder /app/target/release/data-producer /opt/data-producer

CMD ["./data-producer", "--no-interactive"]