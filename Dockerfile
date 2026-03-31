FROM rust:1.85-bookworm AS builder

WORKDIR /app

COPY Cargo.toml ./
COPY Cargo.lock ./
COPY src ./src

RUN cargo build --release --locked

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/codex-openai-proxy /usr/local/bin/codex-openai-proxy

EXPOSE 8080

CMD ["codex-openai-proxy", "--port", "8080"]
