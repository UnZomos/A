# Rust terbaru (Railway default-nya 1.84 terlalu tua untuk beberapa crate).
# Satu tahap supaya binary dan glibc selalu cocok.
FROM rust:1-slim

WORKDIR /app
COPY Cargo.toml ./
COPY Cargo.loc[k] ./
COPY src ./src

RUN cargo build --release

CMD ["/app/target/release/stream-keepalive"]
