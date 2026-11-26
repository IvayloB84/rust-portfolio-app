# Stage 1: Build the Rust binary
FROM rust:alpine3.24 AS builder
# hadolint ignore=DL3018
RUN apk add --no-cache musl-dev sqlite-dev pkgconfig
WORKDIR /build

# Cache dependencies first across workspace members
COPY Cargo.toml Cargo.lock* ./
COPY crates/portfolio-core/Cargo.toml crates/portfolio-core/Cargo.toml
COPY crates/server/Cargo.toml crates/server/Cargo.toml
RUN mkdir -p crates/portfolio-core/src crates/server/src && \
    echo "" > crates/portfolio-core/src/lib.rs && \
    echo "fn main() {}" > crates/server/src/main.rs && \
    cargo build --release -p rust-portfolio-server && \
    rm -rf crates/portfolio-core/src crates/server/src

# Build actual application
COPY crates ./crates
RUN touch crates/portfolio-core/src/lib.rs crates/server/src/main.rs && \
    cargo build --release -p rust-portfolio-server

# Stage 2: Minimal runtime image
FROM alpine:3.24
# hadolint ignore=DL3018
RUN apk add --no-cache ca-certificates sqlite-libs && \
    adduser -D -u 1000 appuser && \
    mkdir -p /app/storage && \
    chown -R 1000:1000 /app

WORKDIR /app
COPY --from=builder /build/target/release/rust-portfolio-server /app/server

USER 1000:1000
EXPOSE 3000
CMD ["/app/server"]
