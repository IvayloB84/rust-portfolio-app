# Architecture Overview (`IvayloB84/rust-portfolio-app`)

## System Topology
- **`crates/portfolio-core`**: Pure Rust financial math & order trigger engine exported with `#[uniffi::export]` so identical rules run on the backend server and native mobile clients.
- **`crates/server` (`rust-portfolio-app`)**: Axum HTTP/WebSocket service backed by SQLite (WAL mode) and deployed via GitHub Actions + ArgoCD to k3s, serving:
  - Embedded mobile-first & desktop single-page UI (`crates/server/src/index.html`)
  - Authenticated REST API (`/portfolio/api/*`)
  - Live broadcast WebSocket (`/portfolio/ws/prices`)
  - Background market pollers (Yahoo Finance, TwelveData, Finnhub, Frankfurter FX)
