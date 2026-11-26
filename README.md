# Rust Portfolio & Forex Tracker (`axum + sqlx + UniFFI`)

Real-time multi-currency portfolio and simulated order management microservice for **Stocks, Forex, Crypto & Indices**, backed by SQLite (WAL mode), built via GitHub Actions, and deployed to k3s via ArgoCD.

- **Web UI & API:** `/portfolio` (`GET/POST /portfolio/api/holdings`, `/portfolio/api/trades`, `/portfolio/ws/prices`)
- **Health Check:** `/portfolio/health`
- **Container Image:** `ghcr.io/ivaylob84/rust-portfolio-app:latest`
- **Backstage TechDocs:** See `catalog-info.yaml` and `docs/` (`Architecture`, `Domain Engine`, `API Reference`, `Runbook`).
