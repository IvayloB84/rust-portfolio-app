# Rust Articles App (`axum` + `sqlx`)

Lightweight Rust web service backed by SQLite (WAL mode) on a `2Gi` Kubernetes PersistentVolumeClaim, built via GitHub Actions and deployed to k3s through ArgoCD.

- **Web UI & API:** `/rust/articles` (`GET` & `POST`)
- **Health Check:** `/rust/health`
- **Container Image:** `ghcr.io/ivaylob84/rust-portfolio-app:latest` (~8 MB Alpine runtime)
