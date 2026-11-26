# Architecture Overview (`IvayloB84/rust-portfolio-app`)

## System Topology
- **`crates/portfolio-core`**: Pure Rust financial math & order trigger engine exported with `#[uniffi::export]` so identical rules run on the backend server and native iOS/Android clients.
- **`crates/server` (`rust-portfolio-app`)**: Axum 0.8 HTTP/WebSocket service backed by SQLite (WAL mode) and deployed via GitHub Actions + ArgoCD to k3s (`0.0.0.0:8083`, `Cache-Control: no-store` on HTML).
- **Session Security**: Sliding idle TTL (`SESSION_IDLE_TTL_SECS`) via `HttpOnly; SameSite=Lax` cookie (`portfolio_session`), auto-renewed on authenticated `2xx` responses.

## What Is Done
- **Auth & Sessions**: Argon2id Email/Password login & registration, Google OAuth2 login/account merge by email, sliding idle session renewal (`SESSION_IDLE_TTL_SECS`).
- **Live Market Feeds & Multi-Region Search**: Swissquote FX (3s), Binance Crypto WS, Finnhub US Stocks WS + Yahoo Finance live quote polling (`includePrePost=true`, accepting `REGULAR`/`LIVE`/`24/7`) for European/UK equities (`.DE`, `.F`, `.MI`, `.L`, `.PA`, `.AS`, `.SW`, `.MC`) and indices; automatic multi-region (`US`, `DE`, `GB`, `IT`, `FR`, `CH`) autocomplete search.
- **Order & Risk Engine**: `Long`/`Short`, `Pending`/`Open` (`$0.00` Market Value & Cost Basis while `Pending`), `SL`/`TP` auto-close, `10-pip` / `0.35%` near-trigger alerts; `OPEN` positions lock `quantity` & `avg_buy_price` (only `SL`/`TP` editable); cancelled `PENDING` orders delete cleanly without booking fake trades.
- **Mobile & Desktop SPA (`crates/server/src/index.html`)**: Grouped symbol rows (`×N`, weighted avg entry, `L / S` badge) with individual position selector & `Close All` / single-position sheet; sub-cent P&L precision; XTB-style slide-in chart (multi-day date labels, Candles/Line, 1:1 CTM crosshair); collapsible SL/TP & `+ New Position` drawer with `Long`/`Short` selector; uniform centered Account dropdown; full-screen Trade History window.

## What Is Not Done (Next Milestones)
1. **GitHub OAuth2**: Add `/portfolio/api/auth/github` and `/portfolio/api/auth/github/callback` with verified primary email lookup and account merge.
2. **Native Mobile Clients (UniFFI)**: Generate Kotlin (Android) and Swift (iOS) bindings from `crates/portfolio-core` and build native screens matching the mobile web UX.
3. **Permanent Named Tunnel & K3s ArgoCD Verification**: Cut over from ephemeral `trycloudflare.com` quick tunnels to the permanent Named Tunnel domain (`TUNNEL_TOKEN` in `.env`) and verify k3s/ArgoCD image sync.
4. **Broker & Tracker Portfolio Import (API Key + CSV)**: Add read-only API key connectors (Trading 212 `/api/v0/equity/portfolio`, Binance Spot, IBKR Flex) + CSV fallback with ticker normalization and idempotent sync.
