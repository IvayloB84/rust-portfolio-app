# AI Engineering Handoff — `rust-portfolio-app`

## 1. Repository & Stack
- **Repo:** `https://github.com/IvayloB84/rust-portfolio-app` (branch: `main`)
- **Workspace Crates:**
  - `crates/portfolio-core`: Pure Rust domain valuation, P&L, Forex pip/notional math, order triggers (`PENDING` -> `OPEN`, `SL`/`TP` execution), and `uniffi` exports.
  - `crates/server`: Single-binary `axum` + `sqlx` (SQLite WAL mode) web server (`crates/server/src/main.rs`) serving an embedded single-page frontend (`crates/server/src/index.html`).
- **Deployment & Backups:**
  - Local dev server: `./start.sh` (runs on `http://localhost:8083/portfolio`, logs to `server.log`).
  - Backups: `/home/theone/backups/backup-r2.sh` (backs up SQLite DB + source tarball locally and to Cloudflare R2).
  - Production CI/CD: GitHub Actions builds container image `ghcr.io/ivaylob84/rust-portfolio-app:latest`, deployed to `k3s` via ArgoCD.

## 2. Coding & Workflow Rules (Must Follow)
- **Step-by-step incremental changes:** Build, compile (`cargo fmt && cargo clippy --workspace -- -D warnings && cargo build -p rust-portfolio-server`), restart (`./start.sh`), and verify one feature at a time.
- **Compact code blocks:** Keep terminal snippets short and self-contained (split larger edits into 2–3 small boxes).
- **Do NOT push to GitHub on every small change:** Wait until several updates are tested and the user explicitly asks to push (`git push origin HEAD && git push origin HEAD:main`).
- **Standardized UI Typography & Responsive Layout:**
  - Use CSS variables `--font-sans`, `--fs-xs` (`0.72rem`), `--fs-sm` (`0.8rem`), `--fs-base` (`0.9rem`), `--fs-lg` (`1.15rem`), `--fs-xl` (`1.45rem`) and `font-variant-numeric: tabular-nums`.
  - Always verify both desktop and mobile (`<= 680px`, iPhone 15) viewports, including responsive SVG `viewBox` sizing.

## 3. Recently Completed Features
- **Live Market Data & Order Execution:** Real-time WebSockets (`/portfolio/ws/prices`), Stocks, Forex (`0.01` lot micro-lots, `100,000` units/lot), Crypto, and Caret Indices (`^VIX`, `^GSPC`).
- **Directional SL/TP Validation:** Enforces `SL < Entry < TP` for `LONG` and `TP < Entry < SL` for `SHORT` across Add, Modify, `+ New Position`, and Bulk CSV Import (`Trading 212` / `IBKR` / generic).
- **Interactive SVG Charts:** Candlesticks/Line/Area, XTB-style inline `BUY`/`SELL` & `SL`/`TP` cash flags, translucent right-axis price tags, out-of-range auto-stretch, and trailing 12M Dividend Yield (`Div: X.XX% ($Y.YY/yr)`) on dividend stocks.
- **Portfolio Analytics & CSV Export:** Interactive SVG Allocation Donut (`Asset Class`, `Symbol`, `Currency`), Risk & Attribution KPIs (`Top Gainer`, `Top Drawdown`, `Max Concentration %`, `Capital at Risk to SL`), Equity Curve vs. Benchmark (`^GSPC` / `BTC-USD`), and 2-decimal CSV exports for open positions and closed trades.

## 4. Current To-Do Roadmap (`project_state.csv`)
1. **Full User Account & Settings Page (Next Priority):** Dedicated slide-over/page replacing the simple account dropdown with user profile info, password change, default display currency & benchmark preferences, active session/OAuth management, and CSV import/export hub.
2. **GitHub OAuth2 Login:** Add `/portfolio/api/auth/github` alongside Google OAuth2 and email/password auth.
3. **Cash Balance & Deposit / Withdrawal Ledger (TWR):** Track uninvested cash (`EUR`/`USD`), deposits/withdrawals, and Time-Weighted Return (`TWR %`).
4. **Native Mobile UniFFI Bindings (Swift / Kotlin):** Generate Swift and Kotlin bindings from `portfolio-core`.
