# REST & WebSocket API Reference (`/portfolio`)

## Authentication

| Method | Path | Body / Params | Description |
|---|---|---|---|
| `POST` | `/portfolio/api/auth/register` | `{ email, password, name? }` | Create user & set HTTP-only sliding-TTL session cookie (`SESSION_IDLE_TTL_SECS`). |
| `POST` | `/portfolio/api/auth/login` | `{ email, password }` | Authenticate & set HTTP-only sliding-TTL session cookie (auto-renewed on `2xx` requests). |
| `POST` | `/portfolio/api/auth/logout` | — | Clear session cookie. |
| `GET` | `/portfolio/api/auth/me` | — | Return `{ id, email, name }` or `401`. |
| `GET` | `/portfolio/api/auth/google` | — | Start Google OAuth2 login flow. |

## Portfolio, Orders & History

| Method | Path | Body / Params | Description |
|---|---|---|---|
| `GET` | `/portfolio/api/portfolio` | `?base=NATIVE\|USD\|EUR` | Returns holdings, live valuations, `near_sl`/`near_tp` flags, `total_unrealized_pnl`, and `total_realized_pnl`. |
| `POST` | `/portfolio/api/holdings` | `{ symbol, asset_type, side?, quantity, avg_buy_price, stop_loss?, take_profit?, currency }` | Creates `OPEN` market order or auto-detects `PENDING` limit/stop order if entry differs from live price. |
| `PUT` | `/portfolio/api/holdings/{id}` | `{ quantity, avg_buy_price, stop_loss?, take_profit? }` | Updates SL/TP levels on `OPEN` positions (size & entry locked), or all fields on `PENDING` orders. |
| `POST` | `/portfolio/api/holdings/{id}/close` | `{ exit_price? }` | Closes `OPEN` position at live/exit price and archives to `closed_trades`, or deletes `PENDING` order cleanly without archiving. |
| `DELETE` | `/portfolio/api/holdings/{id}` | — | Deletes holding without archiving to history. |
| `GET` | `/portfolio/api/trades` | `?base=EUR\|USD` | Lists closed trades with `realized_pnl` (native) and `converted_pnl` (user display currency). |
| `GET` | `/portfolio/api/search` | `?q=AAPL` | Symbol autocomplete across Yahoo Finance & static index/FX catalog. |
| `GET` | `/portfolio/api/history` | `?symbol=...&asset_type=...&range=1d` | OHLC candles, 52-week range, and 14-day ATR (pips for Forex). |
| `WS` | `/portfolio/ws/prices` | Server pushes `Vec<PriceTick>` | Broadcasts live price ticks and triggers `process_symbol_triggers` on every update. |
