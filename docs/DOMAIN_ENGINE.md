# Domain Engine (`crates/portfolio-core/src/lib.rs`)

## Core Types

| Type | Values | Description |
|---|---|---|
| `AssetType` | `Stock`, `Forex`, `Crypto`, `Index` | Asset classification (`Forex` uses `100,000` units/lot contract multiplier). |
| `OrderSide` | `Long`, `Short` | Position direction (`Long` profits when `price > entry`; `Short` profits when `price < entry`). |
| `OrderStatus` | `Pending`, `Open` | `Pending` orders yield `$0.00` Unrealized P&L until live price crosses `entry_price`. |
| `TriggerDir` | `Above`, `Below` | Direction required for a `Pending` limit/stop order to fill into `Open`. |
| `CloseReason` | `Manual`, `StopLoss`, `TakeProfit` | Reason recorded when archiving an order to `closed_trades`. |

## Exported Functions
- `evaluate_position(holding, current_price, side, status) -> HoldingValuation`
  - Computes `effective_qty = qty * 100_000` for `Forex`, else `qty`.
  - Computes signed `unrealized_pnl` and `pnl_percent` (`0` while `Pending`).
- `evaluate_order_triggers(asset_type, side, status, trigger_dir, entry_price, stop_loss, take_profit, current_price) -> TriggerOutcome`
  - **Pending Activation**: Flips `Pending -> Open` when `Above` (`price >= entry`) or `Below` (`price <= entry`).
  - **Near-Trigger Alert**: Sets `near_sl` or `near_tp` when live price is within **10 pips** (`0.0010` or `0.10` for `JPY` pairs) on Forex, or **0.35%** on Stocks/Indices/Crypto.
  - **Auto-Close**: Returns `close_reason = Some(StopLoss | TakeProfit)` when price breaches `SL` or `TP`.
