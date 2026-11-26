use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

uniffi::setup_scaffolding!();

uniffi::custom_type!(Decimal, String, {
    remote,
    try_lift: |val| Decimal::from_str(&val).map_err(uniffi::deps::anyhow::Error::msg),
    lower: |obj| obj.to_string(),
});

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, uniffi::Enum)]
pub enum AssetType {
    Stock,
    Forex,
    Crypto,
    Index,
}

#[derive(Debug, Clone, Serialize, Deserialize, uniffi::Record)]
pub struct Holding {
    pub symbol: String,
    pub asset_type: AssetType,
    pub quantity: Decimal,
    pub avg_buy_price: Decimal,
    pub currency: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, uniffi::Record)]
pub struct PriceTick {
    pub symbol: String,
    pub price: Decimal,
    pub timestamp: i64,
    pub volume: Option<u64>,
    pub market_state: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, uniffi::Record)]
pub struct HoldingValuation {
    pub symbol: String,
    pub market_value: Decimal,
    pub cost_basis: Decimal,
    pub unrealized_pnl: Decimal,
    pub pnl_percent: Decimal,
}

#[uniffi::export]
pub fn validate_holding_input(
    symbol: &str,
    asset_type: AssetType,
    quantity: Decimal,
    avg_buy_price: Decimal,
    currency: &str,
) -> Result<(), String> {
    let clean_sym = symbol.trim().to_uppercase();
    if clean_sym.is_empty() || clean_sym.len() > 20 {
        return Err("Symbol must be 1 to 20 characters.".to_string());
    }
    if asset_type == AssetType::Forex {
        let pair = clean_sym.replace(['/', '-'], "");
        if pair.len() != 6
            || !pair.chars().all(|c| c.is_ascii_alphabetic())
            || pair[..3] == pair[3..]
        {
            return Err(
                "Forex symbol must be a 6-letter pair of two distinct currencies (e.g., EURUSD)."
                    .to_string(),
            );
        }
    } else if !clean_sym
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '/' | '^' | '='))
    {
        return Err("Symbol contains invalid characters.".to_string());
    }
    if quantity <= Decimal::ZERO {
        return Err("Quantity must be greater than zero.".to_string());
    }
    if avg_buy_price <= Decimal::ZERO {
        return Err("Average buy price must be greater than zero.".to_string());
    }
    let clean_ccy = currency.trim();
    if clean_ccy.len() != 3 || !clean_ccy.chars().all(|c| c.is_ascii_alphabetic()) {
        return Err("Currency must be a 3-letter ISO code.".to_string());
    }
    Ok(())
}

#[uniffi::export]
pub fn is_valid_market_tick(
    price: Decimal,
    previous_price: Option<Decimal>,
    volume: Option<u64>,
    is_regular_session: bool,
) -> bool {
    if price <= Decimal::ZERO {
        return false;
    }
    if is_regular_session && matches!(volume, Some(0)) {
        return false;
    }
    if let Some(prev) = previous_price
        && prev > Decimal::ZERO
    {
        let diff = if price >= prev {
            price - prev
        } else {
            prev - price
        };
        if (diff / prev) * Decimal::from(100) > Decimal::from(20) {
            return false;
        }
    }
    true
}

#[uniffi::export]
pub fn evaluate_holding(holding: &Holding, current_price: Decimal) -> HoldingValuation {
    let multiplier = if holding.asset_type == AssetType::Forex {
        Decimal::from(100_000)
    } else {
        Decimal::ONE
    };
    let units = holding.quantity * multiplier;
    let cost_basis = units * holding.avg_buy_price;
    let market_value = units * current_price;
    let unrealized_pnl = market_value - cost_basis;
    let pnl_percent = if cost_basis.is_zero() {
        Decimal::ZERO
    } else {
        (unrealized_pnl / cost_basis) * Decimal::from(100)
    };
    HoldingValuation {
        symbol: holding.symbol.clone(),
        market_value,
        cost_basis,
        unrealized_pnl,
        pnl_percent,
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, uniffi::Enum)]
pub enum OrderSide {
    Long,
    Short,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, uniffi::Enum)]
pub enum OrderStatus {
    Pending,
    Open,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, uniffi::Enum)]
pub enum TriggerDir {
    Above,
    Below,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, uniffi::Enum)]
pub enum CloseReason {
    Manual,
    StopLoss,
    TakeProfit,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, uniffi::Record)]
pub struct OrderAlertState {
    pub triggered_open: bool,
    pub close_reason: Option<CloseReason>,
    pub near_sl: bool,
    pub near_tp: bool,
}

#[uniffi::export]
pub fn evaluate_position(
    holding: &Holding,
    current_price: Decimal,
    side: OrderSide,
    status: OrderStatus,
) -> HoldingValuation {
    let multiplier = if holding.asset_type == AssetType::Forex {
        Decimal::from(100_000)
    } else {
        Decimal::ONE
    };
    let units = holding.quantity * multiplier;
    let cost_basis = units * holding.avg_buy_price;
    if status == OrderStatus::Pending {
        return HoldingValuation {
            symbol: holding.symbol.clone(),
            market_value: cost_basis,
            cost_basis,
            unrealized_pnl: Decimal::ZERO,
            pnl_percent: Decimal::ZERO,
        };
    }
    let market_value = units * current_price;
    let unrealized_pnl = match side {
        OrderSide::Long => market_value - cost_basis,
        OrderSide::Short => cost_basis - market_value,
    };
    let pnl_percent = if cost_basis.is_zero() {
        Decimal::ZERO
    } else {
        (unrealized_pnl / cost_basis) * Decimal::from(100)
    };
    HoldingValuation {
        symbol: holding.symbol.clone(),
        market_value,
        cost_basis,
        unrealized_pnl,
        pnl_percent,
    }
}

#[uniffi::export]
#[allow(clippy::too_many_arguments)]
pub fn evaluate_order_triggers(
    symbol: &str,
    asset_type: AssetType,
    side: OrderSide,
    status: OrderStatus,
    trigger_dir: TriggerDir,
    entry_price: Decimal,
    current_price: Decimal,
    stop_loss: Option<Decimal>,
    take_profit: Option<Decimal>,
) -> OrderAlertState {
    if status == OrderStatus::Pending {
        let hit = match trigger_dir {
            TriggerDir::Above => current_price >= entry_price,
            TriggerDir::Below => current_price <= entry_price,
        };
        return OrderAlertState {
            triggered_open: hit,
            close_reason: None,
            near_sl: false,
            near_tp: false,
        };
    }
    let warn_buf = if asset_type == AssetType::Forex {
        if symbol.to_uppercase().ends_with("JPY") {
            Decimal::new(10, 2) // 10 pips = 0.10
        } else {
            Decimal::new(10, 4) // 10 pips = 0.0010
        }
    } else {
        current_price * Decimal::new(35, 4) // 0.35%
    };

    let mut close_reason = None;
    let mut near_sl = false;
    let mut near_tp = false;

    if let Some(sl) = stop_loss
        && sl > Decimal::ZERO
    {
        match side {
            OrderSide::Long => {
                if current_price <= sl {
                    close_reason = Some(CloseReason::StopLoss);
                } else if current_price - sl <= warn_buf {
                    near_sl = true;
                }
            }
            OrderSide::Short => {
                if current_price >= sl {
                    close_reason = Some(CloseReason::StopLoss);
                } else if sl - current_price <= warn_buf {
                    near_sl = true;
                }
            }
        }
    }
    if close_reason.is_none()
        && let Some(tp) = take_profit
        && tp > Decimal::ZERO
    {
        match side {
            OrderSide::Long => {
                if current_price >= tp {
                    close_reason = Some(CloseReason::TakeProfit);
                } else if tp - current_price <= warn_buf {
                    near_tp = true;
                }
            }
            OrderSide::Short => {
                if current_price <= tp {
                    close_reason = Some(CloseReason::TakeProfit);
                } else if current_price - tp <= warn_buf {
                    near_tp = true;
                }
            }
        }
    }
    OrderAlertState {
        triggered_open: false,
        close_reason,
        near_sl,
        near_tp,
    }
}
