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
pub fn evaluate_holding(holding: &Holding, current_price: Decimal) -> HoldingValuation {
    let cost_basis = holding.quantity * holding.avg_buy_price;
    let market_value = holding.quantity * current_price;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_evaluate_holding_profit() {
        let holding = Holding {
            symbol: "AAPL".to_string(),
            asset_type: AssetType::Stock,
            quantity: Decimal::from_str("10").unwrap(),
            avg_buy_price: Decimal::from_str("150.00").unwrap(),
            currency: "USD".to_string(),
        };

        let val = evaluate_holding(&holding, Decimal::from_str("180.00").unwrap());
        assert_eq!(val.market_value, Decimal::from_str("1800.00").unwrap());
        assert_eq!(val.unrealized_pnl, Decimal::from_str("300.00").unwrap());
        assert_eq!(val.pnl_percent, Decimal::from_str("20.00").unwrap());
    }
}
