use axum::{
    Json, Router,
    extract::{
        Path, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
};
use portfolio_core::{AssetType, Holding, HoldingValuation, PriceTick, evaluate_holding};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, SqlitePool, sqlite::SqliteConnectOptions};
use std::{
    collections::HashMap,
    net::SocketAddr,
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{RwLock, broadcast};

#[derive(Clone)]
struct AppState {
    db: SqlitePool,
    prices: Arc<RwLock<HashMap<String, PriceTick>>>,
    price_tx: broadcast::Sender<Vec<PriceTick>>,
}

#[derive(FromRow)]
struct HoldingRow {
    id: i64,
    symbol: String,
    asset_type: String,
    quantity: String,
    avg_buy_price: String,
    currency: String,
}

#[derive(Deserialize)]
struct CreateHoldingRequest {
    symbol: String,
    asset_type: AssetType,
    quantity: Decimal,
    avg_buy_price: Decimal,
    currency: String,
}

#[derive(Serialize)]
struct HoldingResponse {
    id: i64,
    symbol: String,
    asset_type: AssetType,
    quantity: Decimal,
    avg_buy_price: Decimal,
    currency: String,
    current_price: Option<Decimal>,
    valuation: Option<HoldingValuation>,
}

#[derive(Serialize)]
struct PortfolioSummary {
    holdings: Vec<HoldingResponse>,
    total_market_value: Decimal,
    total_cost_basis: Decimal,
    total_unrealized_pnl: Decimal,
    update_interval_secs: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "sqlite:///app/storage/production.sqlite3?mode=rwc".into());

    let poll_interval_secs: u64 = std::env::var("PRICE_UPDATE_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(15);

    let opts = SqliteConnectOptions::from_str(&db_url)?
        .pragma("journal_mode", "WAL")
        .pragma("synchronous", "NORMAL")
        .pragma("mmap_size", "134217728")
        .create_if_missing(true);

    let db = SqlitePool::connect_with(opts).await?;
    init_db(&db).await?;

    let (price_tx, _) = broadcast::channel::<Vec<PriceTick>>(64);
    let state = AppState {
        db: db.clone(),
        prices: Arc::new(RwLock::new(HashMap::new())),
        price_tx,
    };

    tokio::spawn(run_price_poller(state.clone(), poll_interval_secs));

    let app = build_router(state, poll_interval_secs);

    let addr = SocketAddr::from((std::net::Ipv4Addr::UNSPECIFIED, 3000));
    println!("Listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    db.close().await;
    println!("SQLite pool closed cleanly.");
    Ok(())
}

async fn init_db(db: &SqlitePool) -> anyhow::Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS holdings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            asset_type TEXT NOT NULL,
            quantity TEXT NOT NULL,
            avg_buy_price TEXT NOT NULL,
            currency TEXT NOT NULL
        )",
    )
    .execute(db)
    .await?;

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    println!("Shutdown signal received...");
}

#[derive(Deserialize)]
struct TwelveDataQuote {
    price: Option<String>,
}

async fn run_price_poller(state: AppState, interval_secs: u64) {
    let api_key = match std::env::var("TWELVE_DATA_API_KEY") {
        Ok(key) if !key.trim().is_empty() => key,
        _ => {
            eprintln!("TWELVE_DATA_API_KEY not set; live price poller running in standby mode.");
            return;
        }
    };

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(8))
        .build()
        .expect("failed to build HTTP client");

    let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
    loop {
        ticker.tick().await;

        let rows: Vec<(String, String)> =
            sqlx::query_as("SELECT DISTINCT symbol, asset_type FROM holdings")
                .fetch_all(&state.db)
                .await
                .unwrap_or_default();

        if rows.is_empty() {
            continue;
        }

        let mut api_to_internal: HashMap<String, String> = HashMap::new();
        for (symbol, asset_type) in rows {
            let formatted = format_symbol_for_api(&symbol, &asset_type);
            api_to_internal.insert(formatted, symbol);
        }

        let joined_symbols = api_to_internal
            .keys()
            .cloned()
            .collect::<Vec<_>>()
            .join(",");

        let url = format!(
            "https://twelvedata.com{}&apikey={}",
            joined_symbols, api_key
        );

        let Ok(resp) = client.get(&url).send().await else {
            eprintln!("Failed to reach Twelve Data API");
            continue;
        };

        let Ok(json_val) = resp.json::<serde_json::Value>().await else {
            eprintln!("Failed to parse Twelve Data JSON response");
            continue;
        };

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let mut price_map = state.prices.write().await;
        let mut updated = false;

        if api_to_internal.len() == 1 {
            if let Some((_api_sym, internal_sym)) = api_to_internal.iter().next()
                && let Ok(single) = serde_json::from_value::<TwelveDataQuote>(json_val)
                && let Some(price_str) = single.price
                && let Ok(price) = Decimal::from_str(&price_str)
            {
                price_map.insert(
                    internal_sym.clone(),
                    PriceTick {
                        symbol: internal_sym.clone(),
                        price,
                        timestamp: now,
                    },
                );
                updated = true;
            }
        } else if let Some(obj) = json_val.as_object() {
            for (api_sym, val) in obj {
                if let Some(internal_sym) = api_to_internal.get(api_sym)
                    && let Ok(quote) = serde_json::from_value::<TwelveDataQuote>(val.clone())
                    && let Some(price_str) = quote.price
                    && let Ok(price) = Decimal::from_str(&price_str)
                {
                    price_map.insert(
                        internal_sym.clone(),
                        PriceTick {
                            symbol: internal_sym.clone(),
                            price,
                            timestamp: now,
                        },
                    );
                    updated = true;
                }
            }
        }

        if updated {
            let ticks: Vec<PriceTick> = price_map.values().cloned().collect();
            let _ = state.price_tx.send(ticks);
        }
    }
}

fn format_symbol_for_api(symbol: &str, asset_type: &str) -> String {
    if (asset_type == "Forex" || asset_type == "Crypto")
        && !symbol.contains('/')
        && symbol.len() == 6
    {
        format!("{}/{}", &symbol[..3], &symbol[3..])
    } else {
        symbol.to_string()
    }
}

async fn health() -> &'static str {
    "ok"
}

async fn ws_prices_handler(
    ws: WebSocketUpgrade,
    State((state, _)): State<(AppState, u64)>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_price_socket(socket, state))
}

async fn handle_price_socket(mut socket: WebSocket, state: AppState) {
    let initial_ticks: Vec<PriceTick> = state.prices.read().await.values().cloned().collect();
    if !initial_ticks.is_empty()
        && let Ok(payload) = serde_json::to_string(&initial_ticks)
        && socket.send(Message::Text(payload.into())).await.is_err()
    {
        return;
    }

    let mut rx = state.price_tx.subscribe();
    loop {
        tokio::select! {
            result = rx.recv() => {
                match result {
                    Ok(ticks) => {
                        if let Ok(payload) = serde_json::to_string(&ticks)
                            && socket.send(Message::Text(payload.into())).await.is_err()
                        {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                    _ => {}
                }
            }
        }
    }
}

async fn get_portfolio(
    State((state, interval_secs)): State<(AppState, u64)>,
) -> Json<PortfolioSummary> {
    let rows: Vec<HoldingRow> = sqlx::query_as(
        "SELECT id, symbol, asset_type, quantity, avg_buy_price, currency FROM holdings ORDER BY id DESC",
    )
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

    let prices = state.prices.read().await;
    let mut holdings = Vec::with_capacity(rows.len());
    let mut total_market_value = Decimal::ZERO;
    let mut total_cost_basis = Decimal::ZERO;

    for row in rows {
        let quantity = Decimal::from_str(&row.quantity).unwrap_or(Decimal::ZERO);
        let avg_buy_price = Decimal::from_str(&row.avg_buy_price).unwrap_or(Decimal::ZERO);
        let asset_type = match row.asset_type.as_str() {
            "Forex" => AssetType::Forex,
            "Crypto" => AssetType::Crypto,
            _ => AssetType::Stock,
        };

        let current_price = prices.get(&row.symbol).map(|t| t.price);
        let valuation = current_price.map(|price| {
            let core_holding = Holding {
                symbol: row.symbol.clone(),
                asset_type: asset_type.clone(),
                quantity,
                avg_buy_price,
                currency: row.currency.clone(),
            };
            evaluate_holding(&core_holding, price)
        });

        if let Some(ref val) = valuation {
            total_market_value += val.market_value;
            total_cost_basis += val.cost_basis;
        }

        holdings.push(HoldingResponse {
            id: row.id,
            symbol: row.symbol,
            asset_type,
            quantity,
            avg_buy_price,
            currency: row.currency,
            current_price,
            valuation,
        });
    }

    Json(PortfolioSummary {
        holdings,
        total_market_value,
        total_cost_basis,
        total_unrealized_pnl: total_market_value - total_cost_basis,
        update_interval_secs: interval_secs,
    })
}

async fn add_holding(
    State((state, _)): State<(AppState, u64)>,
    Json(input): Json<CreateHoldingRequest>,
) -> impl IntoResponse {
    let asset_type_str = match input.asset_type {
        AssetType::Stock => "Stock",
        AssetType::Forex => "Forex",
        AssetType::Crypto => "Crypto",
    };

    let symbol = input.symbol.trim().to_uppercase();

    match sqlx::query(
        "INSERT INTO holdings (symbol, asset_type, quantity, avg_buy_price, currency) VALUES (?, ?, ?, ?, ?)",
    )
    .bind(&symbol)
    .bind(asset_type_str)
    .bind(input.quantity.to_string())
    .bind(input.avg_buy_price.to_string())
    .bind(input.currency.trim().to_uppercase())
    .execute(&state.db)
    .await
    {
        Ok(_) => StatusCode::CREATED,
        Err(err) => {
            eprintln!("Failed to insert holding: {err}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

async fn delete_holding(
    State((state, _)): State<(AppState, u64)>,
    Path(id): Path<i64>,
) -> impl IntoResponse {
    match sqlx::query("DELETE FROM holdings WHERE id = ?")
        .bind(id)
        .execute(&state.db)
        .await
    {
        Ok(_) => StatusCode::NO_CONTENT,
        Err(err) => {
            eprintln!("Failed to delete holding {id}: {err}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

fn build_router(state: AppState, interval_secs: u64) -> Router {
    let portfolio_routes = Router::new()
        .route("/health", get(health))
        .route("/api/portfolio", get(get_portfolio))
        .route("/api/holdings", post(add_holding))
        .route("/api/holdings/{id}", delete(delete_holding))
        .route("/ws/prices", get(ws_prices_handler))
        .with_state((state, interval_secs));

    Router::new().nest("/portfolio", portfolio_routes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;

    #[tokio::test]
    async fn test_health_endpoint() {
        let db = SqlitePool::connect("sqlite::memory:").await.unwrap();
        init_db(&db).await.unwrap();
        let (price_tx, _) = broadcast::channel(16);
        let state = AppState {
            db,
            prices: Arc::new(RwLock::new(HashMap::new())),
            price_tx,
        };
        let app = build_router(state, 15);

        let res = app
            .oneshot(
                Request::builder()
                    .uri("/portfolio/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_add_and_fetch_portfolio() {
        let db = SqlitePool::connect("sqlite::memory:").await.unwrap();
        init_db(&db).await.unwrap();

        let mut initial_prices = HashMap::new();
        initial_prices.insert(
            "EURUSD".to_string(),
            PriceTick {
                symbol: "EURUSD".to_string(),
                price: Decimal::from_str("1.1000").unwrap(),
                timestamp: 1_700_000_000,
            },
        );

        let (price_tx, _) = broadcast::channel(16);
        let state = AppState {
            db,
            prices: Arc::new(RwLock::new(initial_prices)),
            price_tx,
        };
        let app = build_router(state, 15);

        let create_req = Request::builder()
            .method("POST")
            .uri("/portfolio/api/holdings")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"symbol":"EURUSD","asset_type":"Forex","quantity":"1000","avg_buy_price":"1.0800","currency":"USD"}"#,
            ))
            .unwrap();

        let res = app.clone().oneshot(create_req).await.unwrap();
        assert_eq!(res.status(), StatusCode::CREATED);

        let list_req = Request::builder()
            .uri("/portfolio/api/portfolio")
            .body(Body::empty())
            .unwrap();

        let res = app.oneshot(list_req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }
}
