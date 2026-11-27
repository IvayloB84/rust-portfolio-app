use axum::http::{
    HeaderMap,
    header::{COOKIE, SET_COOKIE},
};
use axum::{
    Json, Router,
    extract::{
        Path, Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::StatusCode,
    response::{Html, IntoResponse, Redirect},
    routing::{delete, get, post},
};
use portfolio_core::{
    AssetType, CloseReason, Holding, HoldingValuation, OrderSide, OrderStatus, PriceTick,
    TriggerDir, evaluate_order_triggers, evaluate_position, is_valid_market_tick,
    validate_holding_input,
};
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
    user_id: i64,
    symbol: String,
    asset_type: String,
    quantity: String,
    avg_buy_price: String,
    currency: String,
    side: String,
    status: String,
    trigger_dir: String,
    stop_loss: Option<String>,
    take_profit: Option<String>,
}

#[derive(Deserialize)]
struct CreateHoldingRequest {
    symbol: String,
    asset_type: AssetType,
    quantity: Decimal,
    avg_buy_price: Decimal,
    currency: String,
    #[serde(default)]
    side: Option<String>,
    #[serde(default)]
    stop_loss: Option<Decimal>,
    #[serde(default)]
    take_profit: Option<Decimal>,
}

#[derive(Serialize)]
struct HoldingResponse {
    id: i64,
    symbol: String,
    asset_type: AssetType,
    quantity: Decimal,
    avg_buy_price: Decimal,
    currency: String,
    side: String,
    status: String,
    stop_loss: Option<Decimal>,
    take_profit: Option<Decimal>,
    near_sl: bool,
    near_tp: bool,
    current_price: Option<Decimal>,
    volume: Option<u64>,
    market_state: Option<String>,
    valuation: Option<HoldingValuation>,
}
#[derive(FromRow, Serialize)]
struct ClosedTradeRow {
    id: i64,
    symbol: String,
    asset_type: String,
    side: String,
    currency: String,
    quantity: String,
    entry_price: String,
    exit_price: String,
    realized_pnl: String,
    close_reason: String,
    closed_at: i64,
}

#[derive(Serialize)]
struct PortfolioSummary {
    base_currency: String,
    eur_usd_rate: Decimal,
    holdings: Vec<HoldingResponse>,
    total_market_value: Decimal,
    total_cost_basis: Decimal,
    total_unrealized_pnl: Decimal,
    total_realized_pnl: Decimal,
    update_interval_secs: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
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
    sqlx::query("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT NOT NULL UNIQUE, name TEXT NOT NULL DEFAULT '', provider TEXT NOT NULL DEFAULT 'email', provider_sub TEXT, password_hash TEXT, created_at TEXT NOT NULL DEFAULT (datetime('now')))").execute(&db).await.expect("users table");
    sqlx::query("CREATE TABLE IF NOT EXISTS sessions (token TEXT PRIMARY KEY, user_id INTEGER NOT NULL, expires_at INTEGER NOT NULL)").execute(&db).await.expect("sessions table");
    sqlx::query("ALTER TABLE holdings ADD COLUMN user_id INTEGER")
        .execute(&db)
        .await
        .ok();
    let state = AppState {
        db: db.clone(),
        prices: Arc::new(RwLock::new(HashMap::new())),
        price_tx,
    };

    tokio::spawn(run_price_poller(state.clone(), poll_interval_secs));

    let app = build_router(state, poll_interval_secs);

    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3000);
    let addr = SocketAddr::from((std::net::Ipv4Addr::UNSPECIFIED, port));
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
    for a in [
        "ALTER TABLE holdings ADD COLUMN side TEXT NOT NULL DEFAULT 'LONG'",
        "ALTER TABLE holdings ADD COLUMN status TEXT NOT NULL DEFAULT 'OPEN'",
        "ALTER TABLE holdings ADD COLUMN trigger_dir TEXT NOT NULL DEFAULT 'BELOW'",
        "ALTER TABLE holdings ADD COLUMN stop_loss TEXT",
        "ALTER TABLE holdings ADD COLUMN take_profit TEXT",
    ] {
        let _ = sqlx::query(a).execute(db).await;
    }
    sqlx::query("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT NOT NULL UNIQUE, name TEXT NOT NULL DEFAULT '', provider TEXT NOT NULL DEFAULT 'email', provider_sub TEXT, password_hash TEXT, created_at TEXT NOT NULL DEFAULT (datetime('now')))").execute(db).await?;
    sqlx::query("CREATE TABLE IF NOT EXISTS sessions (token TEXT PRIMARY KEY, user_id INTEGER NOT NULL, expires_at INTEGER NOT NULL)").execute(db).await?;
    let _ = sqlx::query("ALTER TABLE holdings ADD COLUMN user_id INTEGER")
        .execute(db)
        .await;
    sqlx::query("CREATE TABLE IF NOT EXISTS closed_trades (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, symbol TEXT NOT NULL, asset_type TEXT NOT NULL, side TEXT NOT NULL, currency TEXT NOT NULL, quantity TEXT NOT NULL, entry_price TEXT NOT NULL, exit_price TEXT NOT NULL, realized_pnl TEXT NOT NULL, close_reason TEXT NOT NULL, closed_at INTEGER NOT NULL)").execute(db).await?;
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

use futures_util::{SinkExt, StreamExt, future::join_all};
use tokio_tungstenite::{connect_async, tungstenite::Message as WsClientMessage};

#[derive(Deserialize)]
struct CoinbaseTicker {
    #[serde(rename = "type")]
    msg_type: String,
    product_id: Option<String>,
    price: Option<String>,
}

#[derive(Deserialize)]
struct CoinbaseSpotResp {
    data: Option<CoinbaseSpotData>,
}

#[derive(Deserialize)]
struct CoinbaseSpotData {
    amount: String,
}

#[derive(Deserialize)]
struct FinnhubMsg {
    #[serde(rename = "type")]
    msg_type: String,
    data: Option<Vec<FinnhubTrade>>,
}

#[derive(Deserialize)]
struct FinnhubTrade {
    s: String,
    p: f64,
}

#[derive(Deserialize)]
struct YahooSearchResp {
    quotes: Option<Vec<YahooSearchQuote>>,
}

#[derive(Deserialize)]
struct YahooSearchQuote {
    symbol: Option<String>,
    shortname: Option<String>,
    longname: Option<String>,
    #[serde(rename = "quoteType")]
    quote_type: Option<String>,
}

#[derive(Deserialize)]
struct YahooChartResp {
    chart: Option<YahooChartBody>,
}

#[derive(Deserialize)]
struct YahooChartBody {
    result: Option<Vec<YahooChartResult>>,
}

#[derive(Deserialize)]
struct YahooChartResult {
    meta: Option<YahooChartMeta>,
}

#[derive(Clone, Deserialize)]
struct YahooChartMeta {
    #[serde(rename = "regularMarketPrice")]
    regular_market_price: Option<f64>,
    #[serde(rename = "regularMarketVolume")]
    regular_market_volume: Option<u64>,
    #[serde(rename = "regularMarketTime")]
    regular_market_time: Option<i64>,
    #[serde(rename = "marketState")]
    market_state: Option<String>,
    #[serde(rename = "instrumentType")]
    instrument_type: Option<String>,
    currency: Option<String>,
}

#[derive(Deserialize)]
struct SwissquoteEntry {
    #[serde(rename = "spreadProfilePrices")]
    spread_profile_prices: Vec<SwissquotePrice>,
}

#[derive(Deserialize)]
struct SwissquotePrice {
    bid: f64,
    ask: f64,
}

async fn run_price_poller(state: AppState, _interval_secs: u64) {
    let binance_state = state.clone();
    tokio::spawn(async move {
        run_binance_ws(binance_state).await;
    });

    let fx_state = state.clone();
    tokio::spawn(async move {
        run_swissquote_fx(fx_state).await;
    });

    let finnhub_state = state.clone();
    tokio::spawn(async move {
        run_finnhub_ws(finnhub_state).await;
    });

    let stock_fallback = state.clone();
    tokio::spawn(async move {
        run_stock_quote_fallback(stock_fallback).await;
    });
}

async fn run_swissquote_fx(state: AppState) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("failed to build HTTP client");

    let host = ["forex-data-feed", "swissquote", "com"].join(".");
    let mut ticker = tokio::time::interval(Duration::from_secs(3));

    loop {
        ticker.tick().await;

        let mut rows: Vec<(String,)> =
            sqlx::query_as("SELECT DISTINCT symbol FROM holdings WHERE asset_type = 'Forex'")
                .fetch_all(&state.db)
                .await
                .unwrap_or_default();
        for base_fx in ["EURUSD", "GBPUSD", "USDCHF", "USDJPY"] {
            if !rows.iter().any(|(r,)| r.eq_ignore_ascii_case(base_fx)) {
                rows.push((base_fx.to_string(),));
            }
        }

        for (sym,) in rows {
            let clean = sym.to_uppercase().replace('/', "");
            if clean.len() == 6 {
                let url = format!(
                    "https://{}/public-quotes/bboquotes/instrument/{}/{}",
                    host,
                    &clean[..3],
                    &clean[3..]
                );
                if let Ok(resp) = client.get(&url).send().await
                    && let Ok(entries) = resp.json::<Vec<SwissquoteEntry>>().await
                    && let Some(first) = entries.first()
                    && let Some(quote) = first.spread_profile_prices.first()
                {
                    let mid = (quote.bid + quote.ask) / 2.0;
                    if let Ok(dec) = Decimal::from_str(&format!("{:.5}", mid)) {
                        update_price_meta(&state, &sym, dec, None, Some("LIVE".to_string())).await;
                    }
                }
            }
        }
    }
}

async fn run_stock_quote_fallback(state: AppState) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(8))
        .build()
        .expect("failed to build HTTP client");

    let mut ticker = tokio::time::interval(Duration::from_secs(20));
    loop {
        ticker.tick().await;

        let yf_host = ["query1", "finance", "yahoo", "com"].join(".");
        let stock_rows: Vec<(String,)> =
            sqlx::query_as("SELECT DISTINCT symbol FROM holdings WHERE asset_type = 'Stock'")
                .fetch_all(&state.db)
                .await
                .unwrap_or_default();
        for (sym,) in stock_rows {
            let url = format!(
                "https://{}/v8/finance/chart/{}?interval=1m&range=1d&includePrePost=true",
                yf_host,
                sym.trim().to_uppercase()
            );
            if let Ok(resp) = client
                .get(&url)
                .header("User-Agent", "Mozilla/5.0")
                .send()
                .await
                && let Ok(parsed) = resp.json::<YahooChartResp>().await
                && let Some(meta) = parsed
                    .chart
                    .and_then(|c| c.result)
                    .and_then(|r| r.into_iter().next())
                    .and_then(|f| f.meta)
                && let Some(price) = meta.regular_market_price.filter(|p| *p > 0.0)
                && let Ok(dec) = Decimal::from_str(&format!("{:.4}", price))
            {
                let now_s = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64;
                let badge = meta
                    .regular_market_time
                    .map(|mt| {
                        if now_s - mt < 2400 || meta.market_state.as_deref() == Some("REGULAR") {
                            "LIVE".to_string()
                        } else {
                            meta.market_state
                                .clone()
                                .unwrap_or_else(|| "CLOSED".to_string())
                        }
                    })
                    .or_else(|| meta.market_state.clone());
                update_price_meta(&state, &sym, dec, meta.regular_market_volume, badge).await;
            }
        }

        let crypto_rows: Vec<(String,)> =
            sqlx::query_as("SELECT DISTINCT symbol FROM holdings WHERE asset_type = 'Crypto'")
                .fetch_all(&state.db)
                .await
                .unwrap_or_default();

        let cb_host = ["api", "coinbase", "com"].join(".");
        for (sym,) in crypto_rows {
            let clean = sym.to_uppercase().replace(['/', '-'], "");
            let base = if clean.ends_with("USDT") && clean.len() > 4 {
                &clean[..clean.len() - 4]
            } else if clean.ends_with("USD") && clean.len() > 3 {
                &clean[..clean.len() - 3]
            } else {
                clean.as_str()
            };
            let url = format!("https://{}/v2/prices/{}-USD/spot", cb_host, base);
            if let Ok(resp) = client.get(&url).send().await
                && let Ok(parsed) = resp.json::<CoinbaseSpotResp>().await
                && let Some(d) = parsed.data
                && let Ok(dec) = Decimal::from_str(&d.amount)
            {
                update_price_meta(&state, &sym, dec, None, Some("LIVE".to_string())).await;
            }
        }

        let index_rows: Vec<(String,)> =
            sqlx::query_as("SELECT DISTINCT symbol FROM holdings WHERE asset_type = 'Index'")
                .fetch_all(&state.db)
                .await
                .unwrap_or_default();

        for (sym,) in index_rows {
            let up = sym.trim().to_uppercase();
            let mapped = match up.as_str() {
                "JP225" | "JPN225" | "NIKKEI225" => Some("NKD=F"),
                "US500" | "SPX500" | "SP500" | "SPX" => Some("ES=F"),
                "VIX" => Some("^VIX"),
                "NAS100" | "US100" | "USTEC" | "NDX" => Some("NQ=F"),
                "US30" | "DJ30" | "WS30" | "DJI" => Some("YM=F"),
                "DE40" | "GER40" | "DE30" | "DAX40" | "DAX" => Some("^GDAXI"),
                "UK100" | "FTSE100" | "FTSE" => Some("^FTSE"),
                "FRA40" | "CAC40" => Some("^FCHI"),
                "ESP35" | "IBEX35" => Some("^IBEX"),
                "EU50" | "EUSTX50" | "STOXX50" => Some("^STOXX50E"),
                "AUS200" | "AU200" => Some("^AXJO"),
                "HK50" | "HSI" => Some("^HSI"),
                "CH20" | "SMI" => Some("^SSMI"),
                _ => None,
            };
            let mut candidates = Vec::new();
            if let Some(m) = mapped {
                candidates.push(m.to_string());
            }
            candidates.push(up.clone());
            if !up.starts_with('^') {
                candidates.push(format!("^{}", up));
            }
            let search_url = format!(
                "https://{}/v1/finance/search?q={}&quotesCount=6&newsCount=0",
                yf_host, up
            );
            if let Ok(sresp) = client
                .get(&search_url)
                .header("User-Agent", "Mozilla/5.0")
                .send()
                .await
                && let Ok(sp) = sresp.json::<YahooSearchResp>().await
                && let Some(quotes) = sp.quotes
            {
                for q in &quotes {
                    if q.quote_type.as_deref() == Some("INDEX")
                        && let Some(ref s_sym) = q.symbol
                    {
                        candidates.push(s_sym.clone());
                    }
                }
                for q in &quotes {
                    if let Some(ref s_sym) = q.symbol {
                        candidates.push(s_sym.clone());
                    }
                }
            }

            for ticker in candidates {
                let url = format!(
                    "https://{}/v8/finance/chart/{}?interval=1m&range=1d",
                    yf_host, ticker
                );
                if let Ok(resp) = client
                    .get(&url)
                    .header("User-Agent", "Mozilla/5.0")
                    .send()
                    .await
                    && let Ok(parsed) = resp.json::<YahooChartResp>().await
                    && let Some(chart) = parsed.chart
                    && let Some(results) = chart.result
                    && let Some(first) = results.first()
                    && let Some(meta) = &first.meta
                    && let Some(price) = meta.regular_market_price
                    && price > 0.0
                    && let Ok(dec) = Decimal::from_str(&format!("{:.2}", price))
                {
                    let now_s = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs() as i64;
                    if let Some(mt) = meta.regular_market_time
                        && now_s - mt > 5 * 86_400
                    {
                        continue;
                    }
                    let session_badge = meta
                        .regular_market_time
                        .map(|mt| {
                            if now_s - mt < 2400 || meta.market_state.as_deref() == Some("REGULAR")
                            {
                                "LIVE".to_string()
                            } else {
                                meta.market_state
                                    .clone()
                                    .unwrap_or_else(|| "CLOSED".to_string())
                            }
                        })
                        .or_else(|| meta.market_state.clone());
                    update_price_meta(&state, &sym, dec, meta.regular_market_volume, session_badge)
                        .await;
                    break;
                }
            }
        }
        let rows: Vec<(String,)> =
            sqlx::query_as("SELECT DISTINCT symbol FROM holdings WHERE asset_type = 'Stock'")
                .fetch_all(&state.db)
                .await
                .unwrap_or_default();
        for (sym,) in rows {
            if let Some((dec, vol, st, _, _)) = fetch_yahoo_quote(&client, &sym).await {
                update_price_meta(&state, &sym, dec, vol, st).await;
            }
        }
    }
}

async fn run_binance_ws(state: AppState) {
    let url = format!(
        "wss://{}.{}",
        ["ws-feed", "exchange", "coinbase"].join("."),
        "com"
    );
    let mut subscribed: std::collections::HashSet<String> = std::collections::HashSet::new();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("failed to build HTTP client");
    let api_host = ["api", "coinbase", "com"].join(".");

    loop {
        subscribed.clear();
        let Ok((ws_stream, _)) = connect_async(&url).await else {
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        };
        let (mut write, mut read) = ws_stream.split();
        let mut sync_tick = tokio::time::interval(Duration::from_secs(3));
        let mut cb_to_internal: HashMap<String, String> = HashMap::new();

        loop {
            tokio::select! {
                _ = sync_tick.tick() => {
                    let rows: Vec<(String,)> = sqlx::query_as("SELECT DISTINCT symbol FROM holdings WHERE asset_type = 'Crypto'")
                        .fetch_all(&state.db)
                        .await
                        .unwrap_or_default();

                    let mut new_products = Vec::new();
                    for (sym,) in rows {
                        let clean = sym.to_uppercase().replace(['/', '-'], "");
                        let product = if clean.ends_with("USDT") && clean.len() > 4 {
                            format!("{}-USD", &clean[..clean.len() - 4])
                        } else if clean.ends_with("USD") && clean.len() > 3 {
                            format!("{}-USD", &clean[..clean.len() - 3])
                        } else {
                            format!("{}-USD", clean)
                        };
                        cb_to_internal.insert(product.clone(), sym.clone());
                        if subscribed.insert(product.clone()) {
                            new_products.push(product.clone());
                            let spot_url = format!("https://{}/v2/prices/{}/spot", api_host, product);
                            if let Ok(resp) = client.get(&spot_url).send().await
                                && let Ok(parsed) = resp.json::<CoinbaseSpotResp>().await
                                && let Some(d) = parsed.data
                                && let Ok(dec) = Decimal::from_str(&d.amount)
                            {
                                update_price_meta(&state, &sym, dec, None, Some("LIVE".to_string())).await;
                            }
                        }
                    }

                    if !new_products.is_empty() {
                        let sub = serde_json::json!({
                            "type": "subscribe",
                            "product_ids": new_products,
                            "channels": ["ticker"]
                        });
                        if write.send(WsClientMessage::Text(sub.to_string().into())).await.is_err() {
                            break;
                        }
                    }
                }
                msg = read.next() => {
                    match msg {
                        Some(Ok(WsClientMessage::Text(txt))) => {
                            if let Ok(tick) = serde_json::from_str::<CoinbaseTicker>(&txt)
                                && tick.msg_type == "ticker"
                                && let Some(pid) = tick.product_id
                                && let Some(price_str) = tick.price
                                && let Some(internal_sym) = cb_to_internal.get(&pid)
                                && let Ok(price) = Decimal::from_str(&price_str)
                            {
                                update_price(&state, internal_sym, price).await;
                            }
                        }
                        Some(Ok(WsClientMessage::Ping(p))) => {
                            let _ = write.send(WsClientMessage::Pong(p)).await;
                        }
                        Some(Err(_)) | None => break,
                        _ => {}
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

async fn run_finnhub_ws(state: AppState) {
    let api_key = match std::env::var("FINNHUB_API_KEY") {
        Ok(k) if !k.trim().is_empty() => k,
        _ => {
            eprintln!("FINNHUB_API_KEY not set; Stocks WS in standby (Crypto WS & FX active).");
            return;
        }
    };
    let url = format!(
        "wss://{}?token={}",
        ["ws", "finnhub", "io"].join("."),
        api_key.trim()
    );
    let mut subscribed: std::collections::HashSet<String> = std::collections::HashSet::new();

    loop {
        subscribed.clear();
        let Ok((ws_stream, _)) = connect_async(&url).await else {
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        };
        let (mut write, mut read) = ws_stream.split();
        let mut sync_tick = tokio::time::interval(Duration::from_secs(3));
        let mut fh_to_internal: HashMap<String, String> = HashMap::new();

        loop {
            tokio::select! {
                _ = sync_tick.tick() => {
                            let rows: Vec<(String,)> = sqlx::query_as("SELECT DISTINCT symbol FROM holdings WHERE asset_type IN ('Stock', 'Index')")
                        .fetch_all(&state.db)
                        .await
                        .unwrap_or_default();

                    for (sym,) in rows {
                        let fh_sym = sym.to_uppercase();
                        fh_to_internal.insert(fh_sym.clone(), sym);
                        if subscribed.insert(fh_sym.clone()) {
                            let sub = serde_json::json!({ "type": "subscribe", "symbol": fh_sym });
                            if write.send(WsClientMessage::Text(sub.to_string().into())).await.is_err() {
                                break;
                            }
                        }
                    }
                }
                msg = read.next() => {
                    match msg {
                        Some(Ok(WsClientMessage::Text(txt))) => {
                            if let Ok(parsed) = serde_json::from_str::<FinnhubMsg>(&txt)
                                && parsed.msg_type == "trade"
                                && let Some(trades) = parsed.data
                            {
                                for t in trades {
                                    if let Some(internal_sym) = fh_to_internal.get(&t.s)
                                        && let Ok(price) = Decimal::from_str(&t.p.to_string())
                                    {
                                        update_price(&state, internal_sym, price).await;
                                    }
                                }
                            }
                        }
                        Some(Ok(WsClientMessage::Ping(p))) => {
                            let _ = write.send(WsClientMessage::Pong(p)).await;
                        }
                        Some(Err(_)) | None => break,
                        _ => {}
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

fn parse_side(s: &str) -> OrderSide {
    if s.eq_ignore_ascii_case("SHORT") {
        OrderSide::Short
    } else {
        OrderSide::Long
    }
}
fn parse_status(s: &str) -> OrderStatus {
    if s.eq_ignore_ascii_case("PENDING") {
        OrderStatus::Pending
    } else {
        OrderStatus::Open
    }
}
fn parse_tdir(s: &str) -> TriggerDir {
    if s.eq_ignore_ascii_case("ABOVE") {
        TriggerDir::Above
    } else {
        TriggerDir::Below
    }
}
fn parse_atype(s: &str) -> AssetType {
    match s {
        "Forex" => AssetType::Forex,
        "Crypto" => AssetType::Crypto,
        "Index" => AssetType::Index,
        _ => AssetType::Stock,
    }
}

async fn archive_and_close_holding(
    db: &SqlitePool,
    row: &HoldingRow,
    exit_price: Decimal,
    reason: &str,
) -> anyhow::Result<()> {
    if row.status.eq_ignore_ascii_case("PENDING") {
        sqlx::query("DELETE FROM holdings WHERE id = ? AND user_id = ?")
            .bind(row.id)
            .bind(row.user_id)
            .execute(db)
            .await?;
        return Ok(());
    }
    let qty = Decimal::from_str(&row.quantity).unwrap_or(Decimal::ZERO);
    let entry = Decimal::from_str(&row.avg_buy_price).unwrap_or(Decimal::ZERO);
    let val = evaluate_position(
        &Holding {
            symbol: row.symbol.clone(),
            asset_type: parse_atype(&row.asset_type),
            quantity: qty,
            avg_buy_price: entry,
            currency: row.currency.clone(),
        },
        exit_price,
        parse_side(&row.side),
        OrderStatus::Open,
    );
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    sqlx::query("INSERT INTO closed_trades (user_id, symbol, asset_type, side, currency, quantity, entry_price, exit_price, realized_pnl, close_reason, closed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .bind(row.user_id).bind(&row.symbol).bind(&row.asset_type).bind(&row.side).bind(&row.currency)
        .bind(&row.quantity).bind(&row.avg_buy_price).bind(exit_price.to_string()).bind(val.unrealized_pnl.to_string()).bind(reason).bind(now)
        .execute(db).await?;
    sqlx::query("DELETE FROM holdings WHERE id = ? AND user_id = ?")
        .bind(row.id)
        .bind(row.user_id)
        .execute(db)
        .await?;
    Ok(())
}

async fn process_symbol_triggers(db: &SqlitePool, symbol: &str, price: Decimal) {
    let rows: Vec<HoldingRow> = sqlx::query_as("SELECT id, user_id, symbol, asset_type, quantity, avg_buy_price, currency, side, status, trigger_dir, stop_loss, take_profit FROM holdings WHERE symbol = ?").bind(symbol).fetch_all(db).await.unwrap_or_default();
    for row in rows {
        let entry = Decimal::from_str(&row.avg_buy_price).unwrap_or(Decimal::ZERO);
        let sl = row
            .stop_loss
            .as_deref()
            .and_then(|v| Decimal::from_str(v).ok());
        let tp = row
            .take_profit
            .as_deref()
            .and_then(|v| Decimal::from_str(v).ok());
        let st = evaluate_order_triggers(
            &row.symbol,
            parse_atype(&row.asset_type),
            parse_side(&row.side),
            parse_status(&row.status),
            parse_tdir(&row.trigger_dir),
            entry,
            price,
            sl,
            tp,
        );
        if st.triggered_open {
            let _ = sqlx::query("UPDATE holdings SET status = 'OPEN' WHERE id = ?")
                .bind(row.id)
                .execute(db)
                .await;
        } else if let Some(reason) = st.close_reason {
            let r = match reason {
                CloseReason::StopLoss => "SL",
                CloseReason::TakeProfit => "TP",
                CloseReason::Manual => "MANUAL",
            };
            let _ = archive_and_close_holding(db, &row, price, r).await;
        }
    }
}

async fn update_price_meta(
    state: &AppState,
    symbol: &str,
    price: Decimal,
    volume: Option<u64>,
    market_state: Option<String>,
) {
    let is_live = matches!(market_state.as_deref(), Some("REGULAR" | "LIVE" | "24/7"));
    let mut price_map = state.prices.write().await;
    if !is_valid_market_tick(
        price,
        price_map.get(symbol).map(|t| t.price),
        volume,
        !symbol.starts_with('^') && is_live,
    ) {
        return;
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let vol = volume.or_else(|| price_map.get(symbol).and_then(|t| t.volume));
    let st = market_state.or_else(|| price_map.get(symbol).and_then(|t| t.market_state.clone()));
    price_map.insert(
        symbol.to_string(),
        PriceTick {
            symbol: symbol.to_string(),
            price,
            timestamp: now,
            volume: vol,
            market_state: st,
        },
    );
    let ticks: Vec<PriceTick> = price_map.values().cloned().collect();
    drop(price_map);
    if is_live {
        process_symbol_triggers(&state.db, symbol, price).await;
    }
    let _ = state.price_tx.send(ticks);
}

async fn update_price(state: &AppState, symbol: &str, price: Decimal) {
    update_price_meta(state, symbol, price, None, None).await;
}

async fn _unused_update_price(state: &AppState, symbol: &str, price: Decimal) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let mut price_map = state.prices.write().await;
    price_map.insert(
        symbol.to_string(),
        PriceTick {
            symbol: symbol.to_string(),
            price,
            timestamp: now,
            volume: None,
            market_state: None,
        },
    );
    let ticks: Vec<PriceTick> = price_map.values().cloned().collect();
    let _ = state.price_tx.send(ticks);
}

#[derive(Serialize, sqlx::FromRow)]
struct AuthUser {
    id: i64,
    email: String,
    name: String,
    provider: String,
}
#[derive(Deserialize)]
struct RegisterInput {
    email: String,
    name: Option<String>,
    password: String,
}
#[derive(Deserialize)]
struct LoginInput {
    email: String,
    password: String,
}

fn extract_session_token(headers: &HeaderMap) -> Option<String> {
    let raw = headers.get(COOKIE)?.to_str().ok()?;
    raw.split(';').find_map(|p| {
        p.trim()
            .strip_prefix("portfolio_session=")
            .filter(|v| !v.is_empty())
            .map(String::from)
    })
}
const SESSION_IDLE_TTL_SECS: i64 = 1_800;

async fn sliding_session_cookie(
    State(state): State<AppState>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let token = extract_session_token(req.headers());
    let mut res = next.run(req).await;
    if let Some(tok) = token
        && res.status().is_success()
        && !res.headers().contains_key(SET_COOKIE)
    {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let valid: Option<(i64,)> =
            sqlx::query_as("SELECT expires_at FROM sessions WHERE token = ? AND expires_at > ?")
                .bind(&tok)
                .bind(now)
                .fetch_optional(&state.db)
                .await
                .ok()
                .flatten();
        if valid.is_some()
            && let Ok(hv) = make_session_cookie(&tok, SESSION_IDLE_TTL_SECS).parse()
        {
            res.headers_mut().insert(SET_COOKIE, hv);
        }
    }

    res
}

async fn current_user(db: &SqlitePool, headers: &HeaderMap) -> Option<AuthUser> {
    let token = extract_session_token(headers)?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let user = sqlx::query_as::<_, AuthUser>("SELECT u.id, u.email, u.name, u.provider FROM sessions s JOIN users u ON u.id = s.user_id WHERE s.token = ? AND s.expires_at > ?")
        .bind(&token)
        .bind(now)
        .fetch_optional(db)
        .await
        .ok()
        .flatten()?;
    let _ = sqlx::query("UPDATE sessions SET expires_at = ? WHERE token = ? AND expires_at < ?")
        .bind(now + SESSION_IDLE_TTL_SECS)
        .bind(&token)
        .bind(now + SESSION_IDLE_TTL_SECS - 60)
        .execute(db)
        .await;
    Some(user)
}
fn make_session_cookie(token: &str, max_age: i64) -> String {
    format!(
        "portfolio_session={}; Path=/; HttpOnly; SameSite=Lax; Max-Age={}",
        token, max_age
    )
}
async fn create_session(db: &SqlitePool, user_id: i64) -> Result<String, String> {
    use rand::RngCore;
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    let token: String = bytes.iter().map(|b| format!("{:02x}", b)).collect();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let _ = sqlx::query("DELETE FROM sessions WHERE expires_at <= ?")
        .bind(now)
        .execute(db)
        .await;
    let exp = now + SESSION_IDLE_TTL_SECS;
    sqlx::query("INSERT INTO sessions (token, user_id, expires_at) VALUES (?, ?, ?)")
        .bind(&token)
        .bind(user_id)
        .bind(exp)
        .execute(db)
        .await
        .map_err(|e| e.to_string())?;
    Ok(token)
}
async fn auth_me(
    State((state, _)): State<(AppState, u64)>,
    headers: HeaderMap,
) -> impl IntoResponse {
    match current_user(&state.db, &headers).await {
        Some(u) => Json(u).into_response(),
        None => StatusCode::UNAUTHORIZED.into_response(),
    }
}
async fn auth_register(
    State((state, _)): State<(AppState, u64)>,
    Json(input): Json<RegisterInput>,
) -> impl IntoResponse {
    use argon2::{Argon2, PasswordHasher};
    let email = input.email.trim().to_lowercase();
    if !email.contains('@') || email.len() < 5 {
        return (StatusCode::BAD_REQUEST, "Enter a valid email.").into_response();
    }
    let has_upper = input.password.chars().any(|c| c.is_ascii_uppercase());
    let has_lower = input.password.chars().any(|c| c.is_ascii_lowercase());
    let has_digit = input.password.chars().any(|c| c.is_ascii_digit());
    if input.password.len() < 8 || !has_upper || !has_lower || !has_digit {
        return (
            StatusCode::BAD_REQUEST,
            "Password must be 8+ chars with uppercase, lowercase, and a number.",
        )
            .into_response();
    }
    let name = input
        .name
        .unwrap_or_else(|| email.split('@').next().unwrap_or("User").to_string())
        .trim()
        .to_string();
    let Ok(hash) = Argon2::default().hash_password(input.password.as_bytes()) else {
        return (StatusCode::INTERNAL_SERVER_ERROR, "Hash failed.").into_response();
    };
    let Ok(r) = sqlx::query(
        "INSERT INTO users (email, name, provider, password_hash) VALUES (?, ?, 'email', ?)",
    )
    .bind(&email)
    .bind(&name)
    .bind(hash.to_string())
    .execute(&state.db)
    .await
    else {
        return (StatusCode::CONFLICT, "Email already registered.").into_response();
    };
    let user_id = r.last_insert_rowid();
    let cnt: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM users")
        .fetch_one(&state.db)
        .await
        .unwrap_or((1,));
    if cnt.0 == 1 {
        sqlx::query("UPDATE holdings SET user_id = ? WHERE user_id IS NULL")
            .bind(user_id)
            .execute(&state.db)
            .await
            .ok();
    }
    let Ok(token) = create_session(&state.db, user_id).await else {
        return (StatusCode::INTERNAL_SERVER_ERROR, "Session failed.").into_response();
    };
    let mut resp = Json(AuthUser {
        id: user_id,
        email,
        name,
        provider: "email".into(),
    })
    .into_response();
    resp.headers_mut().insert(
        SET_COOKIE,
        make_session_cookie(&token, SESSION_IDLE_TTL_SECS)
            .parse()
            .unwrap(),
    );
    resp
}
async fn seed_demo_portfolio(state: &AppState, user_id: i64) {
    let _ = sqlx::query("DELETE FROM holdings WHERE user_id = ?")
        .bind(user_id)
        .execute(&state.db)
        .await;
    let _ = sqlx::query("DELETE FROM closed_trades WHERE user_id = ?")
        .bind(user_id)
        .execute(&state.db)
        .await;

    let sample_holdings = [
        (
            "AAPL",
            "Stock",
            "LONG",
            "8",
            "218.50",
            "USD",
            Some("195.00"),
            Some("265.00"),
        ),
        (
            "NVDA",
            "Stock",
            "LONG",
            "12",
            "128.40",
            "USD",
            Some("110.00"),
            Some("165.00"),
        ),
        (
            "ASML.AS",
            "Stock",
            "LONG",
            "1.5",
            "1150.00",
            "EUR",
            None,
            Some("1450.00"),
        ),
        (
            "EURUSD",
            "Forex",
            "LONG",
            "0.05",
            "1.0820",
            "USD",
            Some("1.0500"),
            Some("1.1350"),
        ),
        (
            "BTC-USD",
            "Crypto",
            "LONG",
            "0.08",
            "58500.00",
            "USD",
            Some("49000.00"),
            Some("75000.00"),
        ),
        (
            "^GSPC",
            "Index",
            "LONG",
            "2",
            "5790.00",
            "USD",
            Some("5400.00"),
            Some("6400.00"),
        ),
    ];

    for (sym, atype, side, qty, px, ccy, sl, tp) in sample_holdings {
        let _ = sqlx::query(
            "INSERT INTO holdings (user_id, symbol, asset_type, quantity, avg_buy_price, currency, side, status, trigger_dir, stop_loss, take_profit) VALUES (?, ?, ?, ?, ?, ?, ?, 'OPEN', 'BELOW', ?, ?)",
        )
        .bind(user_id)
        .bind(sym)
        .bind(atype)
        .bind(qty)
        .bind(px)
        .bind(ccy)
        .bind(side)
        .bind(sl)
        .bind(tp)
        .execute(&state.db)
        .await;

        let parsed_atype = match atype {
            "Forex" => AssetType::Forex,
            "Crypto" => AssetType::Crypto,
            "Index" => AssetType::Index,
            _ => AssetType::Stock,
        };
        if let (Ok(d_px), Ok((live_px, live_vol, live_st))) = (
            Decimal::from_str(px),
            verify_asset_live(
                sym,
                &parsed_atype,
                Decimal::from_str(px).unwrap_or(Decimal::ONE),
            )
            .await,
        ) {
            let _ = d_px;
            update_price_meta(state, sym, live_px, live_vol, live_st).await;
        }
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let sample_trades = [
        (
            "MSFT",
            "Stock",
            "LONG",
            "USD",
            "5",
            "402.00",
            "438.50",
            "182.50",
            "TP",
            now - 172800,
        ),
        (
            "USDJPY",
            "Forex",
            "SHORT",
            "JPY",
            "0.04",
            "161.200",
            "159.400",
            "46.80",
            "MANUAL",
            now - 86400,
        ),
        (
            "ETH-USD",
            "Crypto",
            "LONG",
            "USD",
            "0.6",
            "2890.00",
            "2740.00",
            "-90.00",
            "SL",
            now - 43200,
        ),
    ];
    for (sym, atype, side, ccy, qty, entry, exit, rpnl, reason, closed_at) in sample_trades {
        let _ = sqlx::query(
            "INSERT INTO closed_trades (user_id, symbol, asset_type, side, currency, quantity, entry_price, exit_price, realized_pnl, close_reason, closed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(user_id)
        .bind(sym)
        .bind(atype)
        .bind(side)
        .bind(ccy)
        .bind(qty)
        .bind(entry)
        .bind(exit)
        .bind(rpnl)
        .bind(reason)
        .bind(closed_at)
        .execute(&state.db)
        .await;
    }
}

async fn auth_demo(State((state, _)): State<(AppState, u64)>) -> impl IntoResponse {
    let demo_email = "demo@portfolio.local";
    let demo_name = "Demo Trader";
    let _ =
        sqlx::query("INSERT OR IGNORE INTO users (email, name, provider) VALUES (?, ?, 'demo')")
            .bind(demo_email)
            .bind(demo_name)
            .execute(&state.db)
            .await;

    let Some((user_id,)) = sqlx::query_as::<_, (i64,)>("SELECT id FROM users WHERE email = ?")
        .bind(demo_email)
        .fetch_optional(&state.db)
        .await
        .ok()
        .flatten()
    else {
        return (StatusCode::INTERNAL_SERVER_ERROR, "Demo user init failed.").into_response();
    };

    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM holdings WHERE user_id = ?")
        .bind(user_id)
        .fetch_one(&state.db)
        .await
        .unwrap_or((0,));
    if count.0 == 0 {
        seed_demo_portfolio(&state, user_id).await;
    }

    let Ok(token) = create_session(&state.db, user_id).await else {
        return (StatusCode::INTERNAL_SERVER_ERROR, "Session failed.").into_response();
    };
    let mut resp = Json(AuthUser {
        id: user_id,
        email: demo_email.into(),
        name: demo_name.into(),
        provider: "demo".into(),
    })
    .into_response();
    resp.headers_mut().insert(
        SET_COOKIE,
        make_session_cookie(&token, SESSION_IDLE_TTL_SECS)
            .parse()
            .unwrap(),
    );
    resp
}

async fn auth_demo_reset(
    State((state, _)): State<(AppState, u64)>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let Some(user) = current_user(&state.db, &headers).await else {
        return (StatusCode::UNAUTHORIZED, "Sign in first.").into_response();
    };
    if user.provider != "demo" {
        return (
            StatusCode::FORBIDDEN,
            "Reset is only available in Demo Mode.",
        )
            .into_response();
    }
    seed_demo_portfolio(&state, user.id).await;
    StatusCode::OK.into_response()
}

async fn auth_login(
    State((state, _)): State<(AppState, u64)>,
    Json(input): Json<LoginInput>,
) -> impl IntoResponse {
    use argon2::{Argon2, PasswordHash, PasswordVerifier};
    let email = input.email.trim().to_lowercase();
    let Some((id, em, name, provider, Some(phash))) =
        sqlx::query_as::<_, (i64, String, String, String, Option<String>)>(
            "SELECT id, email, name, provider, password_hash FROM users WHERE email = ?",
        )
        .bind(&email)
        .fetch_optional(&state.db)
        .await
        .ok()
        .flatten()
    else {
        return (StatusCode::UNAUTHORIZED, "Invalid email or password.").into_response();
    };
    let Ok(parsed) = PasswordHash::new(&phash) else {
        return (StatusCode::UNAUTHORIZED, "Invalid email or password.").into_response();
    };
    if Argon2::default()
        .verify_password(input.password.as_bytes(), &parsed)
        .is_err()
    {
        return (StatusCode::UNAUTHORIZED, "Invalid email or password.").into_response();
    }
    let Ok(token) = create_session(&state.db, id).await else {
        return (StatusCode::INTERNAL_SERVER_ERROR, "Session failed.").into_response();
    };
    let mut resp = Json(AuthUser {
        id,
        email: em,
        name,
        provider,
    })
    .into_response();
    resp.headers_mut().insert(
        SET_COOKIE,
        make_session_cookie(&token, SESSION_IDLE_TTL_SECS)
            .parse()
            .unwrap(),
    );
    resp
}
async fn auth_logout(
    State((state, _)): State<(AppState, u64)>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Some(t) = extract_session_token(&headers) {
        sqlx::query("DELETE FROM sessions WHERE token = ?")
            .bind(t)
            .execute(&state.db)
            .await
            .ok();
    }
    let mut resp = StatusCode::OK.into_response();
    resp.headers_mut()
        .insert(SET_COOKIE, make_session_cookie("", 0).parse().unwrap());
    resp
}
#[derive(Deserialize)]
struct GoogleCallbackQuery {
    code: String,
    state: String,
}
#[derive(Deserialize)]
struct GoogleTokenResp {
    access_token: String,
}
#[derive(Deserialize)]
struct GoogleUserInfo {
    sub: String,
    email: String,
    name: Option<String>,
}

fn extract_cookie_val(headers: &HeaderMap, key: &str) -> Option<String> {
    let raw = headers.get(COOKIE)?.to_str().ok()?;
    let prefix = format!("{}=", key);
    raw.split(';').find_map(|p| {
        p.trim()
            .strip_prefix(&prefix)
            .filter(|v| !v.is_empty())
            .map(String::from)
    })
}
fn resolve_google_redirect(headers: &HeaderMap) -> String {
    std::env::var("GOOGLE_REDIRECT_URI").unwrap_or_else(|_| {
        let host = headers
            .get("host")
            .and_then(|h| h.to_str().ok())
            .unwrap_or("localhost:8083");
        let proto = if host.starts_with("localhost") || host.starts_with("127.") {
            "http"
        } else {
            "https"
        };
        format!("{}://{}/portfolio/api/auth/google/callback", proto, host)
    })
}
async fn auth_google_start(headers: HeaderMap) -> impl IntoResponse {
    use rand::RngCore;
    let Ok(cid) = std::env::var("GOOGLE_CLIENT_ID").map(|v| v.trim().to_string()) else {
        return (StatusCode::SERVICE_UNAVAILABLE, "GOOGLE_CLIENT_ID not set").into_response();
    };
    let redir: String = resolve_google_redirect(&headers)
        .bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) {
                (b as char).to_string()
            } else {
                format!("%{:02X}", b)
            }
        })
        .collect();
    let mut bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut bytes);
    let st: String = bytes.iter().map(|b| format!("{:02x}", b)).collect();
    let url = format!(
        "https://accounts.google.com/o/oauth2/v2/auth?client_id={}&redirect_uri={}&response_type=code&scope=openid%20email%20profile&state={}&prompt=select_account",
        cid, redir, st
    );
    let mut resp = Redirect::temporary(&url).into_response();
    resp.headers_mut().insert(
        SET_COOKIE,
        format!(
            "oauth_state={}; Path=/; HttpOnly; SameSite=Lax; Max-Age=600",
            st
        )
        .parse()
        .unwrap(),
    );
    resp
}
async fn auth_google_callback(
    State((state, _)): State<(AppState, u64)>,
    headers: HeaderMap,
    Query(q): Query<GoogleCallbackQuery>,
) -> impl IntoResponse {
    let Some(saved) = extract_cookie_val(&headers, "oauth_state") else {
        return (StatusCode::BAD_REQUEST, "Missing OAuth state.").into_response();
    };
    if saved != q.state {
        return (StatusCode::BAD_REQUEST, "Invalid OAuth state.").into_response();
    }
    let (Ok(cid), Ok(sec)) = (
        std::env::var("GOOGLE_CLIENT_ID").map(|v| v.trim().to_string()),
        std::env::var("GOOGLE_CLIENT_SECRET").map(|v| v.trim().to_string()),
    ) else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "Missing Google credentials.",
        )
            .into_response();
    };
    let redir = resolve_google_redirect(&headers);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(8))
        .build()
        .unwrap();
    let enc = |v: &str| -> String {
        v.bytes()
            .map(|b| {
                if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) {
                    (b as char).to_string()
                } else {
                    format!("%{:02X}", b)
                }
            })
            .collect()
    };
    let body = format!(
        "code={}&client_id={}&client_secret={}&redirect_uri={}&grant_type=authorization_code",
        enc(&q.code),
        enc(&cid),
        enc(&sec),
        enc(&redir)
    );
    let tok_url = format!(
        "https://{}/token",
        ["oauth2", "googleapis", "com"].join(".")
    );
    let Ok(tr) = client
        .post(&tok_url)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await
    else {
        return (StatusCode::BAD_GATEWAY, "Token request failed.").into_response();
    };
    let Ok(tok) = tr.json::<GoogleTokenResp>().await else {
        return (StatusCode::UNAUTHORIZED, "Token exchange rejected.").into_response();
    };
    let Ok(ur) = client
        .get(format!(
            "https://{}/v1/userinfo",
            ["openidconnect", "googleapis", "com"].join(".")
        ))
        .bearer_auth(&tok.access_token)
        .send()
        .await
    else {
        return (StatusCode::BAD_GATEWAY, "Profile request failed.").into_response();
    };
    let Ok(info) = ur.json::<GoogleUserInfo>().await else {
        return (StatusCode::UNAUTHORIZED, "Invalid profile.").into_response();
    };
    let email = info.email.trim().to_lowercase();
    let name = info
        .name
        .unwrap_or_else(|| email.split('@').next().unwrap_or("User").to_string());
    let existing: Option<(i64,)> = sqlx::query_as("SELECT id FROM users WHERE email = ?")
        .bind(&email)
        .fetch_optional(&state.db)
        .await
        .ok()
        .flatten();
    let uid = if let Some((id,)) = existing {
        sqlx::query("UPDATE users SET provider_sub = COALESCE(provider_sub, ?) WHERE id = ?")
            .bind(&info.sub)
            .bind(id)
            .execute(&state.db)
            .await
            .ok();
        id
    } else {
        let Ok(r) = sqlx::query(
            "INSERT INTO users (email, name, provider, provider_sub) VALUES (?, ?, 'google', ?)",
        )
        .bind(&email)
        .bind(&name)
        .bind(&info.sub)
        .execute(&state.db)
        .await
        else {
            return (StatusCode::INTERNAL_SERVER_ERROR, "DB error.").into_response();
        };
        let id = r.last_insert_rowid();
        let cnt: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM users")
            .fetch_one(&state.db)
            .await
            .unwrap_or((1,));
        if cnt.0 == 1 {
            sqlx::query("UPDATE holdings SET user_id = ? WHERE user_id IS NULL")
                .bind(id)
                .execute(&state.db)
                .await
                .ok();
        }
        id
    };
    let Ok(st) = create_session(&state.db, uid).await else {
        return (StatusCode::INTERNAL_SERVER_ERROR, "Session error.").into_response();
    };
    let mut resp = Redirect::temporary("/portfolio").into_response();
    resp.headers_mut().append(
        SET_COOKIE,
        make_session_cookie(&st, SESSION_IDLE_TTL_SECS)
            .parse()
            .unwrap(),
    );
    resp.headers_mut().append(
        SET_COOKIE,
        "oauth_state=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0"
            .parse()
            .unwrap(),
    );
    resp
}
async fn index() -> impl IntoResponse {
    (
        [("Cache-Control", "no-store, must-revalidate")],
        (
            [(
                axum::http::header::CACHE_CONTROL,
                "no-store, no-cache, must-revalidate",
            )],
            (
                [(
                    axum::http::header::CACHE_CONTROL,
                    "no-store, no-cache, must-revalidate",
                )],
                Html(include_str!("index.html")),
            ),
        ),
    )
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

#[derive(Deserialize)]
struct PortfolioQuery {
    base: Option<String>,
}

async fn get_portfolio(
    State((state, interval_secs)): State<(AppState, u64)>,
    headers: HeaderMap,
    Query(pq): Query<PortfolioQuery>,
) -> impl IntoResponse {
    let Some(user) = current_user(&state.db, &headers).await else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let base_mode = pq.base.unwrap_or_else(|| "NATIVE".into()).to_uppercase();
    let rows: Vec<HoldingRow> = sqlx::query_as(
        "SELECT id, user_id, symbol, asset_type, quantity, avg_buy_price, currency, side, status, trigger_dir, stop_loss, take_profit FROM holdings WHERE user_id = ? ORDER BY id DESC",
    )
    .bind(user.id)
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

    let prices = state.prices.read().await;
    let mut holdings = Vec::with_capacity(rows.len());
    let mut total_market_value = Decimal::ZERO;
    let mut total_cost_basis = Decimal::ZERO;
    let mut total_unrealized_pnl = Decimal::ZERO;
    let closed_rows: Vec<ClosedTradeRow> = sqlx::query_as("SELECT id, symbol, asset_type, side, currency, quantity, entry_price, exit_price, realized_pnl, close_reason, closed_at FROM closed_trades WHERE user_id = ?").bind(user.id).fetch_all(&state.db).await.unwrap_or_default();

    for row in rows {
        let quantity = Decimal::from_str(&row.quantity).unwrap_or(Decimal::ZERO);
        let avg_buy_price = Decimal::from_str(&row.avg_buy_price).unwrap_or(Decimal::ZERO);
        let asset_type = match row.asset_type.as_str() {
            "Forex" => AssetType::Forex,
            "Crypto" => AssetType::Crypto,
            "Index" => AssetType::Index,
            _ => AssetType::Stock,
        };

        let tick = prices.get(&row.symbol);
        let current_price = tick.map(|t| t.price);
        let volume = tick.and_then(|t| t.volume);
        let market_state = tick.and_then(|t| t.market_state.clone());
        let to_usd = if row.currency == "USD" {
            Decimal::ONE
        } else if let Some(m) = prices.get(&format!("{}USD", row.currency)) {
            m.price
        } else if let Some(m) = prices.get(&format!("USD{}", row.currency))
            && !m.price.is_zero()
        {
            Decimal::ONE / m.price
        } else {
            Decimal::ONE
        };
        let eur_usd = prices
            .get("EURUSD")
            .map(|m| m.price)
            .filter(|p| !p.is_zero())
            .unwrap_or(Decimal::ONE);
        let display_mult = match base_mode.as_str() {
            "USD" => to_usd,
            "EUR" => {
                if row.currency == "EUR" {
                    Decimal::ONE
                } else {
                    to_usd / eur_usd
                }
            }
            _ => Decimal::ONE,
        };

        let valuation = current_price.map(|price| {
            let core_holding = Holding {
                symbol: row.symbol.clone(),
                asset_type: asset_type.clone(),
                quantity,
                avg_buy_price,
                currency: row.currency.clone(),
            };
            let mut v = evaluate_position(
                &core_holding,
                price,
                parse_side(&row.side),
                parse_status(&row.status),
            );
            total_market_value += v.market_value * to_usd;
            total_cost_basis += v.cost_basis * to_usd;
            total_unrealized_pnl += v.unrealized_pnl * to_usd;
            v.market_value *= display_mult;
            v.cost_basis *= display_mult;
            v.unrealized_pnl *= display_mult;
            v
        });

        let (mut near_sl, mut near_tp) = (false, false);
        if let Some(cp) = current_price {
            let al = evaluate_order_triggers(
                &row.symbol,
                asset_type.clone(),
                parse_side(&row.side),
                parse_status(&row.status),
                parse_tdir(&row.trigger_dir),
                avg_buy_price,
                cp,
                row.stop_loss
                    .as_deref()
                    .and_then(|x| Decimal::from_str(x).ok()),
                row.take_profit
                    .as_deref()
                    .and_then(|x| Decimal::from_str(x).ok()),
            );
            near_sl = al.near_sl;
            near_tp = al.near_tp;
        }
        holdings.push(HoldingResponse {
            id: row.id,
            symbol: row.symbol,
            asset_type,
            quantity,
            avg_buy_price,
            currency: row.currency,
            side: row.side,
            status: row.status,
            stop_loss: row
                .stop_loss
                .as_deref()
                .and_then(|x| Decimal::from_str(x).ok()),
            take_profit: row
                .take_profit
                .as_deref()
                .and_then(|x| Decimal::from_str(x).ok()),
            near_sl,
            near_tp,
            current_price,
            volume,
            market_state,
            valuation,
        });
    }

    let eur_usd = prices
        .get("EURUSD")
        .map(|m| m.price)
        .filter(|p| !p.is_zero())
        .unwrap_or(Decimal::ONE);
    let summary_ccy = if base_mode == "EUR" { "EUR" } else { "USD" };
    let mut total_realized_pnl = Decimal::ZERO;
    for ct in &closed_rows {
        let rp = Decimal::from_str(&ct.realized_pnl).unwrap_or(Decimal::ZERO);
        let m = if ct.currency == "USD" {
            Decimal::ONE
        } else if let Some(x) = prices.get(&format!("{}USD", ct.currency)) {
            x.price
        } else if let Some(x) = prices.get(&format!("USD{}", ct.currency))
            && !x.price.is_zero()
        {
            Decimal::ONE / x.price
        } else {
            Decimal::ONE
        };
        total_realized_pnl += rp * m;
    }
    if summary_ccy == "EUR" {
        total_market_value /= eur_usd;
        total_cost_basis /= eur_usd;
        total_unrealized_pnl /= eur_usd;
        total_realized_pnl /= eur_usd;
    }
    Json(PortfolioSummary {
        base_currency: summary_ccy.to_string(),
        eur_usd_rate: eur_usd,
        holdings,
        total_market_value,
        total_cost_basis,
        total_unrealized_pnl,
        total_realized_pnl,
        update_interval_secs: interval_secs,
    })
    .into_response()
}

fn infer_market_tag(symbol: &str, asset_type: &AssetType) -> &'static str {
    match asset_type {
        AssetType::Forex => "FX",
        AssetType::Crypto => "CRYPTO",
        AssetType::Index => "INDEX",
        AssetType::Stock => {
            if let Some((_, suffix)) = symbol.rsplit_once('.') {
                match suffix {
                    "DE" | "F" | "MU" | "BE" | "DU" | "HM" => "DE",
                    "PA" => "FR",
                    "AS" => "NL",
                    "MI" => "IT",
                    "MC" => "ES",
                    "L" => "UK",
                    "SW" => "CH",
                    "VI" => "AT",
                    "BR" => "BE",
                    "LS" => "PT",
                    "IR" => "IE",
                    "ST" => "SE",
                    "OL" => "NO",
                    "CO" => "DK",
                    "HE" => "FI",
                    "T" => "JP",
                    "HK" => "HK",
                    "TO" => "CA",
                    "AX" => "AU",
                    _ => "INTL",
                }
            } else {
                "US"
            }
        }
    }
}

#[derive(Deserialize)]
struct SearchQuery {
    q: String,
}

#[derive(Serialize)]
struct SearchItem {
    symbol: String,
    name: String,
    asset_type: AssetType,
    currency: String,
    price: Option<Decimal>,
}

fn map_cfd_index(up: &str) -> Option<&'static str> {
    match up {
        "US500" | "SPX500" | "SP500" | "SPX" => Some("ES=F"),
        "NAS100" | "US100" | "USTEC" | "NDX" => Some("NQ=F"),
        "US30" | "DJ30" | "WS30" | "DJI" => Some("YM=F"),
        "JP225" | "JPN225" | "NIKKEI225" => Some("NKD=F"),
        "DE40" | "GER40" | "DE30" | "DAX40" | "DAX" => Some("^GDAXI"),
        "UK100" | "FTSE100" | "FTSE" => Some("^FTSE"),
        "FRA40" | "CAC40" => Some("^FCHI"),
        "ESP35" | "IBEX35" => Some("^IBEX"),
        "EU50" | "EUSTX50" | "STOXX50" => Some("^STOXX50E"),
        "AUS200" | "AU200" => Some("^AXJO"),
        "HK50" | "HSI" => Some("^HSI"),
        "CH20" | "SMI" => Some("^SSMI"),
        _ => None,
    }
}

async fn fetch_yahoo_quote(
    client: &reqwest::Client,
    ticker: &str,
) -> Option<(
    Decimal,
    Option<u64>,
    Option<String>,
    Option<String>,
    Option<String>,
)> {
    let yf_host = ["query1", "finance", "yahoo", "com"].join(".");
    let url = format!(
        "https://{}/v8/finance/chart/{}?interval=1m&range=1d",
        yf_host,
        ticker.replace('^', "%5E")
    );
    let resp = client
        .get(&url)
        .header("User-Agent", "Mozilla/5.0")
        .send()
        .await
        .ok()?;
    let parsed = resp.json::<YahooChartResp>().await.ok()?;
    let meta = parsed.chart?.result?.first()?.meta.clone()?;
    let price = meta.regular_market_price.filter(|&p| p > 0.0)?;
    let now_s = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    if let Some(mt) = meta.regular_market_time
        && now_s - mt > 5 * 86_400
    {
        return None;
    }
    let dec = Decimal::from_str(&format!("{:.4}", price)).ok()?;
    let badge = meta
        .regular_market_time
        .map(|mt| {
            if now_s - mt < 2400 || meta.market_state.as_deref() == Some("REGULAR") {
                "LIVE".to_string()
            } else {
                meta.market_state
                    .clone()
                    .unwrap_or_else(|| "CLOSED".to_string())
            }
        })
        .or(meta.market_state);
    Some((
        dec,
        meta.regular_market_volume,
        badge,
        meta.instrument_type,
        meta.currency,
    ))
}

#[derive(Deserialize)]
struct HistoryQuery {
    symbol: String,
    asset_type: Option<String>,
    range: Option<String>,
}

#[derive(Serialize)]
struct HistoryPoint {
    ts: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
}

#[derive(Serialize)]
struct HistoryResponse {
    symbol: String,
    exchange: Option<String>,
    currency: Option<String>,
    volume: Option<u64>,
    prev_close: Option<f64>,
    high_52w: Option<f64>,
    low_52w: Option<f64>,
    atr_14d_pips: Option<f64>,
    dividend_rate: Option<f64>,
    dividend_yield: Option<f64>,
    points: Vec<HistoryPoint>,
}

async fn get_history(Query(hq): Query<HistoryQuery>) -> Json<HistoryResponse> {
    let raw = hq.symbol.trim().to_uppercase();
    let at = hq.asset_type.unwrap_or_default();
    let ticker = if at == "Index" {
        map_cfd_index(&raw).unwrap_or(&raw).to_string()
    } else if at == "Forex" && raw.len() == 6 {
        format!("{}=X", raw)
    } else if at == "Crypto" && !raw.contains('-') {
        format!(
            "{}-USD",
            raw.trim_end_matches("USDT")
                .trim_end_matches("USD")
                .trim_end_matches('-')
        )
    } else {
        raw.clone()
    };
    let range = match hq.range.as_deref() {
        Some("1m") => ("1d", "1m", 1usize),
        Some("5m") => ("5d", "5m", 1usize),
        Some("15m") => ("5d", "15m", 1),
        Some("1h") => ("5d", "1h", 1),
        Some("4h") => ("1mo", "1h", 4),
        Some("1w") | Some("7d") | Some("5d") => ("5d", "1h", 1),
        Some("1mo") => ("1mo", "1d", 1),
        Some("6mo") => ("6mo", "1d", 1),
        Some("1y") => ("1y", "1wk", 1),
        _ => ("1d", "15m", 1),
    };
    let yf_hosts = [
        ["query1", "finance", "yahoo", "com"].join("."),
        ["query2", "finance", "yahoo", "com"].join("."),
    ];
    let yf_host = yf_hosts.first().unwrap();
    let pre_post = if at == "Stock" && !ticker.contains('.') {
        "&includePrePost=true"
    } else {
        ""
    };
    let client = reqwest::Client::builder()
        .user_agent("Mozilla/5.0")
        .timeout(Duration::from_secs(6))
        .build()
        .unwrap();
    let mut out = HistoryResponse {
        symbol: raw,
        exchange: None,
        currency: None,
        volume: None,
        prev_close: None,
        high_52w: None,
        low_52w: None,
        atr_14d_pips: None,
        dividend_rate: None,
        dividend_yield: None,
        points: vec![],
    };
    let attempts: Vec<(&str, &str, usize)> =
        if matches!(range.0, "1mo" | "6mo" | "1y") && range.2 == 1 {
            vec![(range.0, range.1, 1), ("1mo", "1h", 4), ("5d", "1h", 4)]
        } else {
            vec![range, ("5d", "15m", 1), ("1mo", "1d", 1)]
        };
    for (r_span, r_int, r_grp) in attempts {
        if out.points.len() > 1 {
            break;
        }
        out.points.clear();
        let ev = "";
        for host in &yf_hosts {
            if !out.points.is_empty() {
                break;
            }
            let url = format!(
                "https://{}/v8/finance/chart/{}?interval={}&range={}{}{}",
                host, ticker, r_int, r_span, pre_post, ev
            );
            if let Ok(resp) = client
                .get(&url)
                .header("User-Agent", "Mozilla/5.0")
                .send()
                .await
                && let Ok(val) = resp.json::<serde_json::Value>().await
                && let Some(res) = val["chart"]["result"].as_array().and_then(|a| a.first())
            {
                let meta = &res["meta"];
                out.exchange = meta["exchangeName"].as_str().map(String::from);
                out.currency = meta["currency"].as_str().map(String::from);
                out.volume = meta["regularMarketVolume"].as_u64();
                out.prev_close = meta["regularMarketPreviousClose"]
                    .as_f64()
                    .or_else(|| meta["previousClose"].as_f64())
                    .or_else(|| meta["chartPreviousClose"].as_f64());
                out.high_52w = meta["fiftyTwoWeekHigh"].as_f64();
                out.low_52w = meta["fiftyTwoWeekLow"].as_f64();
                let q = &res["indicators"]["quote"][0];
                if let (Some(ts_arr), Some(cl_arr)) =
                    (res["timestamp"].as_array(), q["close"].as_array())
                {
                    let op_arr = q["open"].as_array();
                    let hi_arr = q["high"].as_array();
                    let lo_arr = q["low"].as_array();
                    let adj_arr = res["indicators"]["adjclose"][0]["adjclose"].as_array();
                    for (i, (t, c)) in ts_arr.iter().zip(cl_arr.iter()).enumerate() {
                        let cl_val = c
                            .as_f64()
                            .or_else(|| adj_arr.and_then(|a| a.get(i)).and_then(|v| v.as_f64()));
                        if let (Some(ts), Some(close)) = (t.as_i64(), cl_val)
                            && close > 0.0
                        {
                            let open = op_arr
                                .and_then(|a| a.get(i))
                                .and_then(|v| v.as_f64())
                                .filter(|&x| x > 0.0)
                                .unwrap_or(close);
                            let high = hi_arr
                                .and_then(|a| a.get(i))
                                .and_then(|v| v.as_f64())
                                .filter(|&x| x > 0.0)
                                .unwrap_or(close.max(open));
                            let low = lo_arr
                                .and_then(|a| a.get(i))
                                .and_then(|v| v.as_f64())
                                .filter(|&x| x > 0.0)
                                .unwrap_or(close.min(open));
                            if r_grp > 1
                                && out.points.last().is_some_and(|l| {
                                    (ts - l.ts) > 0
                                        && (ts - l.ts) < (r_grp as i64 * 3600)
                                        && (ts / 86_400 == l.ts / 86_400)
                                })
                            {
                                if let Some(last) = out.points.last_mut() {
                                    last.high = last.high.max(high);
                                    last.low = last.low.min(low);
                                    last.close = close;
                                }
                            } else {
                                out.points.push(HistoryPoint {
                                    ts,
                                    open,
                                    high,
                                    low,
                                    close,
                                });
                            }
                        }
                    }
                }
            }
        }
    }
    if at == "Stock"
        && let Ok(dr) = client
            .get(format!(
                "https://{}/v8/finance/chart/{}?interval=1mo&range=1y&events=div",
                yf_host,
                ticker.replace('^', "%5E")
            ))
            .send()
            .await
        && let Ok(dj) = dr.json::<serde_json::Value>().await
        && let Some(dres) = dj["chart"]["result"].as_array().and_then(|a| a.first())
    {
        let last_px = dres["meta"]["regularMarketPrice"]
            .as_f64()
            .or_else(|| out.points.last().map(|p| p.close))
            .unwrap_or(0.0);
        if let Some(div_map) = dres["events"]["dividends"].as_object() {
            let annual_div: f64 = div_map
                .values()
                .filter_map(|v| v["amount"].as_f64())
                .filter(|&a| a > 0.0)
                .sum();
            if annual_div > 0.0 {
                out.dividend_rate = Some((annual_div * 100.0).round() / 100.0);
                if last_px > 0.0 {
                    out.dividend_yield = Some(((annual_div / last_px) * 10000.0).round() / 100.0);
                }
            }
        }
    }
    if at == "Forex"
        && let Ok(dr) = client
            .get(format!(
                "https://{}/v8/finance/chart/{}?interval=1d&range=3mo",
                yf_host, ticker
            ))
            .send()
            .await
        && let Ok(dj) = dr.json::<serde_json::Value>().await
        && let Some(dres) = dj["chart"]["result"].as_array().and_then(|a| a.first())
    {
        let dq = &dres["indicators"]["quote"][0];
        if let (Some(hs), Some(ls), Some(cs)) = (
            dq["high"].as_array(),
            dq["low"].as_array(),
            dq["close"].as_array(),
        ) {
            let bars: Vec<(f64, f64, f64)> = (0..hs.len())
                .filter_map(|i| {
                    Some((
                        hs.get(i)?.as_f64()?,
                        ls.get(i)?.as_f64()?,
                        cs.get(i)?.as_f64()?,
                    ))
                })
                .filter(|b| b.0 > 0.0 && b.1 > 0.0 && b.2 > 0.0)
                .collect();
            if bars.len() >= 2 {
                let trs: Vec<f64> = bars
                    .windows(2)
                    .map(|w| {
                        (w[1].0 - w[1].1)
                            .max((w[1].0 - w[0].2).abs())
                            .max((w[1].1 - w[0].2).abs())
                    })
                    .collect();
                let n = trs.len().min(14);
                let atr = trs[trs.len() - n..].iter().sum::<f64>() / (n as f64);
                let pip = if out.symbol.ends_with("JPY") {
                    0.01
                } else {
                    0.0001
                };
                out.atr_14d_pips = Some((atr / pip * 10.0).round() / 10.0);
            }
        }
    }
    Json(out)
}

async fn search_symbols(Query(q): Query<SearchQuery>) -> Json<Vec<SearchItem>> {
    let term = q.q.trim().to_uppercase();
    if term.len() < 2 {
        return Json(vec![]);
    }
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let mut raw_items: Vec<(String, String, AssetType)> = Vec::new();
    let mut seen = std::collections::HashSet::new();

    let aliases = [
        ("US500", "S&P 500 Futures"),
        ("NAS100", "Nasdaq 100 Futures"),
        ("US30", "Dow Jones 30 Futures"),
        ("JP225", "Nikkei 225 Futures"),
        ("DE40", "DAX 40 Index"),
        ("UK100", "FTSE 100 Index"),
    ];
    for (sym, desc) in aliases {
        if (sym.contains(&term) || desc.to_uppercase().contains(&term))
            && seen.insert(sym.to_string())
        {
            raw_items.push((sym.to_string(), desc.to_string(), AssetType::Index));
        }
    }

    let clean_fx = term.replace(['/', '-'], "");
    if clean_fx.len() == 6
        && clean_fx.chars().all(|c| c.is_ascii_alphabetic())
        && seen.insert(clean_fx.clone())
    {
        raw_items.push((
            clean_fx.clone(),
            format!("{}/{} Spot", &clean_fx[..3], &clean_fx[3..]),
            AssetType::Forex,
        ));
    }

    let yf_host = ["query1", "finance", "yahoo", "com"].join(".");
    let mut queries: Vec<(String, &'static str)> = if term.contains('.') {
        vec![(term.clone(), "US")]
    } else {
        vec![
            (term.clone(), "US&lang=en-US"),
            (term.clone(), "DE&lang=de-DE"),
            (term.clone(), "GB&lang=en-GB"),
            (term.clone(), "IT&lang=it-IT"),
            (term.clone(), "FR&lang=fr-FR"),
            (term.clone(), "CH&lang=de-CH"),
            (format!("{}.DE", term), "DE&lang=de-DE"),
            (format!("{}.MI", term), "IT&lang=it-IT"),
            (format!("{}.L", term), "GB&lang=en-GB"),
            (format!("{}.PA", term), "FR&lang=fr-FR"),
        ]
    };
    let mut idx = 0;
    while idx < queries.len() {
        let (q_str, reg) = queries[idx].clone();
        idx += 1;
        let surl = format!(
            "https://{}/v1/finance/search?q={}&region={}&quotesCount=6&newsCount=0",
            yf_host, q_str, reg
        );
        if let Ok(r) = client
            .get(&surl)
            .header("User-Agent", "Mozilla/5.0")
            .send()
            .await
            && let Ok(sp) = r.json::<YahooSearchResp>().await
        {
            for item in sp.quotes.unwrap_or_default() {
                let Some(sym) = item.symbol else { continue };
                if let Some((_, sfx)) = sym.rsplit_once('.')
                    && !matches!(
                        sfx,
                        "DE" | "F"
                            | "PA"
                            | "AS"
                            | "MI"
                            | "MC"
                            | "L"
                            | "SW"
                            | "ST"
                            | "VI"
                            | "BR"
                            | "LS"
                    )
                {
                    continue;
                }
                let qt = item.quote_type.as_deref().unwrap_or("");
                let at = match qt {
                    "INDEX" | "FUTURE" => AssetType::Index,
                    "CRYPTOCURRENCY" => AssetType::Crypto,
                    "CURRENCY" => AssetType::Forex,
                    "EQUITY" | "ETF" => AssetType::Stock,
                    _ => continue,
                };
                let label = item
                    .shortname
                    .or(item.longname)
                    .unwrap_or_else(|| sym.clone())
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ");
                if idx == 1 && !sym.contains('.') && queries.len() <= 10 {
                    let fw = label
                        .split_whitespace()
                        .next()
                        .unwrap_or("")
                        .trim_matches(',');
                    if fw.len() >= 3 && fw.to_uppercase() != term {
                        queries.push((fw.to_string(), "DE&lang=de-DE"));
                        queries.push((fw.to_string(), "GB&lang=en-GB"));
                        queries.push((fw.to_string(), "IT&lang=it-IT"));
                    }
                }
                if seen.insert(sym.clone()) {
                    raw_items.push((sym, label, at));
                }
            }
        }
    }

    let (eu, us): (Vec<_>, Vec<_>) = raw_items.into_iter().partition(|(s, _, _)| s.contains('.'));
    let mut mixed = Vec::new();
    let (mut iu, mut ie) = (us.into_iter(), eu.into_iter());
    while mixed.len() < 16 {
        let a = iu.next();
        let b = ie.next();
        if a.is_none() && b.is_none() {
            break;
        }
        if let Some(x) = a {
            mixed.push(x);
        }
        if let Some(x) = b
            && mixed.len() < 16
        {
            mixed.push(x);
        }
    }
    mixed.retain(|(s, _, at)| {
        !(*at == AssetType::Index
            && s.ends_with("=F")
            && !matches!(s.as_str(), "ES=F" | "NQ=F" | "YM=F" | "NKD=F"))
    });
    let raw_items = mixed;
    let futs = raw_items.into_iter().map(|(sym, name, at)| {
        let c = client.clone();
        async move {
            let mkt = infer_market_tag(&sym, &at);
            let (price, ccy) = match at {
                AssetType::Forex => {
                    let host = ["forex-data-feed", "swissquote", "com"].join(".");
                    let u = format!(
                        "https://{}/public-quotes/bboquotes/instrument/{}/{}",
                        host,
                        &sym[..3],
                        &sym[3..]
                    );
                    let px = if let Ok(r) = c.get(&u).send().await
                        && let Ok(e) = r.json::<Vec<SwissquoteEntry>>().await
                        && let Some(p) = e.first().and_then(|x| x.spread_profile_prices.first())
                        && p.bid > 0.0
                    {
                        Decimal::from_str(&format!("{:.5}", (p.bid + p.ask) / 2.0)).ok()
                    } else {
                        None
                    };
                    (px, sym[3..].to_string())
                }
                AssetType::Index => {
                    let t = map_cfd_index(&sym).unwrap_or(&sym);
                    let q = fetch_yahoo_quote(&c, t).await;
                    (
                        q.as_ref().map(|x| x.0),
                        q.and_then(|x| x.4).unwrap_or_else(|| "USD".into()),
                    )
                }
                _ => {
                    let q = fetch_yahoo_quote(&c, &sym).await;
                    let fallback_ccy = if sym.ends_with(".L") {
                        "GBP"
                    } else if sym.ends_with(".SW") {
                        "CHF"
                    } else if sym.contains('.') {
                        "EUR"
                    } else {
                        "USD"
                    };
                    (
                        q.as_ref().map(|x| x.0),
                        q.and_then(|x| x.4).unwrap_or_else(|| fallback_ccy.into()),
                    )
                }
            };
            SearchItem {
                symbol: sym,
                name: format!("{}|{}", mkt, name),
                asset_type: at,
                currency: ccy,
                price,
            }
        }
    });

    Json(join_all(futs).await)
}

async fn verify_asset_live(
    symbol: &str,
    asset_type: &AssetType,
    buy_price: Decimal,
) -> Result<(Decimal, Option<u64>, Option<String>), String> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(6))
        .build()
        .map_err(|e| e.to_string())?;
    let (live, vol, st) = match asset_type {
        AssetType::Forex => {
            let host = ["forex-data-feed", "swissquote", "com"].join(".");
            let url = format!(
                "https://{}/public-quotes/bboquotes/instrument/{}/{}",
                host,
                &symbol[..3],
                &symbol[3..]
            );
            let entries = client
                .get(&url)
                .send()
                .await
                .map_err(|_| "Forex feed unreachable.")?
                .json::<Vec<SwissquoteEntry>>()
                .await
                .map_err(|_| "Invalid Forex pair.")?;
            let q = entries
                .first()
                .and_then(|e| e.spread_profile_prices.first())
                .ok_or("Forex pair not found on Swissquote.")?;
            if q.bid <= 0.0 || q.ask < q.bid || (q.ask - q.bid) / q.bid > 0.02 {
                return Err("Forex quote failed spread/price check.".into());
            }
            let dec = Decimal::from_str(&format!("{:.5}", (q.bid + q.ask) / 2.0))
                .map_err(|_| "Invalid price.")?;
            (dec, None, Some("LIVE".into()))
        }
        AssetType::Crypto => {
            let base = symbol
                .trim_end_matches("USDT")
                .trim_end_matches("USD")
                .trim_end_matches('-');
            let host = ["api", "coinbase", "com"].join(".");
            let url = format!("https://{}/v2/prices/{}-USD/spot", host, base);
            let parsed = client
                .get(&url)
                .send()
                .await
                .map_err(|_| "Crypto feed unreachable.")?
                .json::<CoinbaseSpotResp>()
                .await
                .map_err(|_| "Crypto symbol not found on Coinbase.")?;
            let amt = parsed.data.ok_or("Crypto pair not supported.")?.amount;
            let dec = Decimal::from_str(&amt).map_err(|_| "Invalid crypto price.")?;
            if dec <= Decimal::ZERO {
                return Err("Crypto price must be positive.".into());
            }
            (dec, None, Some("24/7".into()))
        }
        AssetType::Stock => {
            let (dec, vol, st, itype, _ccy) =
                fetch_yahoo_quote(&client, symbol).await.ok_or_else(|| {
                    format!(
                        "Stock '{}' not found or has no active market price.",
                        symbol
                    )
                })?;
            if !matches!(itype.as_deref(), Some("EQUITY" | "ETF")) {
                return Err(format!(
                    "'{}' is a {}, not a Stock or ETF.",
                    symbol,
                    itype.unwrap_or_else(|| "different asset".into())
                ));
            }
            if st.as_deref() == Some("REGULAR") && matches!(vol, Some(0)) {
                return Err("Stock failed minimum volume check.".into());
            }
            (dec, vol, st)
        }
        AssetType::Index => {
            let ticker = map_cfd_index(symbol).unwrap_or(symbol);
            let (dec, vol, st, itype, _ccy) = fetch_yahoo_quote(&client, ticker)
                .await
                .ok_or_else(|| format!("Index '{}' not found or stale.", symbol))?;
            if !matches!(itype.as_deref(), Some("INDEX" | "FUTURE")) {
                return Err(format!(
                    "'{}' is a {}, not an Index or Future.",
                    symbol,
                    itype.unwrap_or_else(|| "different asset".into())
                ));
            }
            if ticker.ends_with("=F") && matches!(vol, Some(0)) {
                return Err("Index futures failed volume check.".into());
            }
            (dec, vol, st)
        }
    };
    let diff_ratio = if live >= buy_price {
        live - buy_price
    } else {
        buy_price - live
    } / live;
    if diff_ratio > Decimal::from_str("0.80").unwrap() {
        return Err(format!(
            "Avg buy price {} differs by >80% from live price {}. Check symbol or price.",
            buy_price, live
        ));
    }
    Ok((live, vol, st))
}

fn validate_sl_tp_direction(
    is_short: bool,
    ref_px: Decimal,
    sl: Option<Decimal>,
    tp: Option<Decimal>,
) -> Result<(), &'static str> {
    if let Some(s) = sl.filter(|v| *v > Decimal::ZERO)
        && ((!is_short && s >= ref_px) || (is_short && s <= ref_px))
    {
        return Err(if is_short {
            "For SHORT positions, Stop Loss must be above entry price."
        } else {
            "For LONG positions, Stop Loss must be below entry price."
        });
    }
    if let Some(t) = tp.filter(|v| *v > Decimal::ZERO)
        && ((!is_short && t <= ref_px) || (is_short && t >= ref_px))
    {
        return Err(if is_short {
            "For SHORT positions, Take Profit must be below entry price."
        } else {
            "For LONG positions, Take Profit must be above entry price."
        });
    }
    Ok(())
}

async fn add_holding(
    State((state, _)): State<(AppState, u64)>,
    headers: HeaderMap,
    Json(input): Json<CreateHoldingRequest>,
) -> impl IntoResponse {
    let Some(user) = current_user(&state.db, &headers).await else {
        return (StatusCode::UNAUTHORIZED, "Please sign in first.").into_response();
    };
    if let Err(msg) = validate_holding_input(
        &input.symbol,
        input.asset_type.clone(),
        input.quantity,
        input.avg_buy_price,
        &input.currency,
    ) {
        return (StatusCode::BAD_REQUEST, msg).into_response();
    }
    let is_short = input
        .side
        .as_deref()
        .unwrap_or("LONG")
        .eq_ignore_ascii_case("SHORT");
    if let Err(msg) = validate_sl_tp_direction(
        is_short,
        input.avg_buy_price,
        input.stop_loss,
        input.take_profit,
    ) {
        return (StatusCode::BAD_REQUEST, msg).into_response();
    }
    let asset_type_str = match input.asset_type {
        AssetType::Stock => "Stock",
        AssetType::Forex => "Forex",
        AssetType::Crypto => "Crypto",
        AssetType::Index => "Index",
    };

    let mut symbol = input.symbol.trim().to_uppercase();
    let mut currency = input.currency.trim().to_uppercase();
    if input.asset_type == AssetType::Forex {
        symbol = symbol.replace(['/', '-'], "");
        if symbol.len() == 6 {
            currency = symbol[3..].to_string();
        }
    }
    let (live_px, live_vol, live_st) =
        match verify_asset_live(&symbol, &input.asset_type, input.avg_buy_price).await {
            Ok(v) => v,
            Err(msg) => return (StatusCode::BAD_REQUEST, msg).into_response(),
        };

    match sqlx::query(
        "INSERT INTO holdings (user_id, symbol, asset_type, quantity, avg_buy_price, currency, side, status, trigger_dir, stop_loss, take_profit) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(user.id).bind(&symbol)
    .bind(asset_type_str)
    .bind(input.quantity.to_string())
    .bind(input.avg_buy_price.to_string())
    .bind(&currency)
    .bind(if input.side.as_deref().unwrap_or("LONG").eq_ignore_ascii_case("SHORT") { "SHORT" } else { "LONG" })
    .bind(if !live_px.is_zero() && ((if input.avg_buy_price >= live_px { input.avg_buy_price - live_px } else { live_px - input.avg_buy_price }) / live_px) > Decimal::new(5, 5) { "PENDING" } else { "OPEN" })
    .bind(if input.avg_buy_price >= live_px { "ABOVE" } else { "BELOW" })
    .bind(input.stop_loss.filter(|v| *v > Decimal::ZERO).map(|v| v.to_string()))
    .bind(input.take_profit.filter(|v| *v > Decimal::ZERO).map(|v| v.to_string()))
    .execute(&state.db)
    .await
    {
        Ok(_) => {
            update_price_meta(&state, &symbol, live_px, live_vol, live_st).await;
            StatusCode::CREATED.into_response()
        }
        Err(err) => {
            eprintln!("Failed to insert holding: {err}");
            (StatusCode::INTERNAL_SERVER_ERROR, "DB error").into_response()
        }
    }
}

#[derive(Deserialize)]
struct UpdateHoldingInput {
    quantity: Decimal,
    avg_buy_price: Decimal,
    #[serde(default)]
    stop_loss: Option<Decimal>,
    #[serde(default)]
    take_profit: Option<Decimal>,
}

async fn update_holding(
    State((state, _)): State<(AppState, u64)>,
    headers: HeaderMap,
    Path(id): Path<i64>,
    Json(input): Json<UpdateHoldingInput>,
) -> impl IntoResponse {
    let Some(user) = current_user(&state.db, &headers).await else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    if input.quantity <= Decimal::ZERO || input.avg_buy_price <= Decimal::ZERO {
        return (
            StatusCode::BAD_REQUEST,
            "Quantity and price must be positive.",
        )
            .into_response();
    }
    let Some((st_str, side_str, sym_str)) = sqlx::query_as::<_, (String, String, String)>(
        "SELECT status, side, symbol FROM holdings WHERE id = ? AND user_id = ?",
    )
    .bind(id)
    .bind(user.id)
    .fetch_optional(&state.db)
    .await
    .ok()
    .flatten() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let is_open = st_str.eq_ignore_ascii_case("OPEN");
    let ref_px = if is_open {
        state
            .prices
            .read()
            .await
            .get(&sym_str)
            .map(|t| t.price)
            .unwrap_or(input.avg_buy_price)
    } else {
        input.avg_buy_price
    };
    let is_short = side_str.eq_ignore_ascii_case("SHORT");
    if let Some(sl) = input.stop_loss.filter(|v| *v > Decimal::ZERO)
        && ((!is_short && sl >= ref_px) || (is_short && sl <= ref_px))
    {
        return (
            StatusCode::BAD_REQUEST,
            if is_short {
                "For SHORT, Stop Loss must be above price."
            } else {
                "For LONG, Stop Loss must be below price."
            },
        )
            .into_response();
    }
    if let Some(tp) = input.take_profit.filter(|v| *v > Decimal::ZERO)
        && ((!is_short && tp <= ref_px) || (is_short && tp >= ref_px))
    {
        return (
            StatusCode::BAD_REQUEST,
            if is_short {
                "For SHORT, Take Profit must be below price."
            } else {
                "For LONG, Take Profit must be above price."
            },
        )
            .into_response();
    }
    let sl_bind = input
        .stop_loss
        .filter(|v| *v > Decimal::ZERO)
        .map(|v| v.to_string());
    let tp_bind = input
        .take_profit
        .filter(|v| *v > Decimal::ZERO)
        .map(|v| v.to_string());
    let exec_res = if is_open {
        sqlx::query(
            "UPDATE holdings SET stop_loss = ?, take_profit = ? WHERE id = ? AND user_id = ?",
        )
        .bind(&sl_bind)
        .bind(&tp_bind)
        .bind(id)
        .bind(user.id)
        .execute(&state.db)
        .await
    } else {
        sqlx::query("UPDATE holdings SET quantity = ?, avg_buy_price = ?, stop_loss = ?, take_profit = ? WHERE id = ? AND user_id = ?")
            .bind(input.quantity.to_string())
            .bind(input.avg_buy_price.to_string())
            .bind(&sl_bind)
            .bind(&tp_bind)
            .bind(id)
            .bind(user.id)
            .execute(&state.db)
            .await
    };
    match exec_res {
        Ok(r) if r.rows_affected() > 0 => StatusCode::OK.into_response(),
        Ok(_) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn delete_holding(
    State((state, _)): State<(AppState, u64)>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> impl IntoResponse {
    let Some(user) = current_user(&state.db, &headers).await else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    match sqlx::query("DELETE FROM holdings WHERE id = ? AND user_id = ?")
        .bind(id)
        .bind(user.id)
        .execute(&state.db)
        .await
    {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(err) => {
            eprintln!("Failed to delete holding {id}: {err}");
            (StatusCode::INTERNAL_SERVER_ERROR, "DB error").into_response()
        }
    }
}

#[derive(Deserialize)]
struct CloseInput {
    #[serde(default)]
    exit_price: Option<Decimal>,
}
async fn close_holding(
    State((state, _)): State<(AppState, u64)>,
    headers: HeaderMap,
    Path(id): Path<i64>,
    Json(inp): Json<CloseInput>,
) -> impl IntoResponse {
    let Some(user) = current_user(&state.db, &headers).await else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let Some(row): Option<HoldingRow> = sqlx::query_as("SELECT id, user_id, symbol, asset_type, quantity, avg_buy_price, currency, side, status, trigger_dir, stop_loss, take_profit FROM holdings WHERE id = ? AND user_id = ?").bind(id).bind(user.id).fetch_optional(&state.db).await.unwrap_or(None) else { return StatusCode::NOT_FOUND.into_response(); };
    if row.status.eq_ignore_ascii_case("PENDING") {
        let _ = sqlx::query("DELETE FROM holdings WHERE id = ? AND user_id = ?")
            .bind(id)
            .bind(user.id)
            .execute(&state.db)
            .await;
        return StatusCode::OK.into_response();
    }
    let live = state.prices.read().await.get(&row.symbol).map(|t| t.price);
    let Some(px) = inp.exit_price.filter(|p| *p > Decimal::ZERO).or(live) else {
        return (StatusCode::BAD_REQUEST, "No live price").into_response();
    };
    match archive_and_close_holding(&state.db, &row, px, "MANUAL").await {
        Ok(_) => StatusCode::OK.into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}
#[derive(Serialize)]
struct ClosedTradeResponse {
    #[serde(flatten)]
    trade: ClosedTradeRow,
    display_currency: String,
    converted_pnl: String,
}
async fn list_trades(
    State((state, _)): State<(AppState, u64)>,
    headers: HeaderMap,
    Query(q): Query<PortfolioQuery>,
) -> impl IntoResponse {
    let Some(user) = current_user(&state.db, &headers).await else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let rows: Vec<ClosedTradeRow> = sqlx::query_as("SELECT id, symbol, asset_type, side, currency, quantity, entry_price, exit_price, realized_pnl, close_reason, closed_at FROM closed_trades WHERE user_id = ? ORDER BY closed_at DESC, id DESC").bind(user.id).fetch_all(&state.db).await.unwrap_or_default();
    let prices = state.prices.read().await;
    let eur_usd = prices
        .get("EURUSD")
        .map(|t| t.price)
        .filter(|p| !p.is_zero())
        .unwrap_or_else(|| Decimal::new(108, 2));
    let target = match q.base.as_deref().unwrap_or("USD").to_uppercase().as_str() {
        "EUR" => "EUR",
        _ => "USD",
    };
    let out: Vec<ClosedTradeResponse> = rows
        .into_iter()
        .map(|trade| {
            let rp = Decimal::from_str(&trade.realized_pnl).unwrap_or(Decimal::ZERO);
            let to_usd = if trade.currency == "USD" {
                Decimal::ONE
            } else if let Some(x) = prices.get(&format!("{}USD", trade.currency)) {
                x.price
            } else if let Some(x) = prices.get(&format!("USD{}", trade.currency))
                && !x.price.is_zero()
            {
                Decimal::ONE / x.price
            } else {
                Decimal::ONE
            };
            let usd_pnl = rp * to_usd;
            let conv = if target == "EUR" {
                usd_pnl / eur_usd
            } else {
                usd_pnl
            };
            ClosedTradeResponse {
                trade,
                display_currency: target.to_string(),
                converted_pnl: conv.round_dp(2).to_string(),
            }
        })
        .collect();
    Json(out).into_response()
}
async fn delete_trade(
    State((state, _)): State<(AppState, u64)>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> impl IntoResponse {
    let Some(user) = current_user(&state.db, &headers).await else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let _ = sqlx::query("DELETE FROM closed_trades WHERE id = ? AND user_id = ?")
        .bind(id)
        .bind(user.id)
        .execute(&state.db)
        .await;
    StatusCode::NO_CONTENT.into_response()
}

#[derive(Deserialize)]
struct NewsQuery {
    symbol: Option<String>,
}

#[derive(Serialize)]
struct NewsItem {
    title: String,
    publisher: String,
    link: String,
    pub_date: String,
    #[serde(skip_serializing)]
    ts: i64,
}

fn parse_rfc2822_ts(d: &str) -> i64 {
    let parts: Vec<&str> = d.split_whitespace().collect();
    if parts.len() < 5 {
        return 0;
    }
    let day: i64 = parts[1].parse().unwrap_or(0);
    let mon: i64 = match parts[2] {
        "Jan" => 1,
        "Feb" => 2,
        "Mar" => 3,
        "Apr" => 4,
        "May" => 5,
        "Jun" => 6,
        "Jul" => 7,
        "Aug" => 8,
        "Sep" => 9,
        "Oct" => 10,
        "Nov" => 11,
        "Dec" => 12,
        _ => return 0,
    };
    let year: i64 = parts[3].parse().unwrap_or(0);
    let hms: Vec<i64> = parts[4].split(':').filter_map(|x| x.parse().ok()).collect();
    if year < 1970 || day <= 0 || hms.len() < 3 {
        return 0;
    }
    let (y, m) = if mon <= 2 {
        (year - 1, mon + 9)
    } else {
        (year, mon - 3)
    };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * m + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719468;
    days * 86400 + hms[0] * 3600 + hms[1] * 60 + hms[2]
}

fn extract_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{}", tag);
    let close = format!("</{}>", tag);
    let s = xml.find(&open)?;
    let after_open = xml[s..].find('>')? + s + 1;
    let e = xml[after_open..].find(&close)? + after_open;
    let raw = xml[after_open..e].trim();
    let cleaned = raw
        .strip_prefix("<![CDATA[")
        .and_then(|x| x.strip_suffix("]]>"))
        .unwrap_or(raw)
        .replace("&amp;", "&")
        .replace("&quot;", "\"")
        .replace("&apos;", "'");
    Some(cleaned.trim().to_string())
}

async fn get_news(Query(nq): Query<NewsQuery>) -> Json<Vec<NewsItem>> {
    let sym = nq.symbol.unwrap_or_default().trim().to_uppercase();
    let is_fx = sym.len() == 6 && sym.chars().all(|c| c.is_ascii_alphabetic());
    let rss_sym = if sym.is_empty() {
        "^GSPC,EURUSD=X,GC=F,BTC-USD".to_string()
    } else if is_fx {
        format!("{}=X", sym)
    } else {
        sym.clone()
    };
    let client = reqwest::Client::builder()
        .user_agent("Mozilla/5.0")
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let now_ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let min_ts = now_ts - 14 * 86400;
    let mut items = Vec::new();

    let y_base = ["https://feeds", "finance", "yahoo", "com/rss/2.0/headline"].join(".");
    if let Ok(rss_url) = reqwest::Url::parse_with_params(
        &y_base,
        &[("s", rss_sym.as_str()), ("region", "US"), ("lang", "en-US")],
    ) && let Ok(resp) = client.get(rss_url).send().await
        && let Ok(xml) = resp.text().await
    {
        for chunk in xml.split("<item>").skip(1).take(24) {
            if let (Some(title), Some(link)) =
                (extract_tag(chunk, "title"), extract_tag(chunk, "link"))
                && !title.is_empty()
                && !link.is_empty()
            {
                let pub_date = extract_tag(chunk, "pubDate").unwrap_or_default();
                let ts = parse_rfc2822_ts(&pub_date);
                if ts == 0 || ts >= min_ts {
                    items.push(NewsItem {
                        title,
                        publisher: "Yahoo Finance".to_string(),
                        link,
                        pub_date,
                        ts,
                    });
                }
            }
        }
    }

    if items.len() < 12 {
        let gq = if sym.is_empty() {
            "Federal Reserve ECB inflation forex stock market when:3d".to_string()
        } else if is_fx {
            format!("{}/{} OR {} forex when:3d", &sym[..3], &sym[3..], sym)
        } else {
            format!(
                "{} stock OR market news when:3d",
                sym.split('.').next().unwrap_or(&sym)
            )
        };
        let g_base = ["https://news", "google", "com/rss/search"].join(".");
        if let Ok(g_url) = reqwest::Url::parse_with_params(
            &g_base,
            &[
                ("q", gq.as_str()),
                ("hl", "en-US"),
                ("gl", "US"),
                ("ceid", "US:en"),
            ],
        ) && let Ok(resp) = client.get(g_url).send().await
            && let Ok(xml) = resp.text().await
        {
            for chunk in xml.split("<item>").skip(1).take(24) {
                if let (Some(raw_title), Some(link)) =
                    (extract_tag(chunk, "title"), extract_tag(chunk, "link"))
                    && !raw_title.is_empty()
                    && !link.is_empty()
                {
                    let (title, publisher) = match raw_title.rsplit_once(" - ") {
                        Some((t, p)) => (t.trim().to_string(), p.trim().to_string()),
                        None => (raw_title, "Market News".to_string()),
                    };
                    let pub_date = extract_tag(chunk, "pubDate").unwrap_or_default();
                    let ts = parse_rfc2822_ts(&pub_date);
                    if ts == 0 || ts >= min_ts {
                        let key: String = title.to_lowercase().chars().take(48).collect();
                        if !items
                            .iter()
                            .any(|x| x.title.to_lowercase().starts_with(&key))
                        {
                            items.push(NewsItem {
                                title,
                                publisher,
                                link,
                                pub_date,
                                ts,
                            });
                        }
                    }
                }
            }
        }
    }

    items.sort_by_key(|a| std::cmp::Reverse(a.ts));
    items.truncate(24);
    Json(items)
}
#[derive(Deserialize)]
struct BulkImportRequest {
    items: Vec<CreateHoldingRequest>,
    #[serde(default = "default_true")]
    skip_duplicates: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Serialize)]
struct BulkImportResponse {
    imported: usize,
    skipped_duplicates: usize,
    errors: Vec<String>,
}

async fn import_holdings(
    State((state, _)): State<(AppState, u64)>,
    headers: HeaderMap,
    Json(payload): Json<BulkImportRequest>,
) -> impl IntoResponse {
    let Some(user) = current_user(&state.db, &headers).await else {
        return (StatusCode::UNAUTHORIZED, "Please sign in first.").into_response();
    };
    if user.provider == "demo" {
        return (
            StatusCode::FORBIDDEN,
            "Broker CSV import is disabled in Demo Mode. Create a free real account to import your trades.",
        )
            .into_response();
    }
    if payload.items.is_empty() {
        return (StatusCode::BAD_REQUEST, "No rows provided for import.").into_response();
    }
    if payload.items.len() > 250 {
        return (
            StatusCode::BAD_REQUEST,
            "Maximum 250 rows per import batch.",
        )
            .into_response();
    }

    let existing: Vec<(String, String, String, String)> = sqlx::query_as(
        "SELECT symbol, side, quantity, avg_buy_price FROM holdings WHERE user_id = ?",
    )
    .bind(user.id)
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

    let mut imported = 0usize;
    let mut skipped_duplicates = 0usize;
    let mut errors = Vec::new();

    for (idx, input) in payload.items.into_iter().enumerate() {
        let row_num = idx + 1;
        if let Err(msg) = validate_holding_input(
            &input.symbol,
            input.asset_type.clone(),
            input.quantity,
            input.avg_buy_price,
            &input.currency,
        ) {
            errors.push(format!(
                "Row {} ({}): {}",
                row_num,
                input.symbol.trim(),
                msg
            ));
            continue;
        }
        let asset_type_str = match input.asset_type {
            AssetType::Stock => "Stock",
            AssetType::Forex => "Forex",
            AssetType::Crypto => "Crypto",
            AssetType::Index => "Index",
        };
        let mut symbol = input.symbol.trim().to_uppercase();
        let mut currency = input.currency.trim().to_uppercase();
        if input.asset_type == AssetType::Forex {
            symbol = symbol.replace(['/', '-'], "");
            if symbol.len() == 6 {
                currency = symbol[3..].to_string();
            }
        }
        let side = if input
            .side
            .as_deref()
            .unwrap_or("LONG")
            .eq_ignore_ascii_case("SHORT")
        {
            "SHORT"
        } else {
            "LONG"
        };
        if let Err(msg) = validate_sl_tp_direction(
            side == "SHORT",
            input.avg_buy_price,
            input.stop_loss,
            input.take_profit,
        ) {
            errors.push(format!("Row {} ({}): {}", row_num, symbol, msg));
            continue;
        }
        let qty_str = input.quantity.normalize().to_string();
        let px_str = input.avg_buy_price.normalize().to_string();

        if payload.skip_duplicates
            && existing.iter().any(|(es, eside, eq, epx)| {
                es == &symbol
                    && eside == side
                    && Decimal::from_str(eq).ok() == Some(input.quantity)
                    && Decimal::from_str(epx).ok() == Some(input.avg_buy_price)
            })
        {
            skipped_duplicates += 1;
            continue;
        }

        match sqlx::query(
            "INSERT INTO holdings (user_id, symbol, asset_type, quantity, avg_buy_price, currency, side, status, trigger_dir, stop_loss, take_profit) VALUES (?, ?, ?, ?, ?, ?, ?, 'OPEN', 'BELOW', ?, ?)",
        )
        .bind(user.id)
        .bind(&symbol)
        .bind(asset_type_str)
        .bind(&qty_str)
        .bind(&px_str)
        .bind(&currency)
        .bind(side)
        .bind(input.stop_loss.filter(|v| *v > Decimal::ZERO).map(|v| v.to_string()))
        .bind(input.take_profit.filter(|v| *v > Decimal::ZERO).map(|v| v.to_string()))
        .execute(&state.db)
        .await
        {
            Ok(_) => {
                imported += 1;
                if let Ok((live_px, live_vol, live_st)) =
                    verify_asset_live(&symbol, &input.asset_type, input.avg_buy_price).await
                {
                    update_price_meta(&state, &symbol, live_px, live_vol, live_st).await;
                }
            }
            Err(err) => errors.push(format!("Row {} ({}): DB error {}", row_num, symbol, err)),
        }
    }

    (
        StatusCode::OK,
        Json(BulkImportResponse {
            imported,
            skipped_duplicates,
            errors,
        }),
    )
        .into_response()
}

#[derive(Deserialize)]
struct EquityQuery {
    range: Option<String>,
    bench: Option<String>,
}

#[derive(Serialize)]
struct EquityPoint {
    ts: i64,
    port_pct: f64,
    bench_pct: f64,
}

#[derive(Serialize)]
struct EquityResponse {
    benchmark: String,
    points: Vec<EquityPoint>,
}

async fn get_equity_curve(
    State((state, _)): State<(AppState, u64)>,
    headers: HeaderMap,
    Query(eq): Query<EquityQuery>,
) -> impl IntoResponse {
    let Some(user) = current_user(&state.db, &headers).await else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let rng = eq.range.unwrap_or_else(|| "1mo".into());
    let bench = eq
        .bench
        .unwrap_or_else(|| "^GSPC".into())
        .trim()
        .to_uppercase();
    let bench_type = if bench.contains("BTC") || bench.contains("ETH") {
        "Crypto"
    } else {
        "Index"
    };

    let rows: Vec<HoldingRow> = sqlx::query_as(
        "SELECT id, user_id, symbol, asset_type, quantity, avg_buy_price, currency, side, status, trigger_dir, stop_loss, take_profit FROM holdings WHERE user_id = ? AND status = 'OPEN'",
    )
    .bind(user.id)
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

    let bench_hist = get_history(Query(HistoryQuery {
        symbol: bench.clone(),
        asset_type: Some(bench_type.into()),
        range: Some(rng.clone()),
    }))
    .await
    .0;

    if bench_hist.points.len() < 2 {
        return Json(EquityResponse {
            benchmark: bench,
            points: vec![],
        })
        .into_response();
    }

    let mut futs = Vec::with_capacity(rows.len());
    for r in &rows {
        let sym = r.symbol.clone();
        let at = r.asset_type.clone();
        let rg = rng.clone();
        futs.push(async move {
            let h = get_history(Query(HistoryQuery {
                symbol: sym,
                asset_type: Some(at),
                range: Some(rg),
            }))
            .await
            .0;
            h.points
        });
    }
    let all_pts = futures_util::future::join_all(futs).await;

    let b0 = bench_hist.points.first().map(|p| p.close).unwrap_or(1.0);
    let mut out = Vec::with_capacity(bench_hist.points.len());
    let mut base_port_val: Option<f64> = None;

    for bp in &bench_hist.points {
        let mut tot_val = 0.0_f64;
        let mut valid_cnt = 0usize;
        for (idx, r) in rows.iter().enumerate() {
            let pts = &all_pts[idx];
            if pts.is_empty() {
                continue;
            }
            let c_px = pts
                .iter()
                .rev()
                .find(|p| p.ts <= bp.ts)
                .or_else(|| pts.first())
                .map(|p| p.close)
                .unwrap_or(0.0);
            if c_px <= 0.0 {
                continue;
            }
            let qty = r.quantity.parse::<f64>().unwrap_or(0.0);
            let entry = r.avg_buy_price.parse::<f64>().unwrap_or(c_px);
            let mult = if r.asset_type == "Forex" {
                100_000.0
            } else {
                1.0
            };
            let is_short = r.side.eq_ignore_ascii_case("SHORT");
            let pos_val = if is_short {
                (entry + (entry - c_px)).max(0.0) * qty * mult
            } else {
                c_px * qty * mult
            };
            tot_val += pos_val;
            valid_cnt += 1;
        }
        if valid_cnt == 0 {
            continue;
        }
        let v0 = *base_port_val.get_or_insert(tot_val.max(1.0));
        let port_pct = ((tot_val - v0) / v0) * 100.0;
        let bench_pct = if b0 > 0.0 {
            ((bp.close - b0) / b0) * 100.0
        } else {
            0.0
        };
        out.push(EquityPoint {
            ts: bp.ts,
            port_pct: (port_pct * 100.0).round() / 100.0,
            bench_pct: (bench_pct * 100.0).round() / 100.0,
        });
    }

    Json(EquityResponse {
        benchmark: bench,
        points: out,
    })
    .into_response()
}

fn build_router(state: AppState, interval_secs: u64) -> Router {
    let portfolio_routes = Router::new()
        .route("/", get(index))
        .route("/health", get(health))
        .route("/api/portfolio", get(get_portfolio))
        .route("/api/search", get(search_symbols))
        .route("/api/history", get(get_history))
        .route("/api/equity", get(get_equity_curve))
        .route("/api/news", get(get_news))
        .route("/api/auth/me", get(auth_me))
        .route("/api/auth/register", post(auth_register))
        .route("/api/auth/login", post(auth_login))
        .route("/api/auth/demo", post(auth_demo))
        .route("/api/auth/demo/reset", post(auth_demo_reset))
        .route("/api/auth/logout", post(auth_logout))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            sliding_session_cookie,
        ))
        .route("/api/auth/google", get(auth_google_start))
        .route("/api/auth/google/callback", get(auth_google_callback))
        .route("/api/holdings", post(add_holding))
        .route("/api/import/holdings", post(import_holdings))
        .route("/api/holdings/{id}/close", post(close_holding))
        .route("/api/trades", get(list_trades))
        .route("/api/trades/{id}", delete(delete_trade))
        .route(
            "/api/holdings/{id}",
            delete(delete_holding).put(update_holding),
        )
        .route("/ws/prices", get(ws_prices_handler))
        .with_state((state, interval_secs));

    Router::new()
        .route("/", get(|| async { Redirect::permanent("/portfolio") }))
        .nest("/portfolio", portfolio_routes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;

    #[tokio::test]
    async fn test_health_endpoint() {
        let db = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
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
        let db = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        init_db(&db).await.unwrap();

        let mut initial_prices = HashMap::new();
        initial_prices.insert(
            "EURUSD".to_string(),
            PriceTick {
                symbol: "EURUSD".to_string(),
                price: Decimal::from_str("1.1000").unwrap(),
                timestamp: 1_700_000_000,
                volume: None,
                market_state: Some("LIVE".to_string()),
            },
        );

        let (price_tx, _) = broadcast::channel(16);
        let state = AppState {
            db,
            prices: Arc::new(RwLock::new(initial_prices)),
            price_tx,
        };
        let app = build_router(state, 15);

        let reg_req = Request::builder()
            .method("POST")
            .uri("/portfolio/api/auth/register")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"email":"ci-trader@example.com","password":"Password123"}"#,
            ))
            .unwrap();
        let reg_res = app.clone().oneshot(reg_req).await.unwrap();
        assert_eq!(reg_res.status(), StatusCode::OK);
        let cookie = reg_res
            .headers()
            .get("set-cookie")
            .unwrap()
            .to_str()
            .unwrap()
            .split(";")
            .next()
            .unwrap()
            .to_string();

        let create_req = Request::builder()
            .method("POST")
            .uri("/portfolio/api/holdings")
            .header("content-type", "application/json")
            .header("cookie", &cookie)
            .body(Body::from(r#"{"symbol":"EURUSD","asset_type":"Forex","side":"LONG","quantity":"0.1","avg_buy_price":"1.1000","currency":"USD"}"#))
            .unwrap();
        let res = app.clone().oneshot(create_req).await.unwrap();
        assert_eq!(res.status(), StatusCode::CREATED);

        // Verify weak password without uppercase is rejected
        let weak_req = Request::builder()
            .method("POST")
            .uri("/portfolio/api/auth/register")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"email":"weak@example.com","password":"password123"}"#,
            ))
            .unwrap();
        let weak_res = app.clone().oneshot(weak_req).await.unwrap();
        assert_eq!(weak_res.status(), StatusCode::BAD_REQUEST);

        // Verify bulk multi-asset CSV import + duplicate skipping on real account
        let imp_req = Request::builder()
            .method("POST")
            .uri("/portfolio/api/import/holdings")
            .header("content-type", "application/json")
            .header("cookie", &cookie)
            .body(Body::from(
                r#"{"skip_duplicates":true,"items":[{"symbol":"AAPL","asset_type":"Stock","side":"LONG","quantity":"5","avg_buy_price":"228.28","currency":"USD"},{"symbol":"BTC-USD","asset_type":"Crypto","side":"LONG","quantity":"0.015","avg_buy_price":"61200","currency":"USD","stop_loss":"55000","take_profit":"72000"}]}"#,
            ))
            .unwrap();
        let imp_res = app.clone().oneshot(imp_req).await.unwrap();
        assert_eq!(imp_res.status(), StatusCode::OK);

        let dup_req = Request::builder()
            .method("POST")
            .uri("/portfolio/api/import/holdings")
            .header("content-type", "application/json")
            .header("cookie", &cookie)
            .body(Body::from(
                r#"{"skip_duplicates":true,"items":[{"symbol":"AAPL","asset_type":"Stock","side":"LONG","quantity":"5","avg_buy_price":"228.28","currency":"USD"}]}"#,
            ))
            .unwrap();
        let dup_res = app.clone().oneshot(dup_req).await.unwrap();
        assert_eq!(dup_res.status(), StatusCode::OK);

        // Verify 1-Click Demo Sandbox creation & CSV import restriction in Demo Mode
        let demo_req = Request::builder()
            .method("POST")
            .uri("/portfolio/api/auth/demo")
            .body(Body::empty())
            .unwrap();
        let demo_res = app.clone().oneshot(demo_req).await.unwrap();
        assert_eq!(demo_res.status(), StatusCode::OK);
        let demo_cookie = demo_res
            .headers()
            .get("set-cookie")
            .unwrap()
            .to_str()
            .unwrap()
            .split(";")
            .next()
            .unwrap()
            .to_string();

        let demo_imp_req = Request::builder()
            .method("POST")
            .uri("/portfolio/api/import/holdings")
            .header("content-type", "application/json")
            .header("cookie", &demo_cookie)
            .body(Body::from(
                r#"{"skip_duplicates":true,"items":[{"symbol":"AAPL","asset_type":"Stock","side":"LONG","quantity":"1","avg_buy_price":"200","currency":"USD"}]}"#,
            ))
            .unwrap();
        let demo_imp_res = app.clone().oneshot(demo_imp_req).await.unwrap();
        assert_eq!(demo_imp_res.status(), StatusCode::FORBIDDEN);

        let list_req = Request::builder()
            .uri("/portfolio/api/portfolio")
            .header("cookie", &cookie)
            .body(Body::empty())
            .unwrap();
        let res = app.oneshot(list_req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }
}
