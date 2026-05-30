//! Prediction markets (Kalshi + Polymarket) — port of api/queries/prediction_markets.py.

use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;
use serde_json::{Map, Value};
use tokio_postgres::types::ToSql;

use findata::db::rows::{row_to_object, rows_to_objects};
use findata::error::ApiResult;

type P = Vec<Box<dyn ToSql + Sync + Send>>;
fn refs(p: &P) -> Vec<&(dyn ToSql + Sync)> {
    p.iter().map(|b| b.as_ref() as &(dyn ToSql + Sync)).collect()
}

// ---------- markets ----------
pub async fn search_markets(
    pool: &Pool,
    q: &str,
    venue: Option<&str>,
    status: &str,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let pat = format!("%{q}%");
    let mut params: P = Vec::new();
    let mut parts: Vec<String> = Vec::new();
    if venue != Some("kalshi") {
        params.push(Box::new(pat.clone()));
        let mut w = vec![format!("question ILIKE ${}", params.len())];
        match status {
            "open" => w.push("active AND NOT closed".into()),
            "closed" => w.push("closed".into()),
            _ => {}
        }
        parts.push(format!(
            "SELECT 'polymarket' AS venue, condition_id AS market_id, question AS title, slug, \
                    volume_num AS volume, start_date, end_date, closed \
               FROM prediction_markets.polymarket_markets WHERE {}",
            w.join(" AND ")
        ));
    }
    if venue != Some("polymarket") {
        params.push(Box::new(pat));
        let mut w = vec![format!("title ILIKE ${}", params.len())];
        match status {
            "open" => w.push("status NOT IN ('finalized','settled','closed')".into()),
            "closed" => w.push("status IN ('finalized','settled','closed')".into()),
            _ => {}
        }
        parts.push(format!(
            "SELECT 'kalshi' AS venue, ticker AS market_id, title, NULL::text AS slug, \
                    volume::float8 AS volume, open_time AS start_date, close_time AS end_date, \
                    (status IN ('finalized','settled','closed')) AS closed \
               FROM prediction_markets.kalshi_markets WHERE {}",
            w.join(" AND ")
        ));
    }
    params.push(Box::new(limit.clamp(1, 200)));
    let lim = params.len();
    let sql = format!(
        "SELECT * FROM ({}) x ORDER BY closed ASC, volume DESC NULLS LAST LIMIT ${lim}",
        parts.join(" UNION ALL ")
    );
    let client = pool.get().await?;
    let rows = client.query(&sql, &refs(&params)).await?;
    Ok(rows_to_objects(&rows))
}

pub async fn get_polymarket_market(pool: &Pool, condition_id: &str) -> ApiResult<Option<Map<String, Value>>> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            "SELECT condition_id, id AS market_id, question, slug, outcomes, outcome_prices, \
                    clob_token_ids, volume_num AS volume, liquidity_num AS liquidity, \
                    start_date, end_date, closed_time, active, closed, archived, enable_order_book \
               FROM prediction_markets.polymarket_markets WHERE condition_id = $1 LIMIT 1",
            &[&condition_id],
        )
        .await?;
    Ok(row.map(|r| row_to_object(&r)))
}

pub async fn get_kalshi_market(pool: &Pool, ticker: &str) -> ApiResult<Option<Map<String, Value>>> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            "SELECT ticker, event_ticker, title, market_type, status, result, yes_bid, yes_ask, \
                    no_bid, no_ask, last_price, volume, volume_24h, open_interest, open_time, \
                    close_time, created_time FROM prediction_markets.kalshi_markets WHERE ticker = $1 LIMIT 1",
            &[&ticker.to_uppercase()],
        )
        .await?;
    Ok(row.map(|r| row_to_object(&r)))
}

// ---------- trades / orderbook ----------
async fn time_bounded(
    pool: &Pool,
    select_from: &str,
    id_col: &str,
    id_val: String,
    ts_col: &str,
    since: Option<DateTime<Utc>>,
    until: Option<DateTime<Utc>>,
    order_col: &str,
    limit: i64,
    cap: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let mut params: P = vec![Box::new(id_val)];
    let mut where_ = vec![format!("{id_col} = $1")];
    if let Some(s) = since {
        params.push(Box::new(s));
        where_.push(format!("{ts_col} >= ${}", params.len()));
    }
    if let Some(u) = until {
        params.push(Box::new(u));
        where_.push(format!("{ts_col} <= ${}", params.len()));
    }
    params.push(Box::new(limit.clamp(1, cap)));
    let lim = params.len();
    let sql = format!(
        "{select_from} WHERE {} ORDER BY {order_col} DESC LIMIT ${lim}",
        where_.join(" AND ")
    );
    let client = pool.get().await?;
    let rows = client.query(&sql, &refs(&params)).await?;
    Ok(rows_to_objects(&rows))
}

pub async fn polymarket_trades(pool: &Pool, condition_id: &str, since: Option<DateTime<Utc>>, until: Option<DateTime<Utc>>, limit: i64) -> ApiResult<Vec<Map<String, Value>>> {
    time_bounded(pool,
        "SELECT trade_id, asset_id AS token_id, side, price, size, taker, ts FROM prediction_markets.polymarket_trades",
        "condition_id", condition_id.to_string(), "ts", since, until, "ts", limit, 5000).await
}

pub async fn kalshi_trades(pool: &Pool, ticker: &str, since: Option<DateTime<Utc>>, until: Option<DateTime<Utc>>, limit: i64) -> ApiResult<Vec<Map<String, Value>>> {
    time_bounded(pool,
        "SELECT trade_id, ticker, count, yes_price, no_price, taker_side, created_time FROM prediction_markets.kalshi_trades",
        "ticker", ticker.to_uppercase(), "created_time", since, until, "created_time", limit, 5000).await
}

pub async fn polymarket_orderbook(pool: &Pool, asset_id: &str, since: Option<DateTime<Utc>>, until: Option<DateTime<Utc>>, limit: i64) -> ApiResult<Vec<Map<String, Value>>> {
    time_bounded(pool,
        "SELECT asset_id, condition_id, snapshot_ts, bids, asks, tick_size, min_order_size FROM prediction_markets.polymarket_orderbook_snapshots",
        "asset_id", asset_id.to_string(), "snapshot_ts", since, until, "snapshot_ts", limit, 500).await
}

pub async fn kalshi_orderbook(pool: &Pool, ticker: &str, since: Option<DateTime<Utc>>, until: Option<DateTime<Utc>>, limit: i64) -> ApiResult<Vec<Map<String, Value>>> {
    time_bounded(pool,
        "SELECT ticker, snapshot_ts, yes_levels, no_levels FROM prediction_markets.kalshi_orderbook_snapshots",
        "ticker", ticker.to_uppercase(), "snapshot_ts", since, until, "snapshot_ts", limit, 500).await
}

// ---------- candles ----------
fn interval_view(venue: &str, interval_min: i64) -> Option<(&'static str, &'static str)> {
    match (venue, interval_min) {
        ("polymarket", 1) => Some(("prediction_markets.polymarket_candles_1m", "condition_id")),
        ("polymarket", 5) => Some(("prediction_markets.polymarket_candles_5m", "condition_id")),
        ("polymarket", 15) => Some(("prediction_markets.polymarket_candles_15m", "condition_id")),
        ("polymarket", 60) => Some(("prediction_markets.polymarket_candles_1h", "condition_id")),
        ("polymarket", 1440) => Some(("prediction_markets.polymarket_candles_1d", "condition_id")),
        ("kalshi", 1) => Some(("prediction_markets.kalshi_candles_1m", "ticker")),
        ("kalshi", 5) => Some(("prediction_markets.kalshi_candles_5m", "ticker")),
        ("kalshi", 15) => Some(("prediction_markets.kalshi_candles_15m", "ticker")),
        ("kalshi", 60) => Some(("prediction_markets.kalshi_candles_1h", "ticker")),
        ("kalshi", 1440) => Some(("prediction_markets.kalshi_candles_1d", "ticker")),
        _ => None,
    }
}

pub async fn candles(pool: &Pool, venue: &str, market_id: &str, interval_min: i64, limit: i64) -> ApiResult<Vec<Map<String, Value>>> {
    let Some((view, id_col)) = interval_view(venue, interval_min) else {
        return Ok(vec![]);
    };
    let ob_view = match interval_min {
        1 => Some("prediction_markets.polymarket_candles_ob_1m"),
        5 => Some("prediction_markets.polymarket_candles_ob_5m"),
        _ => None,
    };
    let sql = if venue == "polymarket" {
        if matches!(interval_min, 1 | 5) && ob_view.is_some() {
            let ob = ob_view.unwrap();
            format!(r#"
                WITH live AS (
                  SELECT bucket_ts, avg(open)::double precision AS open, max(high)::double precision AS high,
                         min(low)::double precision AS low, avg(close)::double precision AS close,
                         sum(volume)::double precision AS volume, sum(trades)::bigint AS trades
                    FROM {view} WHERE {id_col} = $1 GROUP BY bucket_ts),
                ob AS (
                  SELECT bucket_ts, avg(open)::double precision AS open, max(high)::double precision AS high,
                         min(low)::double precision AS low, avg(close)::double precision AS close,
                         0::double precision AS volume, NULL::bigint AS trades
                    FROM {ob} WHERE asset_id IN (
                      SELECT jsonb_array_elements_text(clob_token_ids) FROM prediction_markets.polymarket_markets
                       WHERE condition_id = $1 AND clob_token_ids IS NOT NULL) GROUP BY bucket_ts)
                SELECT DISTINCT ON (bucket_ts) bucket_ts, open, high, low, close, volume, trades FROM (
                  SELECT bucket_ts, open, high, low, close, volume, trades, 0 AS pri FROM live
                  UNION ALL SELECT bucket_ts, open, high, low, close, volume, trades, 1 AS pri FROM ob) u
                 ORDER BY bucket_ts DESC, pri ASC LIMIT $2"#)
        } else if matches!(interval_min, 15 | 60 | 1440) {
            format!(r#"
                WITH live AS (
                  SELECT bucket_ts, avg(open)::double precision AS open, max(high)::double precision AS high,
                         min(low)::double precision AS low, avg(close)::double precision AS close,
                         sum(volume)::double precision AS volume, sum(trades)::bigint AS trades
                    FROM {view} WHERE {id_col} = $1 GROUP BY bucket_ts),
                archive AS (
                  SELECT bucket_ts, open, high, low, close, volume, NULL::bigint AS trades
                    FROM prediction_markets.candles WHERE venue='polymarket' AND market_id = $1
                      AND interval_min = {interval_min})
                SELECT DISTINCT ON (bucket_ts) bucket_ts, open, high, low, close, volume, trades FROM (
                  SELECT bucket_ts, open, high, low, close, volume, trades, 0 AS pri FROM live
                  UNION ALL SELECT bucket_ts, open, high, low, close, volume, trades, 1 AS pri FROM archive) u
                 ORDER BY bucket_ts DESC, pri ASC LIMIT $2"#)
        } else {
            format!(
                "SELECT bucket_ts, avg(open) AS open, max(high) AS high, min(low) AS low, \
                        avg(close) AS close, sum(volume) AS volume, sum(trades) AS trades \
                   FROM {view} WHERE {id_col} = $1 GROUP BY bucket_ts ORDER BY bucket_ts DESC LIMIT $2"
            )
        }
    } else {
        format!(
            "SELECT bucket_ts, open, high, low, close, volume, trades FROM {view} \
              WHERE {id_col} = $1 ORDER BY bucket_ts DESC LIMIT $2"
        )
    };
    let lim = limit.clamp(1, 5000);
    let client = pool.get().await?;
    let rows = client.query(&sql, &[&market_id, &lim]).await?;
    let mut out = rows_to_objects(&rows);
    out.reverse(); // oldest first
    Ok(out)
}

pub async fn open_interest(pool: &Pool, venue: &str, market_id: &str, limit: i64) -> ApiResult<Vec<Map<String, Value>>> {
    let lim = limit.clamp(1, 5000);
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT bucket_ts, open_interest FROM prediction_markets.open_interest_history \
              WHERE venue = $1 AND market_id = $2 ORDER BY bucket_ts DESC LIMIT $3",
            &[&venue, &market_id, &lim],
        )
        .await?;
    let mut out = rows_to_objects(&rows);
    out.reverse();
    Ok(out)
}

pub async fn top_holders(pool: &Pool, venue: &str, market_id: &str, limit: i64) -> ApiResult<Vec<Map<String, Value>>> {
    let lim = limit.clamp(1, 200);
    let client = pool.get().await?;
    let rows = client
        .query(
            "WITH latest AS (SELECT max(snapshot_ts) AS ts FROM prediction_markets.top_holders \
                              WHERE venue = $1 AND market_id = $2) \
             SELECT th.wallet, th.rank, th.position_size, th.position_value_usd, th.outcome_id, th.snapshot_ts \
               FROM prediction_markets.top_holders th, latest \
              WHERE th.venue = $1 AND th.market_id = $2 AND th.snapshot_ts = latest.ts ORDER BY th.rank LIMIT $3",
            &[&venue, &market_id, &lim],
        )
        .await?;
    Ok(rows_to_objects(&rows))
}

// ---------- wallet ----------
pub async fn wallet_profile(pool: &Pool, wallet: &str) -> ApiResult<Option<Map<String, Value>>> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            "SELECT wallet, primary_style, trading_styles, total_pnl, realized_pnl, volume, trades, \
                    win_rate, first_trade_at, last_trade_at FROM prediction_markets.wallet_profiles \
              WHERE wallet = $1 LIMIT 1",
            &[&wallet.to_lowercase()],
        )
        .await?;
    Ok(row.map(|r| row_to_object(&r)))
}

pub async fn wallet_pnl(pool: &Pool, wallet: &str, granularity: &str, limit: i64) -> ApiResult<Vec<Map<String, Value>>> {
    let lim = limit.clamp(1, 3000);
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT bucket_ts, realized_pnl FROM prediction_markets.wallet_pnl_history \
              WHERE wallet = $1 AND granularity = $2 ORDER BY bucket_ts DESC LIMIT $3",
            &[&wallet.to_lowercase(), &granularity, &lim],
        )
        .await?;
    let mut out = rows_to_objects(&rows);
    out.reverse();
    Ok(out)
}

pub async fn wallet_positions(pool: &Pool, wallet: &str, limit: i64) -> ApiResult<Vec<Map<String, Value>>> {
    let lim = limit.clamp(1, 500);
    let client = pool.get().await?;
    let rows = client
        .query(
            "WITH latest AS (SELECT max(snapshot_ts) AS ts FROM prediction_markets.wallet_positions WHERE wallet = $1) \
             SELECT p.condition_id, p.token_id, p.position_size, p.avg_entry_price, p.unrealized_pnl, \
                    p.realized_pnl, p.snapshot_ts FROM prediction_markets.wallet_positions p, latest \
              WHERE p.wallet = $1 AND p.snapshot_ts = latest.ts ORDER BY p.position_size DESC NULLS LAST LIMIT $2",
            &[&wallet.to_lowercase(), &lim],
        )
        .await?;
    Ok(rows_to_objects(&rows))
}

pub async fn wallet_activity(pool: &Pool, wallet: &str, limit: i64) -> ApiResult<Vec<Map<String, Value>>> {
    let lim = limit.clamp(1, 1000);
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT event_type, condition_id, neg_risk_market_id, amount, tx_hash, ts \
               FROM prediction_markets.activity_events WHERE wallet = $1 ORDER BY ts DESC LIMIT $2",
            &[&wallet.to_lowercase(), &lim],
        )
        .await?;
    Ok(rows_to_objects(&rows))
}

pub async fn leaderboard(pool: &Pool, limit: i64, window: &str, venue: &str) -> ApiResult<Vec<Map<String, Value>>> {
    // i32: `lb.rank <= $2` infers an int4 slot (tokio-postgres won't widen i64).
    let n = limit.clamp(1, 500) as i32;
    let client = pool.get().await?;
    let rows = client
        .query(
            "WITH latest AS (SELECT max(snapshot_ts) AS ts FROM prediction_markets.wallet_leaderboard \
                              WHERE window_name = $1 AND venue = $3) \
             SELECT lb.rank, lb.wallet, lb.total_pnl, lb.realized_pnl, lb.volume, lb.roi, lb.trades, \
                    lb.win_rate, lb.primary_style, lb.is_whale, lb.first_trade_at \
               FROM prediction_markets.wallet_leaderboard lb, latest \
              WHERE lb.window_name = $1 AND lb.venue = $3 AND lb.snapshot_ts = latest.ts AND lb.rank <= $2 \
              ORDER BY lb.rank LIMIT $2",
            &[&window, &n, &venue],
        )
        .await?;
    Ok(rows_to_objects(&rows))
}

pub async fn matched_pairs(pool: &Pool, venue: &str, venue_id: &str, limit: i64) -> ApiResult<Vec<Map<String, Value>>> {
    let lim = limit.clamp(1, 100);
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT venue_b AS other_venue, venue_b_id AS other_id, similarity, match_kind \
               FROM prediction_markets.matched_pairs WHERE venue_a = $1 AND venue_a_id = $2 \
              UNION ALL \
             SELECT venue_a, venue_a_id, similarity, match_kind FROM prediction_markets.matched_pairs \
              WHERE venue_b = $1 AND venue_b_id = $2 ORDER BY similarity DESC NULLS LAST LIMIT $3",
            &[&venue, &venue_id, &lim],
        )
        .await?;
    Ok(rows_to_objects(&rows))
}

pub async fn polymarket_events(pool: &Pool, q: Option<&str>, status: &str, limit: i64) -> ApiResult<Vec<Map<String, Value>>> {
    let mut params: P = Vec::new();
    let mut where_: Vec<String> = Vec::new();
    if let Some(qq) = q {
        params.push(Box::new(format!("%{qq}%")));
        let n = params.len();
        where_.push(format!("(title ILIKE ${n} OR slug ILIKE ${n})"));
    }
    match status {
        "open" => where_.push("active = true".into()),
        "closed" => where_.push("closed = true".into()),
        _ => {}
    }
    params.push(Box::new(limit.clamp(1, 200)));
    let lim = params.len();
    let where_clause = if where_.is_empty() { String::new() } else { format!("WHERE {}", where_.join(" AND ")) };
    let sql = format!(
        "SELECT event_id, slug, title, category, total_volume, active, closed, start_date, end_date \
           FROM prediction_markets.polymarket_events {where_clause} \
          ORDER BY COALESCE(closed, FALSE) ASC, end_date DESC NULLS LAST, total_volume DESC NULLS LAST LIMIT ${lim}"
    );
    let client = pool.get().await?;
    let rows = client.query(&sql, &refs(&params)).await?;
    Ok(rows_to_objects(&rows))
}
