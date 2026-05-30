//! Reference depth + misc — ports of api/queries/{reference_depth,reference_misc}.py.

use chrono::{Datelike, FixedOffset, NaiveDate, NaiveTime, Timelike, Utc};
use deadpool_postgres::Pool;
use serde_json::{Map, Value};

use findata::db::qb::Qb;
use findata::db::rows::rows_to_objects;
use findata::error::ApiResult;

// ---------- reference_depth ----------
pub async fn executives(
    pool: &Pool,
    symbol: &str,
    current_only: bool,
) -> ApiResult<Vec<Map<String, Value>>> {
    let extra = if current_only { " AND until IS NULL" } else { "" };
    let sql = format!(
        "SELECT name, title, since, until, age, gender, pay::float8 AS pay \
           FROM reference.executives WHERE symbol = $1{extra} \
          ORDER BY since DESC NULLS LAST, name"
    );
    let client = pool.get().await?;
    let rows = client.query(&sql, &[&symbol.to_uppercase()]).await?;
    Ok(rows_to_objects(&rows))
}

pub async fn compensation(pool: &Pool, symbol: &str, limit: i64) -> ApiResult<Vec<Map<String, Value>>> {
    let limit = limit.clamp(1, 200);
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT name, year, compensation_total::float8 AS compensation_total, \
                    compensation_breakdown FROM reference.governance_compensation \
              WHERE symbol = $1 ORDER BY year DESC NULLS LAST, compensation_total DESC NULLS LAST LIMIT $2",
            &[&symbol.to_uppercase(), &limit],
        )
        .await?;
    Ok(rows_to_objects(&rows))
}

pub async fn peers(pool: &Pool, symbol: &str, limit: i64) -> ApiResult<Vec<Value>> {
    let limit = limit.clamp(1, 100);
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT peer_symbol FROM reference.peers WHERE symbol = $1 ORDER BY peer_symbol LIMIT $2",
            &[&symbol.to_uppercase(), &limit],
        )
        .await?;
    Ok(rows.iter().map(|r| Value::String(r.get::<_, String>("peer_symbol"))).collect())
}

pub async fn supply_chain(
    pool: &Pool,
    symbol: &str,
    kind: Option<&str>,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let mut qb = Qb::new();
    qb.eq("symbol", symbol.to_uppercase());
    if let Some(k) = kind {
        qb.eq("kind", k.to_string());
    }
    let lim = qb.push(limit.clamp(1, 200));
    let sql = format!(
        "SELECT related_symbol, kind, weight::float8 AS weight FROM reference.supply_chain \
          WHERE {} ORDER BY weight DESC NULLS LAST, related_symbol LIMIT ${lim}",
        qb.and_join()
    );
    let client = pool.get().await?;
    let rows = client.query(&sql, &qb.refs()).await?;
    Ok(rows_to_objects(&rows))
}

pub async fn shares_float(pool: &Pool, symbol: &str, limit: i64) -> ApiResult<Vec<Map<String, Value>>> {
    let limit = limit.clamp(1, 60);
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT date, free_float::float8 AS free_float, float_shares::float8 AS float_shares, \
                    outstanding_shares::float8 AS outstanding_shares FROM reference.shares_float \
              WHERE symbol = $1 ORDER BY date DESC NULLS LAST LIMIT $2",
            &[&symbol.to_uppercase(), &limit],
        )
        .await?;
    Ok(rows_to_objects(&rows))
}

// ---------- reference_misc ----------
pub async fn employee_count(pool: &Pool, symbol: &str, limit: i64) -> ApiResult<Vec<Map<String, Value>>> {
    let limit = limit.clamp(1, 200);
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT as_of, employee_count FROM reference.historical_employee_count \
              WHERE symbol = $1 ORDER BY as_of DESC LIMIT $2",
            &[&symbol.to_uppercase(), &limit],
        )
        .await?;
    Ok(rows_to_objects(&rows))
}

pub async fn symbol_changes(
    pool: &Pool,
    symbol: Option<&str>,
    since: Option<NaiveDate>,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let mut qb = Qb::new();
    if let Some(s) = symbol {
        let su = s.to_uppercase();
        let a = qb.push(su.clone());
        let b = qb.push(su.clone());
        let c = qb.push(su);
        qb.where_.push(format!("(symbol = ${a} OR old_symbol = ${b} OR new_symbol = ${c})"));
    }
    if let Some(d) = since {
        qb.cmp("change_date", ">=", d);
    }
    let lim = qb.push(limit.clamp(1, 500));
    let sql = format!(
        "SELECT change_date, symbol, name, old_symbol, new_symbol FROM reference.symbol_change \
         {} ORDER BY change_date DESC LIMIT ${lim}",
        qb.where_clause()
    );
    let client = pool.get().await?;
    let rows = client.query(&sql, &qb.refs()).await?;
    Ok(rows_to_objects(&rows))
}

pub async fn exchange_holidays(
    pool: &Pool,
    exchange: &str,
    since: Option<NaiveDate>,
    until: Option<NaiveDate>,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let mut qb = Qb::new();
    qb.eq("exchange", exchange.to_uppercase());
    if let Some(d) = since {
        qb.cmp("holiday_date", ">=", d);
    }
    if let Some(d) = until {
        qb.cmp("holiday_date", "<=", d);
    }
    let lim = qb.push(limit.clamp(1, 500));
    let sql = format!(
        "SELECT holiday_date, name, is_closed, is_half_day, adj_open_time, adj_close_time \
           FROM reference.exchange_holidays WHERE {} ORDER BY holiday_date LIMIT ${lim}",
        qb.and_join()
    );
    let client = pool.get().await?;
    let rows = client.query(&sql, &qb.refs()).await?;
    Ok(rows_to_objects(&rows))
}

/// Parse "9:30 AM -04:00" → (time, offset). None on any malformation.
fn parse_hour(s: &str) -> Option<(NaiveTime, FixedOffset)> {
    let toks: Vec<&str> = s.split_whitespace().collect();
    if toks.len() != 3 {
        return None;
    }
    let (h, m) = toks[0].split_once(':')?;
    let mut hh: u32 = h.parse().ok()?;
    let mm: u32 = m.parse().ok()?;
    hh %= 12;
    if toks[1].eq_ignore_ascii_case("PM") {
        hh += 12;
    }
    let off = toks[2];
    let sign = if off.starts_with('-') { -1 } else { 1 };
    let (oh, om) = off.trim_start_matches(['+', '-']).split_once(':')?;
    let secs = sign * (oh.parse::<i32>().ok()? * 3600 + om.parse::<i32>().ok()? * 60);
    Some((NaiveTime::from_hms_opt(hh, mm, 0)?, FixedOffset::east_opt(secs)?))
}

fn compute_is_open(opens_at: Option<&str>, closes_at: Option<&str>) -> Option<bool> {
    let (open_t, tz) = parse_hour(opens_at?)?;
    let (close_t, _) = parse_hour(closes_at?)?;
    let now = Utc::now().with_timezone(&tz);
    if now.weekday().num_days_from_monday() >= 5 {
        return Some(false);
    }
    let t = now.time();
    // truncate to minute precision to mirror Python's `time(h, m)` comparison
    let t = NaiveTime::from_hms_opt(t.hour(), t.minute(), 0)?;
    Some(open_t <= t && t <= close_t)
}

pub async fn exchange_hours(pool: &Pool, exchange: Option<&str>) -> ApiResult<Vec<Map<String, Value>>> {
    let mut qb = Qb::new();
    if let Some(e) = exchange {
        qb.eq("exchange", e.to_uppercase());
    }
    let sql = format!(
        "SELECT exchange, name, opens_at, closes_at, opens_additional, closes_additional, timezone \
           FROM reference.exchange_hours {} ORDER BY exchange",
        qb.where_clause()
    );
    let client = pool.get().await?;
    let rows = client.query(&sql, &qb.refs()).await?;
    let mut out = rows_to_objects(&rows);
    for o in out.iter_mut() {
        let opens = o.get("opens_at").and_then(|v| v.as_str());
        let closes = o.get("closes_at").and_then(|v| v.as_str());
        let is_open = compute_is_open(opens, closes);
        o.insert("is_open".to_string(), is_open.map(Value::Bool).unwrap_or(Value::Null));
    }
    Ok(out)
}
