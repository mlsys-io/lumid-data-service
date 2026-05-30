//! Estimates domain — price targets, grades, recommendation, analyst estimates.
//! Ports of api/queries/{estimates,grades,recommendation,analyst_estimates}.py.

use deadpool_postgres::Pool;
use serde_json::{json, Map, Value};

use crate::db::rows::{row_to_object, rows_to_objects};
use crate::error::{ApiError, ApiResult};

const PRICE_TARGET_SQL: &str = r#"
    SELECT c.symbol,
           c.consensus::float8 AS target_consensus,
           c.high::float8      AS target_high,
           c.low::float8       AS target_low,
           s.last_avg::float8  AS target_median,
           s.num_analysts      AS analysts,
           GREATEST(c.ingest_ts, s.ingest_ts) AS updated_at
      FROM estimates.price_target_consensus c
      LEFT JOIN estimates.price_target_summary s
        ON s.symbol = c.symbol AND s.source = c.source
     WHERE c.symbol = $1 AND c.source = 'fmp'
     LIMIT 1
"#;

pub async fn price_target(pool: &Pool, symbol: &str) -> ApiResult<Option<Map<String, Value>>> {
    let client = pool.get().await?;
    let row = client.query_opt(PRICE_TARGET_SQL, &[&symbol.to_uppercase()]).await?;
    Ok(row.map(|r| row_to_object(&r)))
}

pub async fn grades(pool: &Pool, symbol: &str, limit: i64) -> ApiResult<Vec<Map<String, Value>>> {
    let limit = limit.clamp(1, 200);
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT date, firm, grade, action FROM estimates.grades \
             WHERE symbol = $1 AND source = 'fmp' ORDER BY date DESC LIMIT $2",
            &[&symbol.to_uppercase(), &limit],
        )
        .await?;
    Ok(rows_to_objects(&rows))
}

pub async fn recommendation(
    pool: &Pool,
    symbol: &str,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let limit = limit.clamp(1, 60);
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT period, strong_buy, buy, hold, sell, strong_sell \
             FROM estimates.recommendation WHERE symbol = $1 AND source = 'finnhub' \
             ORDER BY period DESC LIMIT $2",
            &[&symbol.to_uppercase(), &limit],
        )
        .await?;
    Ok(rows_to_objects(&rows))
}

/// Long→wide pivot: one row per (period_end_date, period_type) with a `metrics`
/// map keyed by metric name. Mirrors analyst_estimates.for_symbol.
pub async fn analyst_estimates(
    pool: &Pool,
    symbol: &str,
    period: &str,
    since: Option<&str>,
    limit_periods: i64,
) -> ApiResult<Vec<Value>> {
    if !matches!(period, "quarter" | "annual" | "all") {
        return Err(ApiError::BadRequest(
            "period must be quarter, annual, or all".into(),
        ));
    }
    let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> =
        vec![Box::new(symbol.to_uppercase())];
    let mut where_ = vec!["symbol = $1".to_string(), "source = 'fmp'".to_string()];
    match period {
        "quarter" => where_.push("period_type = 'quarter'".into()),
        "annual" => where_.push("period_type = 'annual'".into()),
        _ => {}
    }
    if let Some(s) = since {
        let d = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .map_err(|_| ApiError::BadRequest(format!("since must be YYYY-MM-DD (got {s:?})")))?;
        params.push(Box::new(d));
        where_.push(format!("period_end_date >= ${}", params.len()));
    }
    params.push(Box::new(limit_periods.clamp(1, 100)));
    let limit_idx = params.len();
    let sql = format!(
        "WITH ranked_periods AS (\
           SELECT period_end_date, period_type FROM estimates.analyst_estimates \
            WHERE {} GROUP BY period_end_date, period_type \
            ORDER BY period_end_date DESC LIMIT ${limit_idx}) \
         SELECT a.period_end_date, a.period_type, a.metric, \
                a.low::float8, a.high::float8, a.avg::float8, a.num_analysts \
           FROM estimates.analyst_estimates a \
           JOIN ranked_periods p USING (period_end_date, period_type) \
          WHERE a.symbol = $1 AND a.source = 'fmp' \
          ORDER BY a.period_end_date DESC, a.period_type, a.metric",
        where_.join(" AND ")
    );
    let refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
        params.iter().map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
    let client = pool.get().await?;
    let rows = client.query(&sql, &refs).await?;

    // Pivot, preserving first-seen period order.
    let mut order: Vec<(String, String)> = Vec::new();
    let mut by_period: std::collections::HashMap<(String, String), Map<String, Value>> =
        std::collections::HashMap::new();
    for r in &rows {
        let o = row_to_object(r);
        let ped = o.get("period_end_date").cloned().unwrap_or(Value::Null);
        let ptype = o.get("period_type").cloned().unwrap_or(Value::Null);
        let key = (ped.to_string(), ptype.to_string());
        let entry = by_period.entry(key.clone()).or_insert_with(|| {
            order.push(key.clone());
            let mut m = Map::new();
            m.insert("period_end_date".into(), ped.clone());
            m.insert("period_type".into(), ptype.clone());
            m.insert("metrics".into(), Value::Object(Map::new()));
            m
        });
        let metric = o.get("metric").and_then(|v| v.as_str()).unwrap_or("").to_string();
        if let Some(Value::Object(metrics)) = entry.get_mut("metrics") {
            metrics.insert(
                metric,
                json!({
                    "low": o.get("low").cloned().unwrap_or(Value::Null),
                    "high": o.get("high").cloned().unwrap_or(Value::Null),
                    "avg": o.get("avg").cloned().unwrap_or(Value::Null),
                    "num_analysts": o.get("num_analysts").cloned().unwrap_or(Value::Null),
                }),
            );
        }
    }
    Ok(order
        .into_iter()
        .map(|k| Value::Object(by_period.remove(&k).unwrap()))
        .collect())
}
