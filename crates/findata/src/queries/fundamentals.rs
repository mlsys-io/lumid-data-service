//! Fundamentals queries — port of `api/queries/fundamentals.py`.

use deadpool_postgres::Pool;
use serde_json::{Map, Value};

use crate::db::rows::{row_to_object, rows_to_objects};
use crate::error::{ApiError, ApiResult};

const LATEST_SQL: &str = r#"
    SELECT symbol, period_end_date, period_type, report_date, currency,
           revenue, gross_profit, operating_income, ebitda, net_income,
           eps, eps_diluted,
           total_assets, total_liabilities, total_equity,
           cash_and_equivalents, total_debt, long_term_debt,
           operating_cash_flow, investing_cash_flow, financing_cash_flow,
           free_cash_flow, capex, dividends_paid, stock_repurchases
      FROM fundamentals.latest_per_symbol
     WHERE symbol = $1 AND source = 'fmp'
"#;

pub async fn latest(pool: &Pool, symbol: &str) -> ApiResult<Option<Map<String, Value>>> {
    let client = pool.get().await?;
    let row = client.query_opt(LATEST_SQL, &[&symbol.to_uppercase()]).await?;
    Ok(row.map(|r| row_to_object(&r)))
}

/// (table, select-list) per statement — mirrors `_STATEMENT_TABLES`.
fn statement_table(statement: &str) -> Option<(&'static str, &'static str)> {
    match statement {
        "income" => Some((
            "fundamentals.income_statement",
            "period_end_date, period_type, revenue, gross_profit, operating_income, ebitda, net_income, eps",
        )),
        "balance" => Some((
            "fundamentals.balance_sheet",
            "period_end_date, period_type, total_assets, total_liabilities, total_equity, cash_and_equivalents, total_debt",
        )),
        "cashflow" => Some((
            "fundamentals.cash_flow_statement",
            "period_end_date, period_type, operating_cash_flow, investing_cash_flow, financing_cash_flow, free_cash_flow, capex",
        )),
        _ => None,
    }
}

pub async fn history(
    pool: &Pool,
    symbol: &str,
    statement: &str,
    period: &str,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let (table, cols) = statement_table(statement).ok_or_else(|| {
        ApiError::BadRequest(format!(
            "statement must be income, balance, or cashflow (got {statement:?})"
        ))
    })?;
    let period_filter = match period {
        "quarter" => " AND period_type IN ('Q1','Q2','Q3','Q4')",
        "fy" => " AND period_type = 'FY'",
        "all" => "",
        _ => {
            return Err(ApiError::BadRequest(format!(
                "period must be quarter, fy, or all (got {period:?})"
            )))
        }
    };
    let limit = limit.clamp(1, 200);
    let sql = format!(
        "SELECT {cols} FROM {table} WHERE symbol = $1 AND source = 'fmp'{period_filter} \
         ORDER BY period_end_date DESC LIMIT $2"
    );
    let client = pool.get().await?;
    let rows = client.query(&sql, &[&symbol.to_uppercase(), &limit]).await?;
    Ok(rows_to_objects(&rows))
}
