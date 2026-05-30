//! Freshness counts — port of `api/queries/freshness.py`.

use deadpool_postgres::Pool;
use serde_json::{Map, Value};

use crate::db::rows::row_to_object;
use crate::error::ApiResult;

const COUNTS_SQL: &str = r#"
    SELECT
        count(*) FILTER (WHERE sla_status = 'green') AS green,
        count(*) FILTER (WHERE sla_status = 'amber') AS amber,
        count(*) FILTER (WHERE sla_status = 'red')   AS red,
        count(*) FILTER (WHERE sla_status = 'gray')  AS gray
      FROM provenance.endpoint_freshness
"#;

pub async fn counts(pool: &Pool) -> ApiResult<Map<String, Value>> {
    let client = pool.get().await?;
    let row = client.query_one(COUNTS_SQL, &[]).await?;
    Ok(row_to_object(&row))
}
