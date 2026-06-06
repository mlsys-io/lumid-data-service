//! Per-table schema cards for LLM planning.
//!
//! Mirrors the Python `lumid_data.retrieve.schema_card` shape so that
//! prompt rendering produces the same M-Schema-style block.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnCard {
    pub name: String,
    /// SQL type, e.g. `"text"`, `"numeric"`, `"timestamptz"`.
    #[serde(rename = "type")]
    pub col_type: String,
    pub nullable: bool,
    pub description: Option<String>,
    #[serde(default)]
    pub is_pk: bool,
    #[serde(default)]
    pub is_fk: bool,
    pub distinct_count: Option<i64>,
    pub null_pct: Option<f64>,
    #[serde(default)]
    pub sample_values: Vec<serde_json::Value>,
    #[serde(default)]
    pub min: Option<serde_json::Value>,
    #[serde(default)]
    pub max: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKeyHint {
    pub column: String,
    pub ref_table: String,
    pub ref_column: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaCard {
    /// Fully-qualified `schema.table`.
    pub fqname: String,
    pub description: Option<String>,
    pub approx_row_count: Option<i64>,
    pub size_bytes: Option<i64>,
    pub columns: Vec<ColumnCard>,
    #[serde(default)]
    pub pk: Vec<String>,
    #[serde(default)]
    pub fks: Vec<ForeignKeyHint>,
    /// UTC timestamp of card construction (ISO-8601 string).
    pub built_at: String,
}

// ── Prompt rendering ──────────────────────────────────────────────────────────

fn sql_identifier(name: &str) -> String {
    let simple = name
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
    let starts_ok = name
        .bytes()
        .next()
        .map(|b| b.is_ascii_lowercase() || b == b'_')
        .unwrap_or(false);
    if simple && starts_ok {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

fn truncate(s: &str, n: usize) -> String {
    if s.len() <= n {
        s.to_string()
    } else {
        let mut end = n.saturating_sub(1);
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}…", &s[..end])
    }
}

pub fn render_card_for_prompt(card: &SchemaCard) -> String {
    let mut lines: Vec<String> = Vec::new();
    lines.push(format!("# Table: {}", card.fqname));
    lines.push("# Use SQL identifiers exactly as shown below.".to_string());
    if let Some(ref desc) = card.description {
        lines.push(format!("# Description: {desc}"));
    }
    if let Some(n) = card.approx_row_count {
        lines.push(format!("# Approx rows: {n}"));
    }
    lines.push("[".to_string());
    for col in &card.columns {
        let mut flags: Vec<&str> = Vec::new();
        if col.is_pk {
            flags.push("PK");
        }
        if col.is_fk {
            flags.push("FK");
        }
        if !col.nullable {
            flags.push("NOT NULL");
        }
        let flag_str = if flags.is_empty() {
            String::new()
        } else {
            format!(" {}", flags.join(","))
        };
        let mut bits: Vec<String> = vec![format!(
            "  ({}:{}{}",
            sql_identifier(&col.name),
            col.col_type,
            flag_str
        )];
        if let Some(ref d) = col.description {
            bits.push(format!(", {d}"));
        }
        if let Some(dc) = col.distinct_count {
            bits.push(format!(", distinct={dc}"));
        }
        if let Some(np) = col.null_pct {
            if np > 0.0 {
                bits.push(format!(", null_pct={np:.2}"));
            }
        }
        if col.min.is_some() && col.max.is_some() {
            bits.push(format!(
                ", min={}, max={}",
                col.min.as_ref().unwrap(),
                col.max.as_ref().unwrap()
            ));
        }
        if !col.sample_values.is_empty() {
            let previews: Vec<String> = col
                .sample_values
                .iter()
                .take(5)
                .map(|v| truncate(&v.to_string(), 60))
                .collect();
            bits.push(format!(", samples=[{}]", previews.join(", ")));
        }
        bits.push(")".to_string());
        lines.push(format!("{},", bits.concat()));
    }
    lines.push("]".to_string());
    if !card.pk.is_empty() {
        lines.push(format!("# Primary key: ({})", card.pk.join(", ")));
    }
    for fk in &card.fks {
        lines.push(format!(
            "# FK: {} -> {}.{}",
            fk.column, fk.ref_table, fk.ref_column
        ));
    }
    lines.join("\n")
}

/// Render a slice of cards into the system-prompt addendum (M-Schema style).
pub fn render_bundle_for_prompt(cards: &[SchemaCard]) -> String {
    let mut parts: Vec<String> = Vec::new();
    for card in cards {
        parts.push(String::new());
        parts.push(render_card_for_prompt(card));
    }
    parts.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_card() -> SchemaCard {
        SchemaCard {
            fqname: "market.ohlc_daily".to_string(),
            description: Some("Daily OHLC bars".to_string()),
            approx_row_count: Some(1_000_000),
            size_bytes: Some(50_000_000),
            columns: vec![
                ColumnCard {
                    name: "ticker".to_string(),
                    col_type: "text".to_string(),
                    nullable: false,
                    description: None,
                    is_pk: false,
                    is_fk: false,
                    distinct_count: Some(8000),
                    null_pct: None,
                    sample_values: vec![
                        serde_json::json!("AAPL"),
                        serde_json::json!("MSFT"),
                    ],
                    min: None,
                    max: None,
                },
                ColumnCard {
                    name: "date".to_string(),
                    col_type: "date".to_string(),
                    nullable: false,
                    description: None,
                    is_pk: true,
                    is_fk: false,
                    distinct_count: None,
                    null_pct: None,
                    sample_values: vec![],
                    min: Some(serde_json::json!("2020-01-01")),
                    max: Some(serde_json::json!("2026-01-01")),
                },
            ],
            pk: vec!["ticker".to_string(), "date".to_string()],
            fks: vec![],
            built_at: "2026-06-03T00:00:00Z".to_string(),
        }
    }

    #[test]
    fn render_card_includes_table_name() {
        let card = sample_card();
        let text = render_card_for_prompt(&card);
        assert!(text.contains("# Table: market.ohlc_daily"));
        assert!(text.contains("Daily OHLC bars"));
        assert!(text.contains("# Primary key:"));
    }

    #[test]
    fn sql_identifier_quotes_mixed_case() {
        assert_eq!(sql_identifier("foo_bar"), "foo_bar");
        assert_eq!(sql_identifier("MyTable"), "\"MyTable\"");
        assert_eq!(sql_identifier("has space"), "\"has space\"");
    }
}
