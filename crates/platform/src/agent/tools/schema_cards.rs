//! `get_schema_cards` tool — returns cached schema cards for LLM planning.

use serde_json::{json, Map, Value};

use crate::error::ApiResult;
use crate::retrieve::schema_card::render_bundle_for_prompt;
use crate::state::AppState;

pub struct GetSchemaCardsTool;

impl super::Tool for GetSchemaCardsTool {
    fn name(&self) -> &str {
        "get_schema_cards"
    }

    fn description(&self) -> &str {
        "Return compact schema cards for SQL planning. Call this before composing \
         SQL for a natural-language data request. The result contains table and \
         column names, useful stats, samples, foreign keys, and join hints. Use \
         the exact identifiers shown — do not guess or invent names."
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "scope": {
                    "type": "string",
                    "description": "Optional comma-separated schema names to restrict the card scope, \
                                    e.g. \"market,fundamentals\". Omit or leave empty for all schemas."
                }
            },
            "required": [],
            "additionalProperties": false
        })
    }
}

/// Disambiguates "no restriction → fetch all" from "empty intersection → fetch none".
///
/// Security: without this distinction a request for a non-permitted schema would
/// pass an empty slice to `card_store.get_or_build`, which treats `&[]` as
/// "all non-system schemas" — leaking every schema to the caller.
#[derive(Debug, PartialEq)]
pub enum Scope {
    /// No allowlist and no explicit request — let the card store return every
    /// non-system schema (passes `&[]` to `get_or_build`).
    All,
    /// A concrete (possibly empty) set of schemas.  An empty `Only(vec![])` means
    /// the intersection was empty → return zero cards without touching the store.
    Only(Vec<String>),
}

/// Compute the effective schema scope to surface to the agent.
///
/// `user_schemas` is a hard visibility cap: when non-empty, only schemas that
/// appear in it can ever be shown — an explicit `requested` scope is intersected
/// with it, not substituted for it.  This prevents the agent from requesting
/// cards for schemas outside the operator's allowlist.
///
/// NOTE — this is a *card-visibility* filter, NOT a query-execution boundary.
/// `replay_retrieval_plan` / `POST /retrieve` run their SELECTs with whatever
/// privileges the effective Postgres role holds; a caller that already knows a
/// table name can read it regardless of `user_schemas`. To make the allowlist a
/// true access boundary, set `LUMID_RETRIEVAL_DB_ROLE` to a NOSUPERUSER role
/// whose SELECT grants match these schemas (see `Settings::retrieval_db_role` and
/// the replayer's `SET LOCAL ROLE`). Treat `user_schemas` as "what the planner is
/// told about", not "what can be read".
///
/// When `user_schemas` is empty, fall back to `requested` as-is (or `Scope::All`
/// to signal "all non-system" to the card store).
pub fn effective_scope(requested: &[String], allowlist: &[String]) -> Scope {
    if allowlist.is_empty() {
        if requested.is_empty() {
            // No restriction at all — show every non-system schema.
            Scope::All
        } else {
            // No allowlist but caller narrowed the scope explicitly.
            Scope::Only(requested.to_vec())
        }
    } else if requested.is_empty() {
        // No explicit request — show everything permitted.
        Scope::Only(allowlist.to_vec())
    } else {
        // Intersection: only schemas that are both requested AND permitted.
        // An empty result here means "nothing accessible" — NOT "all schemas".
        let intersection: Vec<String> = requested
            .iter()
            .filter(|s| allowlist.contains(s))
            .cloned()
            .collect();
        Scope::Only(intersection)
    }
}

pub async fn get_schema_cards(st: &AppState, args: &Map<String, Value>) -> ApiResult<Value> {
    let scope_raw = args
        .get("scope")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();

    let requested: Vec<String> = scope_raw
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let scope = effective_scope(&requested, &st.settings.user_schemas);

    let store = &st.card_store;
    let cards = match scope {
        // No allowlist and no explicit request — fetch all non-system schemas.
        Scope::All => store.get_or_build(&[]).await?,
        // Non-empty concrete set — fetch exactly those schemas.
        Scope::Only(ref schemas) if !schemas.is_empty() => store.get_or_build(schemas).await?,
        // Empty intersection (requested schema not in allowlist) — return zero cards.
        // Security: must NOT fall through to get_or_build(&[]) which means "all".
        Scope::Only(_) => vec![].into(),
    };

    let rendered = render_bundle_for_prompt(&cards);

    Ok(json!({
        "table_count": cards.len(),
        "schema_cards": rendered,
    }))
}

#[cfg(test)]
mod tests {
    use super::{effective_scope, Scope};

    fn sv(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn effective_scope_no_allowlist_returns_all() {
        // No allowlist, no request → All (card store will fetch every non-system schema).
        assert_eq!(effective_scope(&[], &[]), Scope::All);
    }

    #[test]
    fn effective_scope_no_allowlist_with_requested_returns_only() {
        assert_eq!(
            effective_scope(&sv(&["a", "b"]), &[]),
            Scope::Only(sv(&["a", "b"]))
        );
    }

    #[test]
    fn effective_scope_allowlist_empty_requested_returns_allowlist() {
        assert_eq!(
            effective_scope(&[], &sv(&["market", "fundamentals"])),
            Scope::Only(sv(&["market", "fundamentals"]))
        );
    }

    #[test]
    fn effective_scope_intersection_filters_out_of_allowlist() {
        // "secret" is not in the allowlist — must be dropped.
        let result = effective_scope(
            &sv(&["market", "secret", "fundamentals"]),
            &sv(&["market", "fundamentals"]),
        );
        assert_eq!(result, Scope::Only(sv(&["market", "fundamentals"])));
    }

    #[test]
    fn effective_scope_intersection_no_overlap_returns_only_empty() {
        // Security: allowlist ["market"] + requested ["secret"] → Only([]).
        // Must NOT resolve to Scope::All (which would expose every schema).
        let result = effective_scope(&sv(&["secret"]), &sv(&["market", "fundamentals"]));
        assert_eq!(result, Scope::Only(sv(&[])));
    }

    #[test]
    fn effective_scope_intersection_full_overlap_returns_requested_order() {
        let result = effective_scope(
            &sv(&["fundamentals", "market"]),
            &sv(&["market", "fundamentals"]),
        );
        // Order follows the requested slice.
        assert_eq!(result, Scope::Only(sv(&["fundamentals", "market"])));
    }
}
