//! Restricted schemas: data only an allow-list of people may see.
//!
//! Built for UK Biobank at the NUS hub (2026-11-25): its terms limit access to
//! approved researchers, while the catalog otherwise shows every listed schema
//! to every signed-in user. Two layers, either of which alone stops a read:
//!
//! 1. **App.** A restricted schema is invisible to anyone not on its list: it is
//!    left out of `/catalog/schemas`, its catalog routes answer 404, the agent's
//!    schema cards omit it, and a `/retrieve` (or agent) query that names one of
//!    its tables is refused as an unknown table. Local keys and job tokens are
//!    never on a list — a runtime's server-wide key must not become a way around
//!    the allow-list.
//! 2. **Database.** Retrieval runs as `LUMID_RETRIEVAL_DB_ROLE` for everyone,
//!    except allowed callers, who run as `LUMID_RESTRICTED_DB_ROLE`. Deploy the
//!    restricted data so only that role can read it (for a `postgres_fdw`
//!    import, the user mapping exists for that role alone), and a query the app
//!    check missed still fails inside Postgres.
//!
//! Config: `LUMID_RESTRICTED_SCHEMAS` = JSON object, schema → list of lum.id
//! `sub`s, e.g. `{"ukb": ["4886ae45-…", "…"]}`. Empty (default) = nothing is
//! restricted and every path below is a no-op.

use std::collections::{BTreeMap, BTreeSet};

use crate::auth::Identity;
use crate::config::Settings;
use crate::error::{ApiError, ApiResult};

/// Parse `LUMID_RESTRICTED_SCHEMAS`. Malformed JSON is a startup error rather
/// than "nothing restricted": failing open would publish the data.
pub fn parse(raw: &str) -> Result<BTreeMap<String, BTreeSet<String>>, String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(BTreeMap::new());
    }
    let v: BTreeMap<String, Vec<String>> = serde_json::from_str(raw).map_err(|e| {
        format!("LUMID_RESTRICTED_SCHEMAS is not a JSON object of schema -> [sub]: {e}")
    })?;
    Ok(v.into_iter()
        .map(|(schema, subs)| {
            (
                schema.trim().to_string(),
                subs.into_iter()
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect(),
            )
        })
        .filter(|(s, _)| !s.is_empty())
        .collect())
}

/// Roles that are never on an allow-list (server keys, job tokens).
fn is_service_role(role: &str) -> bool {
    matches!(role, "local" | "job")
}

/// Whether `caller` may see `schema`. Unrestricted schemas: always.
pub fn may_see(st: &Settings, caller: Option<&Identity>, schema: &str) -> bool {
    match st.restricted_schemas.get(schema) {
        None => true,
        Some(allowed) => caller
            .is_some_and(|id| id.active && !is_service_role(&id.role) && allowed.contains(&id.sub)),
    }
}

/// Whether `caller` is on the list of at least one restricted schema.
pub fn may_see_any(st: &Settings, caller: Option<&Identity>) -> bool {
    st.restricted_schemas.keys().any(|s| may_see(st, caller, s))
}

/// The role a caller's retrieval runs as.
pub fn retrieval_role<'a>(st: &'a Settings, caller: Option<&Identity>) -> &'a str {
    if !st.restricted_db_role.trim().is_empty() && may_see_any(st, caller) {
        &st.restricted_db_role
    } else {
        &st.retrieval_db_role
    }
}

/// Refuse SQL that names a table in a schema the caller may not see. Answers
/// "unknown table" — the same as for a table that does not exist — so the
/// refusal does not confirm the data is there.
pub fn check_sql(st: &Settings, caller: Option<&Identity>, sql: &str) -> ApiResult<()> {
    if st.restricted_schemas.is_empty() {
        return Ok(());
    }
    let lowered = sql.to_lowercase();
    match crate::jobs::scope::extract_relations(sql) {
        Ok((rels, _ctes)) => {
            for (schema, table) in rels {
                if let Some(s) = schema {
                    if !may_see(st, caller, &s) {
                        return Err(ApiError::NotFound(format!("unknown table {s}.{table}")));
                    }
                }
            }
        }
        // Unparseable SQL: fall back to a substring check rather than letting it
        // through (the database layer still applies either way).
        Err(_) => {
            for s in st.restricted_schemas.keys() {
                if lowered.contains(&format!("{}.", s.to_lowercase())) && !may_see(st, caller, s) {
                    return Err(ApiError::NotFound(format!("unknown schema {s}")));
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(sub: &str, role: &str) -> Identity {
        Identity {
            sub: sub.into(),
            role: role.into(),
            email: None,
            active: true,
            scopes: vec![],
        }
    }
    fn settings() -> Settings {
        let mut s = Settings::from_env();
        s.restricted_schemas = parse(r#"{"ukb": ["alice"]}"#).unwrap();
        s.retrieval_db_role = "lumid_reader".into();
        s.restricted_db_role = "ukb_reader".into();
        s
    }

    #[test]
    fn parse_rejects_garbage_and_accepts_empty() {
        assert!(parse("").unwrap().is_empty());
        assert!(parse("not json").is_err());
        assert_eq!(parse(r#"{"ukb":[" a ",""]}"#).unwrap()["ukb"].len(), 1);
    }

    #[test]
    fn only_listed_people_see_a_restricted_schema() {
        let s = settings();
        assert!(may_see(&s, Some(&id("alice", "user")), "ukb"));
        assert!(!may_see(&s, Some(&id("bob", "user")), "ukb"));
        assert!(!may_see(&s, Some(&id("bob", "admin")), "ukb"));
        assert!(!may_see(&s, None, "ukb"));
        assert!(may_see(&s, Some(&id("bob", "user")), "arxiv"));
    }

    #[test]
    fn service_keys_are_never_on_a_list() {
        let s = settings();
        assert!(!may_see(&s, Some(&id("alice", "local")), "ukb"));
        assert!(!may_see(&s, Some(&id("alice", "job")), "ukb"));
    }

    #[test]
    fn role_switches_only_for_allowed_callers() {
        let s = settings();
        assert_eq!(retrieval_role(&s, Some(&id("alice", "user"))), "ukb_reader");
        assert_eq!(retrieval_role(&s, Some(&id("bob", "user"))), "lumid_reader");
        assert_eq!(retrieval_role(&s, None), "lumid_reader");
    }

    #[test]
    fn sql_naming_a_hidden_table_is_unknown() {
        let s = settings();
        let bob = id("bob", "user");
        assert!(check_sql(&s, Some(&bob), "SELECT * FROM ukb.phenotype LIMIT 5").is_err());
        assert!(check_sql(
            &s,
            Some(&bob),
            "SELECT a.x FROM arxiv.papers a JOIN ukb.participants p ON true"
        )
        .is_err());
        assert!(check_sql(&s, Some(&bob), "SELECT * FROM arxiv.papers LIMIT 5").is_ok());
        assert!(check_sql(
            &s,
            Some(&id("alice", "user")),
            "SELECT * FROM ukb.phenotype LIMIT 5"
        )
        .is_ok());
    }
}
