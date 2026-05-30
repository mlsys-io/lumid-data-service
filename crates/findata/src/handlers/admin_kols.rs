//! KOL roster admin (WRITE) — port of `api/routes/admin_kols.py`.
//!
//! Both endpoints require `super_admin` (the `_require_super_admin` check);
//! the local-key bypass also passes, matching `require_admin` in `ingest.rs`.
//!   POST   /admin/kols           — add/upsert a handle into news.kol_roster.
//!   DELETE /admin/kols/{handle}  — soft-delete (active=false) a handle.

use axum::extract::{Path, State};
use axum::{Extension, Json};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::auth::Identity;
use crate::db::rows::row_to_object;
use crate::error::{ApiError, ApiResult};
use crate::state::AppState;

/// super_admin (or local-key) gate — mirrors `_require_super_admin` plus the
/// `require_admin` local-key bypass used across the ingress admin routes.
fn require_super_admin(identity: &Identity) -> ApiResult<()> {
    if identity.role == "super_admin" || identity.role == "local" {
        Ok(())
    } else {
        Err(ApiError::Forbidden("super_admin role required".into()))
    }
}

#[derive(Deserialize)]
pub struct KolCreateBody {
    pub handle: String,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub twitter_id: Option<String>,
    #[serde(default)]
    pub follower_tier: Option<String>,
    #[serde(default)]
    pub notes: Option<String>,
}

/// POST /admin/kols — INSERT … ON CONFLICT (handle) DO UPDATE, returns the row.
pub async fn add_kol(
    State(st): State<AppState>,
    Extension(identity): Extension<Identity>,
    Json(body): Json<KolCreateBody>,
) -> ApiResult<Json<Value>> {
    require_super_admin(&identity)?;
    let handle = body.handle.trim_start_matches('@').trim().to_string();
    if handle.is_empty() {
        return Err(ApiError::BadRequest("handle is required".into()));
    }
    let client = st.pool.get().await?;
    let row = client
        .query_one(
            "INSERT INTO news.kol_roster \
                 (handle, display_name, twitter_id, follower_tier, notes, \
                  active, added_by, updated_at) \
             VALUES ($1, $2, $3, $4, $5, true, $6, now()) \
             ON CONFLICT (handle) DO UPDATE SET \
                 display_name  = COALESCE(EXCLUDED.display_name, news.kol_roster.display_name), \
                 twitter_id    = COALESCE(EXCLUDED.twitter_id, news.kol_roster.twitter_id), \
                 follower_tier = COALESCE(EXCLUDED.follower_tier, news.kol_roster.follower_tier), \
                 notes         = COALESCE(EXCLUDED.notes, news.kol_roster.notes), \
                 active        = true, \
                 updated_at    = now() \
             RETURNING handle, display_name, twitter_id, follower_tier, notes, \
                       active, added_at, added_by, updated_at",
            &[
                &handle,
                &body.display_name,
                &body.twitter_id,
                &body.follower_tier,
                &body.notes,
                &identity.sub,
            ],
        )
        .await?;
    Ok(Json(Value::Object(row_to_object(&row))))
}

/// DELETE /admin/kols/{handle} — UPDATE active=false; 404 if no active match.
pub async fn remove_kol(
    State(st): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(handle): Path<String>,
) -> ApiResult<Json<Value>> {
    require_super_admin(&identity)?;
    let handle = handle.trim_start_matches('@').trim().to_string();
    let client = st.pool.get().await?;
    let row = client
        .query_opt(
            "UPDATE news.kol_roster \
                SET active = false, updated_at = now() \
              WHERE lower(handle) = lower($1) AND active \
             RETURNING handle",
            &[&handle],
        )
        .await?;
    if row.is_none() {
        return Err(ApiError::NotFound(format!("no active KOL handle '{handle}'")));
    }
    Ok(Json(json!({ "ok": true, "handle": handle, "active": false })))
}
