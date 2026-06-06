//! Lumid identity bridge — port of `api/lumid.py`.
//!
//! Bearer tokens (PAT `lm_pat_live_*`/`rm_pat_live_*` or RS256 JWT) are
//! validated via `POST {lumid_url}/api/v1/identity/introspect` and cached for a
//! short TTL keyed on SHA-256 of the token (so heap dumps don't leak secrets).
//! Negative results are cached too. Transport failures bubble up so the caller
//! can fail closed (503).

use std::time::Duration;

use moka::future::Cache;
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::config::Settings;

#[derive(Clone, Debug)]
pub struct Identity {
    pub sub: String,
    pub role: String,
    pub email: Option<String>,
    pub active: bool,
    pub scopes: Vec<String>,
}

/// Distinguishes "rejected" (cache & return None → 401) from "unreachable"
/// (→ 503, fail-closed). Mirrors the Python split.
#[derive(Debug)]
pub enum LumidError {
    Unreachable(String),
}

#[derive(Deserialize)]
struct IntrospectData {
    #[serde(default)]
    sub: Option<String>,
    #[serde(default)]
    role: Option<String>,
    #[serde(default)]
    email: Option<String>,
    #[serde(default)]
    active: bool,
    #[serde(default)]
    scopes: Option<Vec<String>>,
}

#[derive(Deserialize)]
struct IntrospectBody {
    // lum.id wraps in {"data": {...}, "ret_code": 0}; also accept a bare object.
    data: Option<IntrospectData>,
    #[serde(default)]
    sub: Option<String>,
    #[serde(default)]
    role: Option<String>,
    #[serde(default)]
    email: Option<String>,
    #[serde(default)]
    active: bool,
    #[serde(default)]
    scopes: Option<Vec<String>>,
}

pub struct LumidClient {
    client: reqwest::Client,
    base_url: String,
    enabled: bool,
    // Cache value: Some(identity) on success, None negative-cached.
    cache: Cache<String, Option<Identity>>,
}

impl LumidClient {
    pub fn new(s: &Settings) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(s.lumid_timeout_s.max(1)))
            .user_agent("lumid-data-service/0.1")
            .build()
            .expect("reqwest client");
        let cache = Cache::builder()
            .max_capacity(4096)
            .time_to_live(Duration::from_secs(s.lumid_cache_ttl_s.max(1)))
            .build();
        LumidClient {
            client,
            base_url: s.lumid_url.trim_end_matches('/').to_string(),
            enabled: s.lumid_enabled,
            cache,
        }
    }

    fn looks_like_token(token: &str) -> bool {
        if token.is_empty() {
            return false;
        }
        if token.starts_with("lm_pat_live_") || token.starts_with("rm_pat_live_") {
            return true;
        }
        let parts: Vec<&str> = token.split('.').collect();
        parts.len() == 3 && parts.iter().all(|p| !p.is_empty())
    }

    fn hash(token: &str) -> String {
        let mut h = Sha256::new();
        h.update(token.as_bytes());
        format!("{:x}", h.finalize())
    }

    /// Ok(Some) authed, Ok(None) rejected, Err(Unreachable) → caller returns 503.
    pub async fn introspect(&self, token: &str) -> Result<Option<Identity>, LumidError> {
        if !self.enabled || !Self::looks_like_token(token) {
            return Ok(None);
        }
        let key = Self::hash(token);
        if let Some(cached) = self.cache.get(&key).await {
            return Ok(cached);
        }

        let url = format!("{}/api/v1/identity/introspect", self.base_url);
        let resp = self
            .client
            .post(&url)
            .json(&serde_json::json!({ "token": token }))
            .send()
            .await
            .map_err(|e| LumidError::Unreachable(e.to_string()))?;

        if resp.status() != reqwest::StatusCode::OK {
            self.cache.insert(key, None).await;
            return Ok(None);
        }
        let body: IntrospectBody = match resp.json().await {
            Ok(b) => b,
            Err(_) => return Ok(None),
        };
        let (sub, role, email, active, scopes) = match body.data {
            Some(d) => (d.sub, d.role, d.email, d.active, d.scopes),
            None => (body.sub, body.role, body.email, body.active, body.scopes),
        };
        if !active {
            self.cache.insert(key, None).await;
            return Ok(None);
        }
        let ident = Identity {
            sub: sub.unwrap_or_default(),
            role: role.unwrap_or_else(|| "user".to_string()),
            email,
            active: true,
            scopes: scopes.unwrap_or_default(),
        };
        self.cache.insert(key, Some(ident.clone())).await;
        Ok(Some(ident))
    }
}
