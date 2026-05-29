"""Runtime configuration. All values read from env vars with sensible defaults
so the same image runs locally (with the host DB at 127.0.0.1:5433) and in
docker-compose (DB reachable by container name on findata-net)."""
from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class Settings:
    db_host: str = _env("FINDATA_DB_HOST", "127.0.0.1")
    db_port: int = _env_int("FINDATA_DB_PORT", 5433)
    db_user: str = _env("FINDATA_DB_USER", "postgres")
    db_password: str = _env("FINDATA_DB_PASSWORD", "")
    db_name: str = _env("FINDATA_DB_NAME", "fin_ai_world_model_v2")
    pool_min: int = _env_int("FINDATA_POOL_MIN", 2)
    pool_max: int = _env_int("FINDATA_POOL_MAX", 20)
    statement_timeout_ms: int = _env_int("FINDATA_STATEMENT_TIMEOUT_MS", 30000)
    # Per-checkout statement_timeout for the sync ingest pool. Set generously:
    # a legitimate large stream/file merge must not be killed mid-COPY, but a
    # wedged backend must not pin a connection forever. 0 disables the cap.
    ingest_statement_timeout_ms: int = _env_int("FINDATA_INGEST_STATEMENT_TIMEOUT_MS", 600000)
    ohlc_row_cap: int = _env_int("FINDATA_OHLC_ROW_CAP", 200_000)
    rate_limit_anon: str = _env("FINDATA_RATE_LIMIT_ANON", "60/minute")
    rate_limit_authed: str = _env("FINDATA_RATE_LIMIT_AUTHED", "600/minute")
    lumid_url: str = _env("FINDATA_LUMID_URL", "https://lum.id")
    lumid_enabled: bool = _env("FINDATA_LUMID_ENABLED", "true").lower() in ("1", "true", "yes")
    lumid_cache_ttl: int = _env_int("FINDATA_LUMID_CACHE_TTL", 300)
    lumid_timeout_s: float = float(_env("FINDATA_LUMID_TIMEOUT_S", "5"))

    # Realtime streaming
    redis_url: str = _env("FINDATA_REDIS_URL", "redis://finai-redis:6379/0")
    finnhub_key: str = _env("FINDATA_FINNHUB_KEY", "")
    fmp_key: str = _env("FINDATA_FMP_KEY", "")
    rt_tier_a_finnhub_cap: int = _env_int("FINDATA_RT_TIER_A_FINNHUB_CAP", 60)
    rt_tier_a_fmp_cap: int = _env_int("FINDATA_RT_TIER_A_FMP_CAP", 60)
    rt_tier_b_poll_sec: int = _env_int("FINDATA_RT_TIER_B_POLL_SEC", 5)
    rt_heartbeat_sec: int = _env_int("FINDATA_RT_HEARTBEAT_SEC", 30)
    rt_ws_lifetime_syms: int = _env_int("FINDATA_RT_WS_LIFETIME_SYMS", 500)
    rt_sse_request_syms: int = _env_int("FINDATA_RT_SSE_REQUEST_SYMS", 100)
    rt_slowclient_queue: int = _env_int("FINDATA_RT_SLOWCLIENT_QUEUE", 100)
    rt_news_poll_sec: int = _env_int("FINDATA_RT_NEWS_POLL_SEC", 60)
    twitterapi_key: str = _env("FINDATA_TWITTERAPI_KEY", "")
    rt_kol_poll_sec: int = _env_int("FINDATA_RT_KOL_POLL_SEC", 300)
    rt_kol_max_per_poll: int = _env_int("FINDATA_RT_KOL_MAX_PER_POLL", 20)
    # Twitter media mirror — local on-disk cache of pbs.twimg.com images
    # (populated by loaders.phase4.download_kol_media). Empty disables
    # the /kols/media/* serve mount.
    kol_media_root: str = _env("FINDATA_KOL_MEDIA_ROOT", "")

    # Ingress blob plane — bytes go to the lumid.data /storage/v1 endpoint
    # (sibling service, same org). Empty `lumid_data_base_url` disables the
    # blob plane (POST /ingest/blob + /ingest/.../file with image payloads
    # will return 503).
    # ----- Blob storage (single mounted folder) -----
    # The directory bind-mounted into the api container (RW). Bytes land
    # under <root>/<prefix>/sha256=<hex>. Empty disables the blob plane.
    blob_root: str = _env("FINDATA_BLOB_ROOT", "/app/blobs")
    # Public base URL used in storage_url returned to callers. Defaults to
    # the request's own origin; override when behind nginx/CDN.
    blob_public_base_url: str = _env("FINDATA_BLOB_PUBLIC_BASE_URL", "")
    blob_max_bytes: int = _env_int("FINDATA_BLOB_MAX_BYTES", 100 * 1024 * 1024)

    # Legacy — kept for one minor version so already-emitted lumid.data
    # URLs still resolve via the back-compat alias. New deployments can
    # leave these blank.
    lumid_data_base_url: str = _env("FINDATA_LUMID_DATA_BASE_URL", "")
    lumid_data_public_base_url: str = _env("FINDATA_LUMID_DATA_PUBLIC_BASE_URL", "")
    lumid_data_bucket: str = _env("FINDATA_LUMID_DATA_BUCKET", "findata")
    lumid_data_token: str = _env("FINDATA_LUMID_DATA_TOKEN", "")
    lumid_data_timeout_s: float = float(_env("FINDATA_LUMID_DATA_TIMEOUT_S", "60"))

    # Lumilake handoff — fire-and-forget POST /api/v1/jobs after each
    # successful ingest. Empty `lumilake_base_url` disables the hook
    # entirely (no network call, no log noise).
    lumilake_base_url: str = _env("FINDATA_LUMILAKE_BASE_URL", "")
    lumilake_workflow: str = _env("FINDATA_LUMILAKE_WORKFLOW", "findata-ingress-followup")
    lumilake_token: str = _env("FINDATA_LUMILAKE_TOKEN", "")
    lumilake_timeout_s: float = float(_env("FINDATA_LUMILAKE_TIMEOUT_S", "10"))

    # LLM serving — thin proxy in front of an upstream vLLM-compatible server.
    # Surfaces /v1/chat/completions, /v1/completions, /v1/models (OpenAI shape)
    # and /v1/messages, /v1/messages/count_tokens (Anthropic shape). Empty
    # `llm_backend_url` disables the routes (returns 503).
    llm_backend_url: str = _env("FINDATA_LLM_BACKEND_URL", "")
    llm_default_model: str = _env("FINDATA_LLM_DEFAULT_MODEL", "")
    llm_timeout_s: float = float(_env("FINDATA_LLM_TIMEOUT_S", "600"))
    llm_stream_idle_timeout_s: float = float(_env("FINDATA_LLM_STREAM_IDLE_TIMEOUT_S", "60"))


settings = Settings()
