-- Read-only role for the retrieval / data-agent SQL surface.
--
-- Runs ONCE when ./data/pgdata is first created (mounted after 10_schema.sql).
--
-- WHY: the app uses ONE connection pool for reads, retrieval, /ingest writes,
-- and admin DDL — so the pool's login role needs write+DDL (and in many setups
-- is a superuser). But `POST /retrieve` and the data agent execute caller-shaped
-- SELECTs. A SELECT-only parser + READ ONLY txn block *writes*, not *reads*: a
-- superuser SELECT can still call pg_read_file()/pg_ls_dir() (host files) or read
-- pg_authid (credential hashes). To close that, set LUMID_RETRIEVAL_DB_ROLE to
-- this role; the replayer issues `SET LOCAL ROLE` so retrieval SELECTs run with
-- ONLY these privileges, then reverts at txn end. The pool's normal role is
-- a member of it implicitly (a superuser can SET ROLE to any role; a non-super
-- login role must be GRANTed membership — see the optional GRANT at the bottom).

CREATE ROLE lumid_reader WITH
  NOLOGIN          -- never connected to directly; reached only via SET ROLE
  NOSUPERUSER      -- the whole point: drops pg_read_file() & friends
  NOCREATEDB
  NOCREATEROLE
  NOBYPASSRLS;

-- Read access to the DATA, never to per-user bookkeeping or secrets.
--
-- Do NOT use `GRANT pg_read_all_data TO lumid_reader` (this file did until
-- 2026-11-25). It derives from role membership, so no per-table REVOKE can
-- carve anything out of it, and every caller-shaped SELECT could read:
--   * provenance.runs           — every user's ingest runs (submitter, args)
--   * provenance.webhooks       — includes a `secret_plain` column
--   * raw.tenant_blobs          — every user's private-upload index
--   * provenance.retrieval_runs — who ran which retrieval
--   * pg_authid                 — password hashes
-- and a SQL deny-list cannot fix it: the reader may call
-- query_to_xml('<any sql>').
--
-- Instead: USAGE + SELECT on every non-system schema that exists now, the
-- sensitive tables revoked, and default privileges so tables the app creates
-- LATER (ingest proposals add schemas at runtime) stay readable. The app's own
-- boot DDL revokes each new sensitive table it creates (e.g.
-- handlers::retrieve::migrate) — default privileges would otherwise grant it.
DO $grant$
DECLARE s text;
BEGIN
  FOR s IN SELECT nspname FROM pg_namespace
            WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'sync')
              AND nspname NOT LIKE 'pg_toast%' AND nspname NOT LIKE 'pg_temp%'
  LOOP
    EXECUTE format('GRANT USAGE ON SCHEMA %I TO lumid_reader', s);
    EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO lumid_reader', s);
  END LOOP;
END
$grant$;

DO $revoke$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY['provenance.runs', 'provenance.webhooks',
                           'provenance.ingress_proposals', 'provenance.ingress_acl',
                           'provenance.retrieval_runs', 'raw.tenant_blobs']
  LOOP
    IF to_regclass(t) IS NOT NULL THEN
      EXECUTE format('REVOKE ALL ON %s FROM lumid_reader', t);
    END IF;
  END LOOP;
END
$revoke$;

-- Objects the app creates later (it connects as postgres; change FOR ROLE if not).
ALTER DEFAULT PRIVILEGES FOR ROLE postgres GRANT USAGE ON SCHEMAS TO lumid_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres GRANT SELECT ON TABLES TO lumid_reader;

-- ── If the app pool's login role is NOT a superuser ───────────────────────────
-- It must be a member of lumid_reader to `SET ROLE` into it. Uncomment and set
-- the role name the app connects as (LUMID_DB_USER):
--
--   GRANT lumid_reader TO postgres;
