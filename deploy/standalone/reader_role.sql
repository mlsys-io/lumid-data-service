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

-- Broad read access without superuser. `pg_read_all_data` (PG14+) grants SELECT
-- on every table/view + USAGE on every schema, but NOT the server-side file
-- functions (pg_read_file, pg_ls_dir — those require superuser or
-- pg_read_server_files). This closes the host-file exfiltration vector.
--
-- KNOWN LIMITATION: pg_read_all_data also covers pg_catalog tables including
-- pg_authid (password hashes, stored as scram-sha-256). The REVOKE below has
-- no effect because the grant derives from role membership, not a table ACL.
-- If you need to exclude pg_authid, use the per-schema grant block below
-- instead of pg_read_all_data.
GRANT pg_read_all_data TO lumid_reader;

-- ── Stricter alternative: make LUMID_USER_SCHEMAS a real access boundary ───────
-- Replace the pg_read_all_data grant above with per-schema grants so the reader
-- can ONLY see the schemas you expose to the agent. Repeat per schema and add a
-- matching ALTER DEFAULT PRIVILEGES so future tables are covered:
--
--   GRANT USAGE ON SCHEMA app TO lumid_reader;
--   GRANT SELECT ON ALL TABLES IN SCHEMA app TO lumid_reader;
--   ALTER DEFAULT PRIVILEGES IN SCHEMA app GRANT SELECT ON TABLES TO lumid_reader;

-- ── If the app pool's login role is NOT a superuser ───────────────────────────
-- It must be a member of lumid_reader to `SET ROLE` into it. Uncomment and set
-- the role name the app connects as (LUMID_DB_USER):
--
--   GRANT lumid_reader TO postgres;
