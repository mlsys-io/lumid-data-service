-- Your warehouse DDL. Runs ONCE when ./data/pgdata is first created.
-- Convention: every fact table carries the universal provenance columns the
-- ingest plane stamps (source, source_endpoint, source_run_id, ingest_ts) so
-- /ingest + lineage work out of the box.

CREATE SCHEMA IF NOT EXISTS app;

CREATE TABLE IF NOT EXISTS app.events (
  id          bigint        NOT NULL,
  ts          timestamptz   NOT NULL DEFAULT now(),
  kind        text          NOT NULL,
  payload     jsonb,
  source           text,
  source_endpoint  text,
  source_run_id    uuid,
  ingest_ts        timestamptz,
  PRIMARY KEY (id, ts)
);

-- Optional: make a time-series table a TimescaleDB hypertable + compression.
SELECT create_hypertable('app.events', 'ts', if_not_exists => TRUE);
ALTER TABLE app.events SET (timescaledb.compress, timescaledb.compress_segmentby = 'kind');
SELECT add_compression_policy('app.events', INTERVAL '30 days', if_not_exists => TRUE);
