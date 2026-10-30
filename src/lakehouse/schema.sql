CREATE TABLE IF NOT EXISTS ingest_batches (
    batch_id     bigserial PRIMARY KEY,
    ingested_at  timestamptz NOT NULL,
    messages     int NOT NULL,
    inserted     int NOT NULL,
    duplicates   int NOT NULL,
    quarantined  int NOT NULL,
    parquet_file text  -- NULL until the batch's Parquet file is in place
);

CREATE TABLE IF NOT EXISTS events (
    event_id       uuid PRIMARY KEY,  -- the dedup key
    event_type     text NOT NULL,
    user_id        bigint NOT NULL,
    occurred_at    timestamptz NOT NULL,
    amount_cents   bigint,
    currency       char(3) NOT NULL,
    session_id     text,
    schema_version smallint NOT NULL,
    batch_id       bigint NOT NULL,  -- no FK: written in the batch's own transaction; a per-row RI check is not free
    ingested_at    timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS events_batch_id ON events (batch_id);

CREATE TABLE IF NOT EXISTS events_quarantine (
    topic       text NOT NULL,
    partition   int NOT NULL,
    "offset"    bigint NOT NULL,
    reason      text NOT NULL,
    payload     bytea NOT NULL,
    batch_id    bigint NOT NULL,
    PRIMARY KEY (topic, partition, "offset")
);

-- Consumer position lives here, committed in the same transaction as the rows.
CREATE TABLE IF NOT EXISTS ingest_offsets (
    topic       text NOT NULL,
    partition   int NOT NULL,
    next_offset bigint NOT NULL,
    PRIMARY KEY (topic, partition)
);
