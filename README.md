# events-lakehouse

[![ci](https://github.com/chipontix/events-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/chipontix/events-lakehouse/actions/workflows/ci.yml)

A clickstream pipeline **Kafka → Postgres + Parquet → dbt**: exactly-once
loading with dedup by event id, an Avro schema in two versions read through
one reader schema, a row-level data contract with a quarantine table,
table-level checks in dbt, and incremental dbt models that stay correct when
events arrive days late. A deterministic generator produces 1M messages with
duplicates, late events and bad records; the outputs are compared with a
reference computed independently from the generator's ground truth.

```bash
make test     # 38 tests: unit, contract, dbt checks, end to end against Kafka + Postgres (Docker only)
make demo     # 100k messages through the pipeline, then verify against the reference
make bench    # 1M messages, 1 and 3 ingest workers, 3 runs each -> results/bench.jsonl
make test-1m  # the two-phase end-to-end comparison at 1M messages
make down     # remove this project's containers and volumes
```

Needs Docker with Compose v2 and `make`. Everything, tests included, runs in
an `app` container on the compose network (compose project `lakehouse`, host
ports 16092 for Kafka and 16432 for Postgres, overridable with `KAFKA_PORT`,
`PG_PORT`). `make unit` runs the tests that need no brokers on the host with
[uv](https://docs.astral.sh/uv/).

## What is proven, and how

| claim | evidence |
|---|---|
| Every output equals the independently computed expectation | `test_two_phases_with_late_events_match_the_reference`: half the stream, `dbt run`, the other half, `dbt run`. Postgres `events`, the Parquet lake and `fct_events` match the reference row for row (count + order-independent digest of all columns); `agg_daily_events` and `daily_revenue` match key for key; quarantine counts per reason and the duplicate count match. Runs at 50k in `make test` and at 1M in `make test-1m` / every bench run |
| …at 1M messages with duplicates and late events | 1,000,000 messages → 944,774 events, 50,250 duplicates dropped, 4,976 quarantined; equal to the reference in all 6 bench runs ([`results/bench.jsonl`](results/bench.jsonl)) and in `make test-1m` ([`results/raw/test-1m-*.log`](results/raw)) |
| The reference is independent | `reference.py` reads only the generator's labels (event / duplicate / bad + reason, the canonical event). No Avro, Kafka, contract code or SQL |
| Late events land in already aggregated days and the aggregates are still right | the second phase carries late events for dates the first `dbt run` already built (`test_late_events_reach_dates_the_first_half_already_covered` guards that the test really exercises this) |
| The row contract catches every injected bad record, and nothing else | `test_injected_bad_records_are_caught_with_the_right_reason`: 200k messages with 2% bad records of 8 kinds; 0 missed, 0 false alarms, each with the expected reason; plus 17 rule edge cases |
| Table-level checks catch bad rows that bypass the contract | `test_mixed_in_bad_rows_fail_exactly_their_checks`: 10 rogue rows written straight into the lake fail exactly the 10 expected dbt tests with the expected failure counts; a clean lake passes all 13; a 25-hour-old lake fails `dbt source freshness` |
| Old data stays readable | v1 messages decode into the v2 shape (renamed fields via aliases, widened type, defaults); a CI test resolves every registered schema to the latest and shows that a breaking v3 is rejected |
| A crash between the Postgres commit and the Parquet write loses nothing | `test_crash_between_commit_and_parquet_is_repaired`: a failpoint kills the process right after the first commit; the next start rewrites the missing file from Postgres and the outputs match the reference |
| Replays are no-ops | `test_replaying_the_topic_adds_nothing`: the stored offsets are deleted and the whole topic is read again; 0 rows inserted, all counted as duplicates |

## Numbers

All from real runs on 2026-10-30/31; raw output in [`results/`](results/)
(`bench.jsonl`: one JSON line per run with every phase and per-worker
figure; `raw/`: the console output). Reproduce with `make bench` (or
`scripts/bench.sh WORKERS RUNS`). Each run starts from an empty topic,
database and lake and sends the same 1,000,000 seeded messages
(944,774 unique valid events, 50,250 duplicates, 4,976 bad records).

| 1M messages, 3 runs each | 1 ingest worker | 3 ingest workers (2 partitions each) |
|---|---|---|
| produce: generate + Avro-encode + send, one process | 80,994–89,424 msg/s (11.2–12.3 s) | 75,713–87,202 msg/s (11.5–13.2 s) |
| **ingest, in**: messages consumed | **42,045–45,633 msg/s** | **48,441–68,179 msg/s** |
| **ingest, out**: new rows in Postgres + Parquet | **39,723–43,113 rows/s** | **45,766–64,414 rows/s** |
| ingest time for 1M | 21.9–23.8 s | 14.7–20.6 s |
| `dbt run` (4 models, first build) | 9.0–11.3 s | 8.7–15.1 s |
| **full run: produce + ingest + `dbt run`** | **44.6–47.3 s** | **36.2–50.5 s** |
| `dbt test` (13 tests, all pass) | 4.2–7.4 s | 3.2–6.4 s |
| verify vs reference (Postgres, Parquet, dbt) | match, 3/3 | match, 3/3 |

"Ingest time" runs from the first message any worker receives to the last
batch being committed with its Parquet file in place. Batches of up to 50,000
messages. `make test-1m` (two phases, two workers, `dbt run` twice, `dbt
test`, verify) passed in 67.3 s.

**Hardware.** Apple M4 Max (16 cores, 48 GiB). Kafka, Postgres and the
pipeline run in one colima VM with 4 vCPU and 5.8 GiB, **shared with other
projects' containers: 11–14 containers were running during the runs**, and
the host's 1-minute load average was 35.9–49.3 before the runs (16 cores),
the VM's 9.7–13.7 after them (4 vCPU). Container limits: Kafka 1 GiB
(512 MiB heap), Postgres 1 GiB (`shared_buffers=256MB`,
`synchronous_commit=on`), app 2 GiB. Kafka 3.9.1 single broker in KRaft mode,
6 partitions, producer with idempotence (acks=all) and lz4; Postgres 17.

**Reading it.** One worker is steady at 42–45.6k msg/s. Three workers came out
6–49% above the best single-worker run, with a wide spread: they share 4 vCPU with Kafka, Postgres and
the other projects, so the extra processes mostly compete for the same cores.
In a profile of one worker over 100k messages
([`results/raw/profile-ingest-100k.txt`](results/raw/profile-ingest-100k.txt))
the largest item is waiting on Postgres (2.1 of 6.4 s: the batch's
statements and commits), then Avro decoding (0.9 s) and the contract (0.7 s). Expect higher and tighter numbers on dedicated hardware.

## Architecture

```
 generator ──► Kafka topic "events" (6 partitions, key = user_id)
 (seeded)        │  value = 0x00 | schema id | Avro body      (v1 and v2 writers)
                 ▼
            ingest worker(s), one transaction per batch of up to 50k messages
              1. decode with the writer schema, resolve to the latest (v2)
              2. row contract ──► failures: events_quarantine (reason + raw bytes)
              3. dedup in the batch, then INSERT .. ON CONFLICT (event_id) DO NOTHING
              4. same transaction: ingest_batches row + next offset per partition
              5. after commit: new rows ──► lake/events/ingest_date=D/batch-N.parquet
                 (tmp + rename; a committed batch without its file is rebuilt at startup)
                 │                                   │
                 ▼                                   ▼
          Postgres 17 (serving copy,          Parquet lake ──► dbt (DuckDB)
          dedup authority, offsets)                 stg_events (view)
                                                    fct_events (incremental, new batches)
                                                    agg_daily_events, daily_revenue
                                                      (incremental, recompute touched days)
                                                    + 13 data tests, source freshness
```

| piece | file | |
|---|---|---|
| generator | [`generator.py`](src/lakehouse/generator.py) | Seeded stream; ground-truth label on every message |
| schemas | [`schemas/`](src/lakehouse/schemas), [`codec.py`](src/lakehouse/codec.py) | File-based registry, Confluent-style framing, writer → latest resolution |
| contract | [`contract.py`](src/lakehouse/contract.py) | Row rules, first broken rule = quarantine reason |
| ingest | [`ingest.py`](src/lakehouse/ingest.py), [`schema.sql`](src/lakehouse/schema.sql) | Batch transaction, dedup, offsets in Postgres, Parquet writer and repair |
| dbt | [`dbt/`](dbt) | Models, generic tests `in_range` and `unique_combination` |
| reference / verify | [`reference.py`](src/lakehouse/reference.py), [`verify.py`](src/lakehouse/verify.py) | Expected result; reads all three outputs and diffs |

### Schema versions

| field | v1 (legacy tracker) | v2 (current, the reader schema) |
|---|---|---|
| user | `user: long?` | `user_id: long?`, alias `user` |
| time | `ts: long` (epoch ms) | `occurred_at: long`, alias `ts` |
| amount | `amount_cents: int?` | `amount_cents: long?` (int → long promotion) |
| currency | — | `currency: string`, default `"USD"` |
| session | — | `session_id: string?`, default `null` |

30% of the generated stream is written as v1. A v1 message is decoded with the
v1 schema and resolved to v2, so everything downstream sees one shape; the
original version is kept in `schema_version`.

### Data quality

| check | where | on failure |
|---|---|---|
| decodable Avro, known schema id | ingest | quarantine `undecodable` / `unknown_schema` |
| `event_id` is a UUID (normalized to lowercase text) | ingest | quarantine `bad_event_id` |
| `event_type` in page_view / add_to_cart / purchase | ingest; dbt `accepted_values` | quarantine; test fails |
| `user_id` not null and > 0 | ingest; dbt `not_null`, `in_range` | quarantine; test fails |
| `occurred_at` in [2020-01-01, ingest time + 5 min] | ingest; dbt `in_range` | quarantine; test fails |
| purchase: amount in [1, 10,000,000] cents; others: no amount | ingest; dbt `not_null` + `in_range` (where purchase) | quarantine; test fails |
| `currency` is three capital letters | ingest; dbt `not_null` | quarantine; test fails |
| `event_id` unique | Postgres primary key; dbt `unique` on `stg_events` and `fct_events` | dropped as duplicate; test fails |
| freshness: newest `ingested_at` | `dbt source freshness` | warn after 1 h, error after 24 h |

## Design decisions

- **Postgres is the dedup authority, not the consumer.** `ON CONFLICT
  (event_id) DO NOTHING` against a primary key is correct across batches,
  restarts and parallel workers; an in-memory set would be none of these.
  The Parquet file gets only the rows Postgres reported as new.
- **Offsets live in Postgres, in the batch's transaction.** Rows, quarantine,
  batch record and the next offset per partition commit together, so a crash
  replays the batch and the replay inserts nothing (exactly-once effect on an
  at-least-once source). Kafka consumer-group commits are not used.
- **Parquet is derived, and repairable.** The file is written after the
  commit, by tmp + rename, and its name is recorded on the batch. A batch
  committed without a file (crash in between) is rebuilt from Postgres at the
  next start. The reverse order (file first) would leave duplicate files on
  replay.
- **The lake is partitioned by ingest date, one file per batch.** Partitioning
  by event date would spread one batch over many directories (no atomic
  publish) and make every late event rewrite an old partition. Event time is
  handled in the models instead.
- **Incremental models track batches, not a high-water mark.** With several
  workers, batch ids commit out of order; `batch_id > max(batch_id)` would
  skip a batch that commits after a higher one was loaded. `fct_events` takes
  the batches it does not contain yet (anti-join).
- **Aggregates recompute touched days.** Distinct users cannot be merged
  from partial results, so every day that received rows in this run, however
  old, is rebuilt from `fct_events` and replaced (`delete+insert`). Late
  events cost the size of the days they touch, not a full rebuild.
- **Two quality layers.** The row contract keeps bad records out and keeps
  them, with the reason and raw bytes, in `events_quarantine` instead of
  failing the batch (one bad producer does not stop the topic). dbt tests
  check the tables, for whatever reaches the lake another way.
- **Avro with a file-based registry.** Avro's writer/reader resolution gives
  renames (aliases), type widening and defaults for free; the framing matches
  the Confluent serializers, so a real Schema Registry can replace the
  `schemas/` directory without changing the messages. Not running one saves a
  JVM in a 6 GiB VM (5.8 GiB visible).
- **The reference reads labels, not data.** It never parses what the
  pipeline parses, so a decoder or contract bug cannot cancel itself out in
  the comparison.
- **Python end to end.** Decoding is fastavro (C), Kafka is librdkafka, rows go
  to Postgres through `COPY` into a temp table; the measured ceiling is
  the Postgres insert, and the pipeline scales by running more workers over
  disjoint partitions (see Numbers).

## Tests

| file | needs | what |
|---|---|---|
| `test_generator.py` | — | determinism by seed; the promised mix of duplicates, bad records, late events, v1/v2 |
| `test_schema.py` | — | v1 → v2 resolution, decode failures, compatibility gate |
| `test_contract.py` | — | every injected bad record caught with its reason; rule edge cases |
| `test_dbt_checks.py` | dbt | rogue rows fail exactly their dbt tests; freshness |
| `test_pipeline.py` | Kafka, Postgres | two phases vs reference; crash repair; replay |

`make test` runs all 38 in the app container; [CI](.github/workflows/ci.yml)
runs the broker-less ones on the runner and `make test` in Docker.

## Running on macOS with colima

- If `docker pull` fails with `error getting credentials ... docker-credential-desktop: executable file not found`,
  the global `~/.docker/config.json` names a credential helper that is not
  installed. Use a separate config instead of editing the global one:

  ```bash
  mkdir -p ~/.docker-lakehouse
  echo "{\"auths\":{},\"currentContext\":\"colima\",\"cliPluginsExtraDirs\":[\"$HOME/.docker/cli-plugins\"]}" > ~/.docker-lakehouse/config.json
  ln -s ~/.docker/contexts ~/.docker-lakehouse/contexts   # keeps the colima context resolvable
  export DOCKER_CONFIG=~/.docker-lakehouse
  ```

- With colima's ssh port forwarder the published ports were not reachable
  from the host during development. Nothing here depends on them: tests and
  benchmarks run in the `app` container and talk to `kafka:19092` and
  `postgres:5432`.

## Limitations

- One broker, one Postgres, one laptop VM shared with other workloads.
  Absolute numbers are for this setup only.
- Small files: one Parquet file per batch. At 50k-message batches that is 20
  files per 1M messages; a stream with small batches would need compaction.
- Postgres keeps every event (it is also the dedup index). Retention would
  need a dedup window instead, e.g. partitioned by day and dropped after the
  maximum replay horizon.
- The quarantine is write-only: there is no replay tool for fixed records.
- The schema registry is a directory baked into the image; adding a version
  means a deploy. Producers are trusted to use a registered id.
- `fct_events`' batch anti-join scans the distinct batch ids of the table
  every run; fine for thousands of batches, not for millions.
- `dbt source freshness` checks the ingest time of the newest file, i.e. that
  the pipeline runs, not that producers are sending.

## Third-party software

| | license | used for |
|---|---|---|
| [confluent-kafka-python](https://github.com/confluentinc/confluent-kafka-python) / librdkafka | Apache-2.0 / BSD-2-Clause | producer, consumer, admin |
| [fastavro](https://github.com/fastavro/fastavro) | MIT | Avro encoding, schema resolution |
| [psycopg 3](https://github.com/psycopg/psycopg) | LGPL-3.0 | Postgres client |
| [pyarrow](https://github.com/apache/arrow) | Apache-2.0 | Parquet writer |
| [DuckDB](https://github.com/duckdb/duckdb) | MIT | warehouse engine, reading Parquet |
| [dbt-core](https://github.com/dbt-labs/dbt-core), [dbt-duckdb](https://github.com/duckdb/dbt-duckdb) | Apache-2.0 | models and data tests |
| [pytest](https://github.com/pytest-dev/pytest) | MIT | tests |
| Docker images `apache/kafka:3.9.1`, `postgres:17-alpine`, `python:3.12-slim` | Apache-2.0, PostgreSQL License, PSF | runtime |
| [uv](https://github.com/astral-sh/uv) | MIT / Apache-2.0 | dependency management, in the image |

## License

MIT, see [LICENSE](LICENSE).
