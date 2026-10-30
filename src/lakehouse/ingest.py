"""Kafka -> Postgres + Parquet.

One loop, one transaction per batch:

1. decode (writer schema -> latest) and apply the contract; failures go to
   ``events_quarantine`` with the reason and the raw bytes;
2. dedup inside the batch, then ``INSERT .. ON CONFLICT (event_id) DO
   NOTHING RETURNING event_id`` dedups against everything already loaded;
3. the batch row, the events, the quarantine rows and the next offset per
   partition commit together, so a crash replays the batch from the stored
   offsets and nothing is applied twice;
4. after the commit the batch's new rows are written to one Parquet file
   (tmp + rename) and the file name is recorded on the batch. A batch
   committed without a file (crash in between) is rewritten from Postgres
   at startup.
"""

import datetime as dt
import os
import time
from importlib.resources import files
from pathlib import Path

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, KafkaException, TopicPartition

from . import codec, contract

PARQUET_SCHEMA = pa.schema([
    ("event_id", pa.string()),
    ("event_type", pa.string()),
    ("user_id", pa.int64()),
    ("occurred_at", pa.timestamp("ms", tz="UTC")),
    ("amount_cents", pa.int64()),
    ("currency", pa.string()),
    ("session_id", pa.string()),
    ("schema_version", pa.int16()),
    ("batch_id", pa.int64()),
    ("ingested_at", pa.timestamp("us", tz="UTC")),
])

STAGE = """
CREATE TEMP TABLE IF NOT EXISTS stage (
    event_id text, event_type text, user_id int8, occurred_ms int8, amount_cents int8,
    currency text, session_id text, schema_version int2)
"""
STAGE_TYPES = ["text", "text", "int8", "int8", "int8", "text", "text", "int2"]

INSERT_EVENTS = """
INSERT INTO events (event_id, event_type, user_id, occurred_at, amount_cents, currency,
                    session_id, schema_version, batch_id, ingested_at)
SELECT event_id::uuid, event_type, user_id, 'epoch'::timestamptz + occurred_ms * interval '1 millisecond',
       amount_cents, currency, session_id, schema_version, %s, %s
FROM stage
ON CONFLICT (event_id) DO NOTHING
RETURNING event_id::text
"""


def apply_schema(conn: psycopg.Connection) -> None:
    conn.execute("SELECT pg_advisory_xact_lock(7012)")  # concurrent CREATE IF NOT EXISTS can still collide
    conn.execute(files("lakehouse").joinpath("schema.sql").read_text())
    conn.commit()


def write_parquet(lake: Path, batch_id: int, ingested_at: dt.datetime, rows: list[tuple]) -> str:
    """rows: (event_id, event_type, user_id, occurred_at_ms, amount_cents, currency, session_id, schema_version)."""
    cols = [list(c) for c in zip(*rows)]
    n = len(rows)
    table = pa.Table.from_arrays(
        [pa.array(c, type=f.type) for c, f in zip(cols, PARQUET_SCHEMA)]
        + [pa.array([batch_id] * n, pa.int64()), pa.array([ingested_at] * n, PARQUET_SCHEMA.field("ingested_at").type)],
        schema=PARQUET_SCHEMA,
    )
    rel = f"events/ingest_date={ingested_at:%Y-%m-%d}/batch-{batch_id:08d}.parquet"
    path = lake / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    pq.write_table(table, tmp, compression="zstd")
    os.replace(tmp, path)
    return rel


def recover_parquet(conn: psycopg.Connection, lake: Path) -> int:
    """Writes the Parquet file of every committed batch that has none."""
    todo = conn.execute(
        "SELECT batch_id, ingested_at FROM ingest_batches WHERE parquet_file IS NULL AND inserted > 0 ORDER BY 1"
    ).fetchall()
    for batch_id, ingested_at in todo:
        rows = conn.execute(
            """SELECT event_id::text, event_type, user_id, (extract(epoch FROM occurred_at) * 1000)::int8,
                      amount_cents, currency, session_id, schema_version
               FROM events WHERE batch_id = %s""",
            [batch_id],
        ).fetchall()
        rel = write_parquet(lake, batch_id, ingested_at, rows)
        conn.execute("UPDATE ingest_batches SET parquet_file = %s WHERE batch_id = %s", [rel, batch_id])
        conn.commit()
    return len(todo)


class Ingest:
    def __init__(self, conn: psycopg.Connection, lake: Path, topic: str):
        self.conn, self.lake, self.topic = conn, lake, topic
        self.stats = {"messages": 0, "inserted": 0, "duplicates": 0, "quarantined": 0, "batches": 0}

    def flush(self, msgs: list) -> None:
        ingested_at = dt.datetime.now(dt.UTC)
        now_ms = int(ingested_at.timestamp() * 1000)
        rows: dict[str, tuple] = {}
        quarantine = []
        next_offsets: dict[int, int] = {}
        dups = 0
        for m in msgs:
            next_offsets[m.partition()] = m.offset() + 1
            raw = m.value() or b""
            try:
                version, e = codec.decode(raw)
                reason = contract.violation(e, now_ms)
            except codec.DecodeError as err:
                reason = err.reason
            if reason:
                quarantine.append((self.topic, m.partition(), m.offset(), reason, raw))
            elif e["event_id"] in rows:
                dups += 1
            else:
                rows[e["event_id"]] = (e["event_id"], e["event_type"], e["user_id"], e["occurred_at"],
                                       e["amount_cents"], e["currency"], e["session_id"], version)

        with self.conn.transaction():
            cur = self.conn.cursor()
            batch_id = cur.execute(
                "INSERT INTO ingest_batches (ingested_at, messages, inserted, duplicates, quarantined)"
                " VALUES (%s, %s, 0, 0, %s) RETURNING batch_id",
                [ingested_at, len(msgs), len(quarantine)],
            ).fetchone()[0]
            inserted: set[str] = set()
            if rows:
                cur.execute(STAGE)
                cur.execute("TRUNCATE stage")
                with cur.copy("COPY stage FROM STDIN (FORMAT BINARY)") as copy:
                    copy.set_types(STAGE_TYPES)
                    for r in rows.values():
                        copy.write_row(r)
                cur.execute(INSERT_EVENTS, [batch_id, ingested_at])
                inserted = {r[0] for r in cur.fetchall()}
            dups += len(rows) - len(inserted)
            cur.execute("UPDATE ingest_batches SET inserted = %s, duplicates = %s WHERE batch_id = %s",
                        [len(inserted), dups, batch_id])
            if quarantine:
                cur.executemany(
                    'INSERT INTO events_quarantine (topic, partition, "offset", reason, payload, batch_id)'
                    " VALUES (%s, %s, %s, %s, %s, " + str(batch_id) + ") ON CONFLICT DO NOTHING",
                    quarantine,
                )
            cur.executemany(
                "INSERT INTO ingest_offsets (topic, partition, next_offset) VALUES (%s, %s, %s)"
                " ON CONFLICT (topic, partition) DO UPDATE SET next_offset = excluded.next_offset",
                [(self.topic, p, o) for p, o in next_offsets.items()],
            )
        if os.environ.get("LAKEHOUSE_CRASH_AFTER_COMMIT"):  # failpoint for the recovery test
            os._exit(3)

        if inserted:
            rel = write_parquet(self.lake, batch_id, ingested_at, [r for k, r in rows.items() if k in inserted])
            self.conn.execute("UPDATE ingest_batches SET parquet_file = %s WHERE batch_id = %s", [rel, batch_id])
            self.conn.commit()

        s = self.stats
        s["messages"] += len(msgs)
        s["inserted"] += len(inserted)
        s["duplicates"] += dups
        s["quarantined"] += len(quarantine)
        s["batches"] += 1


def run(bootstrap: str, dsn: str, lake: Path, topic: str, batch_size: int, max_wait: float,
        stop_at_eof: bool, partitions: list[int] | None = None) -> dict:
    """Consumes ``partitions`` (default: all). Several processes with disjoint
    partition sets can run side by side: offsets are per partition and the
    dedup is the primary key."""
    conn = psycopg.connect(dsn)
    apply_schema(conn)
    recovered = recover_parquet(conn, lake)

    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": "lakehouse-ingest",  # required by librdkafka; offsets are kept in Postgres, not here
        "enable.auto.commit": False,
        "enable.partition.eof": True,
    })
    parts = partitions or sorted(consumer.list_topics(topic, timeout=10).topics[topic].partitions)
    stored = dict(conn.execute("SELECT partition, next_offset FROM ingest_offsets WHERE topic = %s", [topic]).fetchall())
    conn.commit()
    consumer.assign([TopicPartition(topic, p, stored.get(p, OFFSET_BEGINNING)) for p in parts])

    ing = Ingest(conn, lake, topic)
    buf: list = []
    eof: set[int] = set()
    first = last = deadline = None
    while True:
        for m in consumer.consume(num_messages=min(10_000, batch_size), timeout=0.2):
            if m.error():
                if m.error().code() == KafkaError._PARTITION_EOF:
                    eof.add(m.partition())
                    continue
                raise KafkaException(m.error())
            if not buf:
                deadline = time.monotonic() + max_wait
            first = first or time.monotonic()
            eof.discard(m.partition())
            buf.append(m)
        caught_up = len(eof) == len(parts)
        if buf and (len(buf) >= batch_size or caught_up or time.monotonic() >= deadline):
            ing.flush(buf)
            buf = []
            last = time.monotonic()
        elif caught_up and stop_at_eof:
            break
    consumer.close()
    conn.close()
    secs = last - first if first else 0.0  # first message received -> last batch durable
    out = dict(ing.stats, recovered_batches=recovered, seconds=round(secs, 3),
               first_message_at=first and time.time() - (time.monotonic() - first),
               last_commit_at=last and time.time() - (time.monotonic() - last))
    if secs:
        out["messages_per_s"] = round(out["messages"] / secs)
        out["inserted_per_s"] = round(out["inserted"] / secs)
    return out

