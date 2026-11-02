"""End to end against real Kafka and Postgres (``make test`` runs this in the
app container on the compose network). Each test gets its own topic,
database, lake directory and warehouse file."""

import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

import psycopg
import pytest
from confluent_kafka.admin import AdminClient

from conftest import dbt, lake_env
from lakehouse import produce, reference, verify
from lakehouse.generator import Spec

pytestmark = pytest.mark.integration

N = int(os.environ.get("LAKEHOUSE_N", 50_000))
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:16092")
BASE_DSN = os.environ.get("PG_DSN", "postgresql://lakehouse:lakehouse@localhost:16432/lakehouse")


@pytest.fixture
def env(tmp_path):
    name = "t_" + uuid.uuid4().hex[:12]
    with psycopg.connect(BASE_DSN, autocommit=True) as c:
        c.execute(f"CREATE DATABASE {name}")
    e = lake_env(tmp_path) | {"PG_DSN": BASE_DSN.rsplit("/", 1)[0] + "/" + name, "TOPIC": name}
    produce.ensure_topic(BOOTSTRAP, name, 6)
    yield e
    AdminClient({"bootstrap.servers": BOOTSTRAP}).delete_topics([name])
    with psycopg.connect(BASE_DSN, autocommit=True) as c:
        c.execute(f"DROP DATABASE {name} WITH (FORCE)")


def ingest(env, *args, check=True) -> list[dict]:
    """Runs one ingest process per partition set, in parallel, until drained."""
    sets = [a for a in args if a.startswith("--partitions=")] or [""]
    flags = [a for a in args if not a.startswith("--partitions=")]
    procs = [subprocess.Popen([sys.executable, "-m", "lakehouse", "ingest", "--stop-at-eof", *flags, *filter(None, [s])],
                              env=env, stdout=subprocess.PIPE, text=True) for s in sets]
    outs = [(p.communicate()[0], p.returncode) for p in procs]
    if check:
        assert all(rc == 0 for _, rc in outs), outs
    return [json.loads(o) for o, rc in outs if rc == 0]


def send(env, start, stop, n=N):
    produce.run(BOOTSTRAP, env["TOPIC"], Spec(n=n), start, stop)


def test_two_phases_with_late_events_match_the_reference(env, tmp_path):
    """Half the stream, dbt, the other half (with late events for dates that
    are already aggregated), dbt again: Postgres, the Parquet lake and every
    dbt model equal the independently computed reference."""
    workers = ("--partitions=0,1,2", "--partitions=3,4,5")
    send(env, 0, N // 2)
    ingest(env, *workers)
    assert dbt(tmp_path, env, "run").returncode == 0
    send(env, N // 2, N)
    ingest(env, *workers)
    assert dbt(tmp_path, env, "run").returncode == 0
    out = dbt(tmp_path, env, "test")
    assert out.returncode == 0, out.stdout

    problems = verify.compare(reference.expected(Spec(n=N)), env["PG_DSN"], Path(env["LAKE_DIR"]),
                              Path(env["WAREHOUSE"]))
    assert problems == []


def test_crash_between_commit_and_parquet_is_repaired(env, tmp_path):
    n = 20_000
    send(env, 0, None, n)
    crashed = subprocess.run([sys.executable, "-m", "lakehouse", "ingest", "--stop-at-eof", "--batch-size=5000"],
                             env=env | {"LAKEHOUSE_CRASH_AFTER_COMMIT": "1"})
    assert crashed.returncode == 3
    with psycopg.connect(env["PG_DSN"]) as c:
        assert c.execute("SELECT count(*), count(parquet_file) FROM ingest_batches").fetchone() == (1, 0)
    assert not list(Path(env["LAKE_DIR"]).glob("events/*/*.parquet"))

    [stats] = ingest(env, "--batch-size=5000")
    assert stats["recovered_batches"] == 1
    assert dbt(tmp_path, env, "run").returncode == 0
    assert verify.compare(reference.expected(Spec(n=n)), env["PG_DSN"], Path(env["LAKE_DIR"]),
                          Path(env["WAREHOUSE"])) == []


def test_replaying_the_topic_adds_nothing(env):
    n = 10_000
    send(env, 0, None, n)
    [first] = ingest(env)
    with psycopg.connect(env["PG_DSN"]) as c:
        c.execute("DELETE FROM ingest_offsets")  # forget the position: next run re-reads everything
    [again] = ingest(env)
    assert again["messages"] == n and again["inserted"] == 0
    assert again["duplicates"] == first["inserted"] + first["duplicates"]
    pg = verify.from_postgres(env["PG_DSN"])
    want = reference.expected(Spec(n=n))
    assert (pg.events, pg.digest) == (want.events, want.digest)


def test_quarantine_replay_loads_what_passes_now(env):
    """A purchase from a producer whose clock ran an hour ahead is quarantined;
    once that hour has passed, replay loads it as its own batch with a Parquet
    file and removes it from the quarantine. A record that is still broken
    stays. A second replay changes nothing."""
    import datetime as dt
    from lakehouse import codec, ingest as ing

    now = int(dt.datetime.now(dt.UTC).timestamp() * 1000)
    ev = {"event_id": str(uuid.uuid4()), "event_type": "purchase", "user_id": 7, "occurred_at": now + 3_600_000,
          "amount_cents": 1999, "currency": "EUR", "session_id": None}
    early = codec.encode(ev, codec.LATEST)
    broken = codec.encode(ev | {"event_id": str(uuid.uuid4()), "currency": "eur"}, codec.LATEST)
    lake = Path(env["LAKE_DIR"])
    with psycopg.connect(env["PG_DSN"]) as c:
        ing.apply_schema(c)
        for off, raw in enumerate((early, broken)):
            assert ing.check(raw, now)[0] in ("occurred_at_out_of_range", "bad_currency")
            c.execute('INSERT INTO events_quarantine (topic, partition, "offset", reason, payload, batch_id)'
                      " VALUES (%s, 0, %s, %s, %s, 0)", [env["TOPIC"], off, ing.check(raw, now)[0], raw])
        c.commit()

        assert ing.replay_quarantine(c, lake, now_ms=now)["released"] == 0  # too early: nothing moves
        later = now + 2 * 3_600_000
        out = ing.replay_quarantine(c, lake, now_ms=later)
        assert (out["checked"], out["released"], out["inserted"]) == (2, 1, 1)
        assert c.execute("SELECT reason FROM events_quarantine").fetchall() == [("bad_currency",)]
        assert c.execute("SELECT currency, batch_id FROM events WHERE event_id = %s",
                         [ev["event_id"]]).fetchone() == ("EUR", out["batch_id"])
        rel = c.execute("SELECT parquet_file FROM ingest_batches WHERE batch_id = %s", [out["batch_id"]]).fetchone()[0]
        assert pq_rows(lake / rel) == 1

        again = ing.replay_quarantine(c, lake, now_ms=later)
        assert (again["checked"], again["released"]) == (1, 0)
        assert c.execute("SELECT count(*) FROM events").fetchone()[0] == 1


def pq_rows(path: Path) -> int:
    import pyarrow.parquet as pq
    return pq.read_metadata(path).num_rows
