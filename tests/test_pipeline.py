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
