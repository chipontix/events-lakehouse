"""The table-level checks (dbt tests + source freshness) catch bad rows that
reach the lake without passing the ingest's contract. No Kafka or Postgres
needed: the lake is written directly."""

import datetime as dt
import json
import uuid

from conftest import dbt, lake_env, utcnow
from lakehouse.generator import Spec, generate
from lakehouse.ingest import write_parquet


def _clean_rows(n=3_000):
    return [(m.event["event_id"], m.event["event_type"], m.event["user_id"], m.event["occurred_at"],
             m.event["amount_cents"], m.event["currency"], m.event["session_id"], m.value[4])
            for m in generate(Spec(n=n)) if m.kind == "event"]


def _row(**kw):
    base = dict(event_id=str(uuid.uuid4()), event_type="purchase", user_id=1, occurred_at=1_790_812_800_000,
                amount_cents=500, currency="USD", session_id=None, schema_version=2)
    return tuple((base | kw).values())


def _test_results(tmp):
    """{test name without its argument suffix: (status, failures)}"""
    res = json.loads((tmp / "target" / "run_results.json").read_text())["results"]
    return {r["unique_id"].split(".")[2].split("__")[0]: (r["status"], r["failures"]) for r in res}


def test_clean_lake_passes(tmp_path):
    env = lake_env(tmp_path)
    write_parquet(tmp_path / "lake", 1, utcnow(), _clean_rows())
    assert dbt(tmp_path, env, "run").returncode == 0
    out = dbt(tmp_path, env, "test")
    assert out.returncode == 0, out.stdout
    assert {s for s, _ in _test_results(tmp_path).values()} == {"pass"}


def test_mixed_in_bad_rows_fail_exactly_their_checks(tmp_path):
    env = lake_env(tmp_path)
    clean = _clean_rows()
    write_parquet(tmp_path / "lake", 1, utcnow(), clean)
    bad = [
        clean[0],                                   # the same event_id again
        _row(event_id=None),
        _row(event_type="clicked", amount_cents=None),
        _row(user_id=None),
        _row(user_id=0),
        _row(occurred_at=4_102_444_800_000),        # 2100
        _row(occurred_at=1_420_070_400_000),        # 2015
        _row(amount_cents=0),
        _row(amount_cents=None),
        _row(currency=None),
    ]
    write_parquet(tmp_path / "lake", 2, utcnow(), bad)
    assert dbt(tmp_path, env, "run").returncode == 0
    assert dbt(tmp_path, env, "test").returncode == 1

    failed = {k: f for k, (s, f) in _test_results(tmp_path).items() if s == "fail"}
    assert failed == {
        "unique_stg_events_event_id": 1,
        "unique_fct_events_event_id": 1,
        "not_null_stg_events_event_id": 1,
        "accepted_values_stg_events_event_type": 1,
        "not_null_stg_events_user_id": 1,
        "in_range_stg_events_user_id": 1,
        "in_range_stg_events_occurred_at": 2,
        "in_range_stg_events_amount_cents": 1,
        "not_null_stg_events_amount_cents": 1,
        "not_null_stg_events_currency": 1,
    }


def test_freshness(tmp_path):
    env = lake_env(tmp_path)
    write_parquet(tmp_path / "lake", 1, utcnow() - dt.timedelta(hours=25), _clean_rows(500))
    stale = dbt(tmp_path, env, "source", "freshness")
    assert stale.returncode == 1 and "ERROR STALE" in stale.stdout

    write_parquet(tmp_path / "lake", 2, utcnow(), _clean_rows(500))
    assert dbt(tmp_path, env, "source", "freshness").returncode == 0
