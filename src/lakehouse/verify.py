"""Reads what the pipeline produced and compares it with ``reference.expected``."""

from pathlib import Path

import duckdb
import psycopg

from .reference import Result

COLS = "event_id, event_type, user_id, occurred_ms, amount_cents, currency, session_id"


def from_postgres(dsn: str) -> Result:
    res = Result()
    with psycopg.connect(dsn) as conn:
        with conn.cursor(name="verify") as cur:
            cur.itersize = 50_000
            cur.execute("""SELECT event_id::text, event_type, user_id,
                                  (extract(epoch FROM occurred_at) * 1000)::int8, amount_cents,
                                  currency, session_id FROM events""")
            for row in cur:
                res.add_row(*row)
        res.quarantine = dict(conn.execute("SELECT reason, count(*) FROM events_quarantine GROUP BY 1").fetchall())
        res.duplicates = conn.execute("SELECT coalesce(sum(duplicates), 0) FROM ingest_batches").fetchone()[0]
    return res


def _digest_rows(res: Result, rel: duckdb.DuckDBPyRelation) -> None:
    reader = rel.to_arrow_reader(100_000)
    for batch in reader:
        for row in zip(*(c.to_pylist() for c in batch.columns)):
            res.add_row(*row)


def from_lake(lake: Path) -> Result:
    res = Result()
    con = duckdb.connect()
    files = str(lake / "events" / "*" / "*.parquet")
    _digest_rows(res, con.sql(f"SELECT event_id, event_type, user_id, epoch_ms(occurred_at), amount_cents,"
                              f" currency, session_id FROM read_parquet('{files}')"))
    return res


def from_warehouse(path: Path) -> Result:
    """The dbt models: fct_events, agg_daily_events, daily_revenue."""
    res = Result()
    con = duckdb.connect(str(path), read_only=True)
    _digest_rows(res, con.sql("SELECT event_id, event_type, user_id, epoch_ms(occurred_at), amount_cents,"
                              " currency, session_id FROM fct_events"))
    res.daily = {(d, t): (n, u) for d, t, n, u in
                 con.sql("SELECT event_date, event_type, events, users FROM agg_daily_events").fetchall()}
    res.revenue = {(d, c): (n, r) for d, c, n, r in
                   con.sql("SELECT event_date, currency, purchases, revenue_cents FROM daily_revenue").fetchall()}
    con.close()
    return res


def diff(name: str, want: Result, got: Result, fields: tuple[str, ...]) -> list[str]:
    out = []
    for f in fields:
        w, g = getattr(want, f), getattr(got, f)
        if w != g:
            if isinstance(w, dict):
                keys = sorted(set(w) | set(g), key=str)
                bad = [(k, w.get(k), g.get(k)) for k in keys if w.get(k) != g.get(k)]
                out.append(f"{name}.{f}: {len(bad)} keys differ, first: {bad[:3]}")
            else:
                out.append(f"{name}.{f}: want {w}, got {g}")
    return out


def compare(want: Result, dsn: str, lake: Path, warehouse: Path) -> list[str]:
    """Empty list = everything matches."""
    rows = ("events", "digest")
    return (diff("postgres", want, from_postgres(dsn), rows + ("quarantine", "duplicates"))
            + diff("parquet", want, from_lake(lake), rows)
            + diff("dbt", want, from_warehouse(warehouse), rows + ("daily", "revenue")))
