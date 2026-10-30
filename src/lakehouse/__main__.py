"""python -m lakehouse {produce,ingest,expected,verify}; prints one JSON line."""

import argparse
import json
import os
import sys
from pathlib import Path

from . import ingest, produce, reference, verify
from .generator import Spec

ENV = {
    "bootstrap": os.environ.get("KAFKA_BOOTSTRAP", "localhost:16092"),
    "dsn": os.environ.get("PG_DSN", "postgresql://lakehouse:lakehouse@localhost:16432/lakehouse"),
    "lake": Path(os.environ.get("LAKE_DIR", "data/lake")),
    "warehouse": Path(os.environ.get("WAREHOUSE", "data/warehouse.duckdb")),
    "topic": os.environ.get("TOPIC", "events"),
}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="lakehouse")
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("produce", "expected", "verify"):
        p = sub.add_parser(name)
        p.add_argument("--n", type=int, default=Spec.n, help="messages in the whole stream")
        p.add_argument("--seed", type=int, default=Spec.seed)
    sub.choices["produce"].add_argument("--start", type=int, default=0, help="first message index to send")
    sub.choices["produce"].add_argument("--stop", type=int, help="index after the last message to send")
    sub.choices["produce"].add_argument("--partitions", type=int, default=6)
    p = sub.add_parser("ingest")
    p.add_argument("--batch-size", type=int, default=50_000)
    p.add_argument("--max-wait", type=float, default=1.0, help="seconds before a partial batch is flushed")
    p.add_argument("--stop-at-eof", action="store_true", help="exit once every partition is drained")
    p.add_argument("--partitions", type=lambda v: [int(x) for x in v.split(",")], help="e.g. 0,1,2 (default: all)")
    a = ap.parse_args(argv)

    if a.cmd == "produce":
        produce.ensure_topic(ENV["bootstrap"], ENV["topic"], a.partitions)
        out = produce.run(ENV["bootstrap"], ENV["topic"], Spec(n=a.n, seed=a.seed), a.start, a.stop)
    elif a.cmd == "ingest":
        out = ingest.run(ENV["bootstrap"], ENV["dsn"], ENV["lake"], ENV["topic"], a.batch_size, a.max_wait,
                         a.stop_at_eof, a.partitions)
    elif a.cmd == "expected":
        r = reference.expected(Spec(n=a.n, seed=a.seed))
        out = {"events": r.events, "digest": f"{r.digest:016x}", "duplicates": r.duplicates,
               "quarantine": r.quarantine}
    else:
        problems = verify.compare(reference.expected(Spec(n=a.n, seed=a.seed)), ENV["dsn"], ENV["lake"],
                                  ENV["warehouse"])
        out = {"ok": not problems, "problems": problems}
    print(json.dumps(out, default=str))
    return 0 if out.get("ok", True) else 1


if __name__ == "__main__":
    sys.exit(main())
