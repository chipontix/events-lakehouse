"""One timed 1M run inside the app container: produce, ingest (W workers),
dbt run + test, verify against the reference. Prints one JSON object."""

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import psycopg
from confluent_kafka.admin import AdminClient

from lakehouse import produce, reference, verify
from lakehouse.generator import Spec

ap = argparse.ArgumentParser()
ap.add_argument("--n", type=int, default=1_000_000)
ap.add_argument("--workers", type=int, default=1)
ap.add_argument("--batch-size", type=int, default=50_000)
a = ap.parse_args()

BOOT, BASE = os.environ["KAFKA_BOOTSTRAP"], os.environ["PG_DSN"]
DB = TOPIC = "bench"
data = Path("/work/data/bench")
env = dict(os.environ, PG_DSN=BASE.rsplit("/", 1)[0] + "/" + DB, TOPIC=TOPIC,
           LAKE_DIR=str(data / "lake"), WAREHOUSE=str(data / "warehouse.duckdb"))

# reset
with psycopg.connect(BASE, autocommit=True) as c:
    c.execute(f"DROP DATABASE IF EXISTS {DB} WITH (FORCE)")
    c.execute(f"CREATE DATABASE {DB}")
admin = AdminClient({"bootstrap.servers": BOOT})
if TOPIC in admin.list_topics(timeout=10).topics:
    admin.delete_topics([TOPIC])[TOPIC].result()
    while TOPIC in admin.list_topics(timeout=10).topics:
        time.sleep(0.5)
shutil.rmtree(data, ignore_errors=True)
for _ in range(20):  # topic deletion is asynchronous on the broker
    try:
        produce.ensure_topic(BOOT, TOPIC, 6)
        break
    except Exception:
        time.sleep(1)


def timed(cmd):
    t = time.monotonic()
    p = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if p.returncode:
        sys.exit(f"{cmd} failed:\n{p.stdout}\n{p.stderr}")
    return p.stdout, round(time.monotonic() - t, 3)


out = {"n": a.n, "workers": a.workers, "batch_size": a.batch_size}
t_all = time.monotonic()
out["produce"] = produce.run(BOOT, TOPIC, Spec(n=a.n), 0, None)

parts = [",".join(str(p) for p in range(6) if p % a.workers == w) for w in range(a.workers)]
t = time.monotonic()
procs = [subprocess.Popen([sys.executable, "-m", "lakehouse", "ingest", "--stop-at-eof", f"--batch-size={a.batch_size}",
                           f"--partitions={ps}"], env=env, stdout=subprocess.PIPE, text=True) for ps in parts]
workers = [json.loads(p.communicate()[0]) for p in procs]
assert all(p.returncode == 0 for p in procs)
wall = round(time.monotonic() - t, 3)
msgs = sum(w["messages"] for w in workers)
ins = sum(w["inserted"] for w in workers)
# first message received by any worker -> last batch durable in any worker
busy = max(w["last_commit_at"] for w in workers) - min(w["first_message_at"] for w in workers)
out["ingest"] = {"messages": msgs, "inserted": ins, "duplicates": sum(w["duplicates"] for w in workers),
                 "quarantined": sum(w["quarantined"] for w in workers), "seconds": round(busy, 3),
                 "process_wall_seconds": wall, "messages_per_s": round(msgs / busy),
                 "inserted_per_s": round(ins / busy), "per_worker": workers}

dbt = [str(Path(sys.executable).parent / "dbt"), "--no-use-colors"]
proj = ["--project-dir", "/work/dbt", "--profiles-dir", "/work/dbt", "--target-path", str(data / "target"),
        "--log-path", str(data / "logs")]
_, out["dbt_run_seconds"] = timed([*dbt, "run", *proj])
out["pipeline_seconds"] = round(time.monotonic() - t_all, 3)  # produce + ingest + dbt run
log, out["dbt_test_seconds"] = timed([*dbt, "test", *proj])
out["dbt_test_summary"] = next(line for line in log.splitlines() if "PASS=" in line).split("Done. ")[-1]

t = time.monotonic()
problems = verify.compare(reference.expected(Spec(n=a.n)), env["PG_DSN"], Path(env["LAKE_DIR"]), Path(env["WAREHOUSE"]))
out["verify"] = {"ok": not problems, "problems": problems, "seconds": round(time.monotonic() - t, 3)}
out["vm"] = {"nproc": os.cpu_count(), "loadavg_after": Path("/proc/loadavg").read_text().split()[:3],
             "mem_total_kb": int(Path("/proc/meminfo").read_text().split()[1])}
print(json.dumps(out))
sys.exit(0 if not problems else 1)
