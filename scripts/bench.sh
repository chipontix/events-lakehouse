#!/usr/bin/env bash
# Usage: scripts/bench.sh WORKERS RUNS [N]. Appends one JSON line per run to
# results/bench.jsonl (host facts added here, the run itself in the container)
# and keeps the raw container output in results/raw/.
set -euo pipefail
workers=${1:-1} runs=${2:-1} n=${3:-1000000}
mkdir -p results/raw
for i in $(seq "$runs"); do
  ts=$(date -u +%Y%m%dT%H%M%SZ)
  containers=$(docker ps -q | wc -l | tr -d ' ')
  host_load=$(sysctl -n vm.loadavg 2>/dev/null || cat /proc/loadavg)
  raw=results/raw/bench-$ts-w$workers.log
  tmp=$(mktemp)
  docker compose run --rm -T app python scripts/bench.py --n "$n" --workers "$workers" 2>&1 | tee "$raw" | tail -1 > "$tmp"
  python3 - "$ts" "$containers" "$host_load" "$(git rev-parse --short HEAD 2>/dev/null || echo none)" "$tmp" <<'PY' >> results/bench.jsonl
import json, platform, subprocess, sys
r = json.load(open(sys.argv[5]))
def sh(*c):
    try: return subprocess.run(c, capture_output=True, text=True).stdout.strip()
    except OSError: return None
r["host"] = {"cpu": sh("sysctl", "-n", "machdep.cpu.brand_string") or platform.processor(),
             "ncpu": sh("sysctl", "-n", "hw.ncpu"), "memsize": sh("sysctl", "-n", "hw.memsize"),
             "loadavg_before": sys.argv[3]}
r.update(started_utc=sys.argv[1], docker_containers_running=int(sys.argv[2]), commit=sys.argv[4])
print(json.dumps(r))
PY
  rm -f "$tmp"
  tail -1 results/bench.jsonl | python3 -c 'import json,sys; r=json.load(sys.stdin); print(r["workers"], "workers:", r["ingest"]["messages_per_s"], "msg/s in,", r["ingest"]["inserted_per_s"], "rows/s out, pipeline", r["pipeline_seconds"], "s, verify ok:", r["verify"]["ok"])'
done
