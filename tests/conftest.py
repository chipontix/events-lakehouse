import datetime as dt
import os
import subprocess
import sys
from pathlib import Path

DBT_DIR = Path(__file__).resolve().parent.parent / "dbt"


def dbt(tmp: Path, env: dict, *args: str) -> subprocess.CompletedProcess:
    """Runs the dbt CLI against the project with this test's lake and warehouse."""
    cmd = [str(Path(sys.executable).parent / "dbt"), *args, "--project-dir", str(DBT_DIR),
           "--profiles-dir", str(DBT_DIR), "--target-path", str(tmp / "target"), "--log-path", str(tmp / "logs")]
    return subprocess.run(cmd, env=env, capture_output=True, text=True)


def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


def lake_env(tmp: Path) -> dict:
    return dict(os.environ, LAKE_DIR=str(tmp / "lake"), WAREHOUSE=str(tmp / "warehouse.duckdb"))
