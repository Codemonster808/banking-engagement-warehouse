"""
The single most important correctness property in this repo: reprocessing
the same bronze data from scratch must produce byte-identical dim_customer
history. If a Spark job's output depends on shuffle/partition ordering
(missing an explicit orderBy somewhere), this test catches it — it would
NOT be caught by a test that only runs the pipeline once.
"""
import hashlib
import json
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

REPO_ROOT = Path(__file__).resolve().parents[1]
BRONZE_GLOB = "data/bronze/month=*.jsonl"
GOLD_PATH = "s3://bank-gold/dim_customer/"


def _run_gold_job() -> None:
    result = subprocess.run(
        [sys.executable, "src/gold.py", "--bronze", BRONZE_GLOB],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert result.returncode == 0, f"gold.py failed:\n{result.stdout}\n{result.stderr}"


def _hash_dim_customer() -> str:
    from common import warehouse

    con = warehouse.connect()
    warehouse.read_parquet(con, GOLD_PATH + "**/*.parquet", "dim_customer")
    rows = con.execute(
        "SELECT customer_id, segment, valid_from, valid_to, is_current "
        "FROM dim_customer ORDER BY customer_id, valid_from"
    ).fetchall()
    return hashlib.sha256(json.dumps(rows, default=str).encode()).hexdigest()


@pytest.mark.skipif(
    not (REPO_ROOT / "data" / "bronze").exists(),
    reason="run `python3 src/data_gen.py --out data/bronze` first",
)
def test_scd2_backfill_is_byte_identical_on_reprocess():
    _run_gold_job()
    hash_1 = _hash_dim_customer()

    _run_gold_job()  # full reprocess from the same bronze data
    hash_2 = _hash_dim_customer()

    assert hash_1 == hash_2, "dim_customer history changed on reprocess — non-determinism in the Spark job"


@pytest.mark.skipif(
    not (REPO_ROOT / "data" / "bronze").exists(),
    reason="run `python3 src/data_gen.py --out data/bronze` first",
)
def test_no_overlapping_validity_ranges_per_customer():
    from common import warehouse

    _run_gold_job()
    con = warehouse.connect()
    warehouse.read_parquet(con, GOLD_PATH + "**/*.parquet", "dim_customer")

    overlaps = con.execute("""
        SELECT a.customer_id
        FROM dim_customer a
        JOIN dim_customer b
          ON a.customer_id = b.customer_id
         AND a.valid_from < COALESCE(b.valid_to, TIMESTAMP '9999-12-31')
         AND COALESCE(a.valid_to, TIMESTAMP '9999-12-31') > b.valid_from
         AND a.valid_from != b.valid_from
        LIMIT 1
    """).fetchall()
    assert overlaps == [], f"overlapping validity ranges found for customer(s): {overlaps}"


@pytest.mark.skipif(
    not (REPO_ROOT / "data" / "bronze").exists(),
    reason="run `python3 src/data_gen.py --out data/bronze` first",
)
def test_exactly_one_current_row_per_customer():
    from common import warehouse

    _run_gold_job()
    con = warehouse.connect()
    warehouse.read_parquet(con, GOLD_PATH + "**/*.parquet", "dim_customer")

    bad = con.execute("""
        SELECT customer_id, COUNT(*) AS n_current
        FROM dim_customer
        WHERE is_current
        GROUP BY customer_id
        HAVING COUNT(*) != 1
        LIMIT 5
    """).fetchall()
    assert bad == [], f"customers without exactly one current row: {bad}"
