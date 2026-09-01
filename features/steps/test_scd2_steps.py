"""
Step definitions for features/scd2.feature.

Wraps the real setup/assertions from tests/data_quality/test_scd2_backfill.py:
runs the actual gold.py Spark job against the seeded bronze fixtures and
queries dim_customer via the same DuckDB-over-S3-Parquet path the tests use.
"""
import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

REPO_ROOT = Path(__file__).resolve().parents[2]
BRONZE_GLOB = "data/bronze/month=*.jsonl"
GOLD_PATH = "s3://bank-gold/dim_customer/"

scenarios("../scd2.feature")

pytestmark = pytest.mark.skipif(
    not (REPO_ROOT / "data" / "bronze").exists(),
    reason="run `python3 src/ingestion/data_gen.py --out data/bronze` first",
)


def _run_gold_job() -> None:
    result = subprocess.run(
        [sys.executable, "src/transformation/gold.py", "--bronze", BRONZE_GLOB],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert result.returncode == 0, f"gold.py failed:\n{result.stdout}\n{result.stderr}"


def _dim_customer_con():
    from utils import warehouse

    con = warehouse.connect()
    warehouse.read_parquet(con, GOLD_PATH + "**/*.parquet", "dim_customer")
    return con


def _hash_dim_customer() -> str:
    con = _dim_customer_con()
    rows = con.execute(
        "SELECT customer_id, segment, valid_from, valid_to, is_current "
        "FROM dim_customer ORDER BY customer_id, valid_from"
    ).fetchall()
    return hashlib.sha256(json.dumps(rows, default=str).encode()).hexdigest()


@pytest.fixture(scope="module")
def gold_built():
    _run_gold_job()
    return True


@given("the gold job has been run against the bronze fixtures", target_fixture="gold_built_flag")
def given_gold_built(gold_built):
    return gold_built


@given(
    parsers.parse(
        "customer {customer_id} changed segment from {old_segment} to {new_segment} on {change_date}"
    ),
    target_fixture="customer_rows",
)
def customer_segment_rows(gold_built_flag, customer_id, old_segment, new_segment, change_date):
    con = _dim_customer_con()
    rows = con.execute(
        "SELECT segment, valid_from, valid_to, is_current FROM dim_customer "
        "WHERE customer_id = ? ORDER BY valid_from",
        [customer_id],
    ).fetchall()
    assert rows, f"no dim_customer rows found for {customer_id}"
    return {"customer_id": customer_id, "old_segment": old_segment, "new_segment": new_segment, "rows": rows}


@then(parsers.parse("the {segment} row for {customer_id} has valid_to set and is_current false"))
def prior_row_closed(customer_rows, segment, customer_id):
    matching = [r for r in customer_rows["rows"] if r[0] == segment]
    assert matching, f"no {segment} row found for {customer_id}"
    _, valid_from, valid_to, is_current = matching[0]
    assert valid_to is not None, f"expected valid_to set on prior {segment} row for {customer_id}"
    assert is_current is False, f"expected is_current=false on prior {segment} row for {customer_id}"


@then(parsers.parse("the {segment} row for {customer_id} is_current true"))
def new_row_open(customer_rows, segment, customer_id):
    matching = [r for r in customer_rows["rows"] if r[0] == segment]
    assert matching, f"no {segment} row found for {customer_id}"
    _, valid_from, valid_to, is_current = matching[-1]
    assert is_current is True, f"expected is_current=true on {segment} row for {customer_id}"
    assert valid_to is None, f"expected valid_to unset on current {segment} row for {customer_id}"


@then("every customer has exactly one current row in dim_customer")
def one_current_row(gold_built_flag):
    con = _dim_customer_con()
    bad = con.execute("""
        SELECT customer_id, COUNT(*) AS n_current
        FROM dim_customer
        WHERE is_current
        GROUP BY customer_id
        HAVING COUNT(*) != 1
        LIMIT 5
    """).fetchall()
    assert bad == [], f"customers without exactly one current row: {bad}"


@then("no customer has overlapping validity ranges in dim_customer")
def no_overlaps(gold_built_flag):
    con = _dim_customer_con()
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


@when("the gold job is run again against the same bronze fixtures", target_fixture="reprocess_hashes")
def reprocess(gold_built_flag):
    hash_before = _hash_dim_customer()
    _run_gold_job()  # full reprocess from the same bronze data
    hash_after = _hash_dim_customer()
    return {"before": hash_before, "after": hash_after}


@then("the dim_customer history hash is unchanged")
def hash_unchanged(reprocess_hashes):
    assert reprocess_hashes["before"] == reprocess_hashes["after"], (
        "dim_customer history changed on reprocess — non-determinism in the Spark job"
    )
