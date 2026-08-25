"""
End-to-end quality test: bronze -> gates -> silver -> gold, including a
seeded defect that must be BLOCKED (not just detected) — gold must
provably never contain data from the blocked month, and any stale
silver output from a prior run of that month must be revoked.
"""
import json
import sys
import time
import uuid
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from common import aws, warehouse  # noqa: E402
from common.quality import Dimension, QualityReport  # noqa: E402
from pipeline import list_bronze_months, run_pipeline  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
N_MONTHS = 3
N_CUSTOMERS = 100


def _clear_bucket(s3, bucket: str, prefix: str = "") -> None:
    objs = s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])
    for o in objs:
        s3.delete_object(Bucket=bucket, Key=o["Key"])


@pytest.fixture(scope="module")
def clean_buckets():
    s3 = aws.client("s3")
    for b in ("bank-bronze", "bank-silver", "bank-gold"):
        _clear_bucket(s3, b)
    yield


def test_full_pipeline_quality(clean_buckets):
    import subprocess

    run_id = uuid.uuid4().hex[:8]
    data_dir = REPO_ROOT / "data" / f"e2e_{run_id}"

    gen = subprocess.run(
        [sys.executable, "src/data_gen.py", "--months", str(N_MONTHS), "--customers", str(N_CUSTOMERS),
         "--out", str(data_dir), "--seed", "7"],
        cwd=REPO_ROOT, capture_output=True, text=True, timeout=30,
    )
    assert gen.returncode == 0, gen.stderr

    upload = subprocess.run(
        [sys.executable, "src/upload_bronze.py", "--in", str(data_dir)],
        cwd=REPO_ROOT, capture_output=True, text=True, timeout=30,
    )
    assert upload.returncode == 0, upload.stderr

    known_customer_ids = {f"cust_{i:06d}" for i in range(N_CUSTOMERS)}

    # --- run 1: clean data, everything should promote ---
    t0 = time.perf_counter()
    clean_result = run_pipeline(known_customer_ids)
    clean_run_seconds = time.perf_counter() - t0

    # --- inject a seeded defect into the last month, re-run ---
    s3 = aws.client("s3")
    months = list_bronze_months(s3)
    target_month = months[-1]
    body = s3.get_object(Bucket="bank-bronze", Key=f"{target_month}.jsonl")["Body"].read().decode()
    lines = body.splitlines()
    bad = json.loads(lines[0])
    bad["amount_cents"] = -9999
    lines[0] = json.dumps(bad)
    s3.put_object(Bucket="bank-bronze", Key=f"{target_month}.jsonl", Body="\n".join(lines).encode())

    dirty_result = run_pipeline(known_customer_ids)

    report = QualityReport(pipeline="banking-engagement-warehouse")

    report.check(
        Dimension.COMPLETENESS, "all_months_reconcile_bronze_eq_silver_plus_rejects",
        measured=sum(1 for m in clean_result["months"] if m.get("silver_stats", {}).get("reconciled")),
        threshold=N_MONTHS, detail="bronze_rows == silver_rows + rejects for every promoted month",
    )
    report.check(
        Dimension.TIMELINESS, "sla_timer_runs_via_real_step_functions",
        measured=1.0 if "sla" in clean_result and "breached" in clean_result["sla"] else 0.0,
        threshold=1.0, detail=f"sla={clean_result.get('sla')}",
    )

    report.check(
        Dimension.CORRECTNESS, "clean_run_promotes_all_months",
        measured=len(clean_result["promoted"]), threshold=N_MONTHS,
        detail=f"promoted {clean_result['promoted']} of {N_MONTHS} months on clean data",
    )
    report.check(
        Dimension.CORRECTNESS, "seeded_defect_blocks_exactly_the_bad_month",
        measured=1.0 if dirty_result["blocked"] == [{"month": target_month, "failed_gates": ["range_outliers"]}] else 0.0,
        threshold=1.0, detail=f"blocked={dirty_result['blocked']}",
    )

    n_silver_bad_month = s3.list_objects_v2(Bucket="bank-silver", Prefix=f"clean/{target_month}/").get("KeyCount", 0)
    report.check(
        Dimension.VALIDITY, "blocked_month_has_no_silver_output",
        measured=n_silver_bad_month, threshold=0, higher_is_better=False,
        detail="a month that fails gates must leave zero rows in silver, including revoking a prior successful run",
    )

    con = warehouse.connect()
    warehouse.read_parquet(con, "s3://bank-gold/dim_customer/**/*.parquet", "dim_customer")
    gold_row_count = con.execute("SELECT COUNT(*) FROM dim_customer").fetchone()[0]
    report.check(
        Dimension.VALIDITY, "gold_reflects_only_promoted_months",
        measured=1.0 if gold_row_count == dirty_result["gold_rows_written"] else 0.0,
        threshold=1.0, detail=f"gold has {gold_row_count} rows, pipeline reported writing {dirty_result['gold_rows_written']}",
    )

    sqs = aws.client("sqs")
    queue_url = sqs.get_queue_url(QueueName="quality-oncall-queue")["QueueUrl"]
    found_alert = False
    for _ in range(5):
        msgs = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=1).get("Messages", [])
        if any(target_month in m["Body"] for m in msgs):
            found_alert = True
            break
    report.check(
        Dimension.VALIDITY, "gate_failure_publishes_sns_alert", measured=1.0 if found_alert else 0.0,
        threshold=1.0, detail="SNS -> SQS alert for the blocked month reached the on-call queue",
    )

    report.check(
        Dimension.TIMELINESS, "clean_pipeline_run_under_sla", measured=round(clean_run_seconds, 1),
        threshold=180.0, higher_is_better=False, detail=f"{N_MONTHS}-month pipeline run wall time",
    )

    report.to_json(str(REPO_ROOT / "benchmarks" / "quality-report.json"))
    report.to_markdown(str(REPO_ROOT / "docs" / "quality-report.md"))

    for f in data_dir.glob("*"):
        f.unlink()
    data_dir.rmdir()

    report.assert_all_passed()
