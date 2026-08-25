#!/usr/bin/env python3
"""
Orchestrates bronze -> gates -> silver -> gold for every month found in
s3://bank-bronze/. A month whose gates fail is NOT promoted to silver —
gold.py only ever sees data from promoted months, so a blocked month
provably never reaches the dimensional model. Failures publish a real
SNS alert (deduped by the alert-dedup table in cost_sla.py's pattern).
"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import uuid  # noqa: E402

from common import aws  # noqa: E402
from gates import run_all_gates  # noqa: E402
from gold import build_dim_customer  # noqa: E402
from silver import build_spark, clean_month  # noqa: E402
from statemachine import check_sla, mark_started  # noqa: E402

ALERT_TOPIC = "quality-alerts"
SLA_SECONDS = 180


def list_bronze_months(s3) -> list[str]:
    objs = s3.list_objects_v2(Bucket="bank-bronze").get("Contents", [])
    return sorted(
        obj["Key"].rsplit(".jsonl", 1)[0] for obj in objs if obj["Key"].startswith("month=")
    )


def publish_gate_failure(sns, month: str, failed_results: list) -> None:
    topic_arn = sns.create_topic(Name=ALERT_TOPIC)["TopicArn"]
    reasons = "; ".join(f"{r.name}: {r.detail}" for r in failed_results)
    sns.publish(TopicArn=topic_arn, Message=f"Quality gate blocked promotion of {month}: {reasons}")


def revoke_silver_promotion(s3, month_key: str) -> None:
    """A month that failed gates on this run must not leave stale silver
    data from a PRIOR successful run — otherwise a month can silently
    stay 'promoted' via leftover data even after its source started
    failing quality checks. Found by manually corrupting a previously-
    promoted month and re-running: the block worked, but old silver
    output for that month was still sitting there until this was added."""
    for prefix in (f"clean/{month_key}/", f"rejects/{month_key}/"):
        objs = s3.list_objects_v2(Bucket="bank-silver", Prefix=prefix).get("Contents", [])
        for obj in objs:
            s3.delete_object(Bucket="bank-silver", Key=obj["Key"])


def run_pipeline(known_customer_ids: set) -> dict:
    run_id = str(uuid.uuid4())
    mark_started(run_id)  # Step Functions: real Choice/Retry/Catch around the SLA timer

    spark = build_spark("pipeline")
    s3 = aws.client("s3")
    sns = aws.client("sns")

    months = list_bronze_months(s3)
    results = {"months": [], "promoted": [], "blocked": []}
    prior_segment_counts = None

    try:
        for month_key in months:
            expected_month = "2024-" + str(int(month_key.split("=")[1]) + 1).zfill(2)
            gate_results = run_all_gates(
                spark, f"s3a://bank-bronze/{month_key}.jsonl",
                known_customer_ids, prior_segment_counts, expected_month,
            )
            failed = [r for r in gate_results if not r.passed]

            df = spark.read.json(f"s3a://bank-bronze/{month_key}.jsonl")
            from pyspark.sql import functions as F
            counts = {
                row["segment_at_event"]: row["n"]
                for row in df.groupBy("segment_at_event").count().withColumnRenamed("count", "n").collect()
            }
            prior_segment_counts = counts

            if failed:
                publish_gate_failure(sns, month_key, failed)
                revoke_silver_promotion(s3, month_key)
                results["blocked"].append({"month": month_key, "failed_gates": [r.name for r in failed]})
                results["months"].append({"month": month_key, "promoted": False, "failed_gates": [r.name for r in failed]})
                continue

            silver_stats = clean_month(spark, month_key)
            results["promoted"].append(month_key)
            results["months"].append({"month": month_key, "promoted": True, "silver_stats": silver_stats})

        if results["promoted"]:
            promoted_globs = [f"s3a://bank-silver/clean/{m}/*.json" for m in results["promoted"]]
            dim_customer = build_dim_customer(spark, promoted_globs)
            n_gold_rows = dim_customer.count()
            dim_customer.coalesce(1).write.mode("overwrite").parquet("s3a://bank-gold/dim_customer/")
            results["gold_rows_written"] = n_gold_rows
        else:
            results["gold_rows_written"] = 0

        results["sla"] = check_sla(run_id, sla_seconds=SLA_SECONDS)
        return results
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--customers", type=int, default=5000,
                         help="must match --customers passed to data_gen.py")
    args = parser.parse_args()

    known_customer_ids = {f"cust_{i:06d}" for i in range(args.customers)}
    results = run_pipeline(known_customer_ids)
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
