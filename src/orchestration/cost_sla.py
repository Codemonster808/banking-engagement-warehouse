#!/usr/bin/env python3
"""
Per-pipeline cost attribution + SLA breach detection. Tag-based: each
pipeline run reports its own resource usage (files written, bytes, run
seconds) under a pipeline_id, so cost is attributed to the pipeline that
caused it rather than computed globally and guessed at.

SLA alerts are deduped: firing one alert per (pipeline, day) even if the
check runs multiple times, using a DynamoDB conditional write as the dedup
gate — the same idempotency pattern as fintech-txn-integrity-pipeline's
gate, applied to alerting instead of transactions.
"""

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils import aws  # noqa: E402

COST_TABLE = "pipeline-cost"
ALERT_DEDUP_TABLE = "sla-alert-dedup"
ALERT_TOPIC = "quality-alerts"

# Rough LocalStack/MiniStack-era cost model: $ per GB-processed, a stand-in
# for what a real Redshift/EMR bill would scale with. Not a real AWS price.
COST_PER_GB = 0.023


def record_pipeline_run(
    pipeline_id: str, run_date: str, bytes_processed: int, duration_seconds: float
) -> dict:
    ddb = aws.client("dynamodb")
    cost_usd = (bytes_processed / (1024**3)) * COST_PER_GB

    ddb.put_item(
        TableName=COST_TABLE,
        Item={
            "pipeline_id": {"S": f"{pipeline_id}#{run_date}"},
            "run_date": {"S": run_date},
            "bytes_processed": {"N": str(bytes_processed)},
            "duration_seconds": {"N": str(duration_seconds)},
            "cost_usd": {"N": str(round(cost_usd, 6))},
        },
    )
    return {"pipeline_id": pipeline_id, "run_date": run_date, "cost_usd": round(cost_usd, 6)}


def check_sla(pipeline_id: str, run_date: str, duration_seconds: float, sla_seconds: float) -> dict:
    if duration_seconds <= sla_seconds:
        return {"breached": False}

    ddb = aws.client("dynamodb")
    dedup_key = f"{pipeline_id}#{run_date}"
    try:
        ddb.put_item(
            TableName=ALERT_DEDUP_TABLE,
            Item={"alert_id": {"S": dedup_key}},
            ConditionExpression="attribute_not_exists(alert_id)",
        )
    except ddb.exceptions.ConditionalCheckFailedException:
        return {"breached": True, "alert_sent": False, "reason": "already alerted today (deduped)"}

    sns = aws.client("sns")
    topic_arn = sns.create_topic(Name=ALERT_TOPIC)["TopicArn"]
    sns.publish(
        TopicArn=topic_arn,
        Message=f"SLA breach: {pipeline_id} took {duration_seconds:.0f}s (limit {sla_seconds:.0f}s) on {run_date}",
    )
    return {"breached": True, "alert_sent": True}


def cost_by_pipeline() -> list[dict]:
    ddb = aws.client("dynamodb")
    items = []
    paginator = ddb.get_paginator("scan")
    for page in paginator.paginate(TableName=COST_TABLE):
        for item in page.get("Items", []):
            items.append(
                {
                    "pipeline_id": item["pipeline_id"]["S"],
                    "run_date": item["run_date"]["S"],
                    "cost_usd": float(item["cost_usd"]["N"]),
                }
            )
    return items


if __name__ == "__main__":
    today = date.today().isoformat()
    print(
        record_pipeline_run(
            "dim_customer_scd2", today, bytes_processed=50_000_000, duration_seconds=42.0
        )
    )
