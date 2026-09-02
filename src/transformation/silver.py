#!/usr/bin/env python3
"""bronze -> silver: dedupe by event_id, drop rows missing required
fields (with those rows preserved in silver_rejects, not dropped
silently), write both to S3."""

import argparse
import os
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from models.gates import REQUIRED_FIELDS  # noqa: E402


def silverize(df: Any) -> tuple[Any, Any, dict]:
    """Dedupe bronze by event_id, then split required-field failures to
    rejects so they are never dropped silently.

    `reconciled` is bronze_rows == silver_rows + rejects. Duplicate
    event_ids make that False on purpose: they are collapsed before the
    split, so the conserved quantity is unique events, not raw bronze
    rows. Tests that assert conservation use unique event_ids.
    """
    n_bronze = df.count()
    deduped = df.dropDuplicates(["event_id"])
    valid = deduped.na.drop(subset=list(REQUIRED_FIELDS))
    rejects = deduped.exceptAll(valid)
    n_silver = valid.count()
    n_rejects = rejects.count()
    return (
        valid,
        rejects,
        {
            "bronze_rows": n_bronze,
            "silver_rows": n_silver,
            "rejects": n_rejects,
            "reconciled": n_bronze == n_silver + n_rejects,
        },
    )


def build_spark(app_name: str = "bronze-to-silver"):
    from pyspark.sql import SparkSession

    endpoint = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4583")
    return (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.5.0")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID", "test"))
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY", "test"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def clean_month(spark, month_key: str) -> dict:
    bronze_path = f"s3a://bank-bronze/{month_key}.jsonl"
    silver_path = f"s3a://bank-silver/clean/{month_key}/"
    rejects_path = f"s3a://bank-silver/rejects/{month_key}/"

    df = spark.read.json(bronze_path)
    valid, rejects, stats = silverize(df)
    stats["month"] = month_key

    valid.write.mode("overwrite").json(silver_path)
    if stats["rejects"] > 0:
        rejects.write.mode("overwrite").json(rejects_path)

    return stats


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", required=True, help="e.g. month=00")
    args = parser.parse_args()

    spark = build_spark()
    try:
        stats = clean_month(spark, args.month)
        print(f"silver: {stats}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
