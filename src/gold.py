#!/usr/bin/env python3
"""
Builds dim_customer as a proper SCD Type 2 table from the bronze event
files: for each customer, whenever segment_at_event changes from the
previous known value, close the old row (valid_to) and open a new one.

This must be re-runnable from scratch against the same bronze data and
produce byte-identical history every time — that's what
tests/test_scd2_backfill.py checks, and it's the single most important
correctness property in this repo.
"""
import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import SparkSession, Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402


def build_spark(app_name: str = "scd2-gold") -> SparkSession:
    endpoint = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")
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


def build_dim_customer(spark: SparkSession, bronze_glob: str) -> "pyspark.sql.DataFrame":
    events = spark.read.json(bronze_glob).withColumn("event_ts", F.col("ts").cast("timestamp"))

    # One row per (customer, segment) transition: the first time each
    # customer is seen with a given segment after having a different one.
    window = Window.partitionBy("customer_id").orderBy("event_ts")
    with_prev = events.select("customer_id", "segment_at_event", "event_ts").withColumn(
        "prev_segment", F.lag("segment_at_event").over(window)
    )
    transitions = with_prev.filter(
        F.col("prev_segment").isNull() | (F.col("prev_segment") != F.col("segment_at_event"))
    ).select(
        F.col("customer_id"),
        F.col("segment_at_event").alias("segment"),
        F.col("event_ts").alias("valid_from"),
    )

    # Deterministic ordering is mandatory here — without it, ties on the
    # same event_ts can be assigned inconsistent valid_from/valid_to
    # across runs, which is exactly the kind of non-determinism that
    # breaks byte-identical reprocessing.
    dedup_window = Window.partitionBy("customer_id", "valid_from").orderBy(F.col("segment"))
    transitions = (
        transitions.withColumn("_rn", F.row_number().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    change_window = Window.partitionBy("customer_id").orderBy("valid_from")
    dim_customer = transitions.withColumn(
        "valid_to", F.lead("valid_from").over(change_window)
    ).withColumn(
        "is_current", F.col("valid_to").isNull()
    )

    return dim_customer.orderBy("customer_id", "valid_from")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze", default="data/bronze/month=*.jsonl")
    parser.add_argument("--dst", default="s3a://bank-gold/dim_customer/")
    args = parser.parse_args()

    spark = build_spark()
    try:
        dim_customer = build_dim_customer(spark, args.bronze)
        n_rows = dim_customer.count()
        (
            dim_customer.coalesce(1)
            .write.mode("overwrite")
            .parquet(args.dst)
        )
        print(f"wrote {n_rows} dim_customer rows to {args.dst}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
