#!/usr/bin/env python3
"""Builds fact_engagement_daily (one row per customer/day/event_type) and
dim_offer (distinct offer-related event types seen) from promoted silver
data — the gold-layer facts the README promises alongside dim_customer.

`src/pipeline.py` now calls build_fact_engagement_daily()/build_dim_offer()
directly as part of every orchestrated run, over the same promoted-months
silver globs dim_customer is built from — so `make demo`/the daily pipeline
run already produces all three gold tables. This file's main() is kept as
a standalone CLI for ad hoc reruns (e.g. rebuilding just the facts/offer
tables from a --silver-glob without rerunning gates/silver/dim_customer);
it is not required for the automated run."""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F  # noqa: E402

from transformation.silver import build_spark  # noqa: E402


def build_fact_engagement_daily(spark, silver_glob: str):
    df = spark.read.json(silver_glob)
    return (
        df.withColumn("event_date", F.to_date(F.col("ts").cast("timestamp")))
        .groupBy("customer_id", "event_date", "event_type")
        .agg(
            F.count("*").alias("n_events"),
            F.sum(F.coalesce(F.col("amount_cents"), F.lit(0))).alias("total_amount_cents"),
        )
        .orderBy("customer_id", "event_date")
    )


def build_dim_offer(spark, silver_glob: str):
    df = spark.read.json(silver_glob)
    offer_events = df.filter(F.col("event_type").isin(["offer_shown", "offer_redeemed"]))
    return (
        offer_events.groupBy("event_type")
        .agg(
            F.count("*").alias("n_events"),
            F.countDistinct("customer_id").alias("n_distinct_customers"),
        )
        .withColumn("offer_id", F.concat(F.lit("offer_"), F.col("event_type")))
        .orderBy("event_type")
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver-glob", default="s3a://bank-silver/clean/*/*.json")
    args = parser.parse_args()

    spark = build_spark("facts")
    try:
        fact = build_fact_engagement_daily(spark, args.silver_glob)
        n_fact = fact.count()
        fact.coalesce(1).write.mode("overwrite").parquet("s3a://bank-gold/fact_engagement_daily/")

        offer = build_dim_offer(spark, args.silver_glob)
        n_offer = offer.count()
        offer.coalesce(1).write.mode("overwrite").parquet("s3a://bank-gold/dim_offer/")

        print(f"fact_engagement_daily: {n_fact} rows, dim_offer: {n_offer} rows")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
