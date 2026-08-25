#!/usr/bin/env python3
"""
6 data quality gates that run against bronze data BEFORE gold.py is
allowed to run. Each gate is a pure function: (spark, df) -> GateResult.
A single failing gate blocks the whole promotion — see main().
"""
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))


@dataclass
class GateResult:
    name: str
    passed: bool
    detail: str = ""


REQUIRED_FIELDS = {"event_id", "customer_id", "event_type", "segment_at_event", "ts"}
VALID_SEGMENTS = {"mass", "affluent", "private", "student", "dormant"}
VALID_EVENT_TYPES = {"login", "card_txn", "offer_shown", "offer_redeemed"}


def gate_required_fields(df) -> GateResult:
    from pyspark.sql import functions as F

    missing_counts = {}
    for field in REQUIRED_FIELDS:
        if field not in df.columns:
            missing_counts[field] = "column absent"
            continue
        n_null = df.filter(F.col(field).isNull()).count()
        if n_null > 0:
            missing_counts[field] = n_null
    if missing_counts:
        return GateResult("required_fields", False, f"null/missing: {missing_counts}")
    return GateResult("required_fields", True)


def gate_cardinality_drift(df, prior_segment_counts: dict | None = None) -> GateResult:
    """Segment distribution shouldn't shift more than 30 percentage points month over month."""
    from pyspark.sql import functions as F

    counts = {
        row["segment_at_event"]: row["n"]
        for row in df.groupBy("segment_at_event").count().withColumnRenamed("count", "n").collect()
    }
    total = sum(counts.values()) or 1
    shares = {k: v / total for k, v in counts.items()}

    if prior_segment_counts is None:
        return GateResult("cardinality_drift", True, "no prior month to compare")

    prior_total = sum(prior_segment_counts.values()) or 1
    prior_shares = {k: v / prior_total for k, v in prior_segment_counts.items()}

    for segment in VALID_SEGMENTS:
        drift = abs(shares.get(segment, 0) - prior_shares.get(segment, 0))
        if drift > 0.30:
            return GateResult("cardinality_drift", False, f"{segment} share drifted {drift:.1%}")
    return GateResult("cardinality_drift", True)


def gate_referential_integrity(events_df, known_customer_ids: set) -> GateResult:
    from pyspark.sql import functions as F

    orphans = events_df.filter(~F.col("customer_id").isin(list(known_customer_ids))).count()
    if orphans > 0:
        return GateResult("referential_integrity", False, f"{orphans} events reference unknown customers")
    return GateResult("referential_integrity", True)


def gate_duplicates(df) -> GateResult:
    total = df.count()
    unique = df.select("event_id").distinct().count()
    if total != unique:
        return GateResult("duplicates", False, f"{total - unique} duplicate event_id(s)")
    return GateResult("duplicates", True)


def gate_range_outliers(df) -> GateResult:
    from pyspark.sql import functions as F

    negative = df.filter(F.col("amount_cents") < 0).count()
    if negative > 0:
        return GateResult("range_outliers", False, f"{negative} events with negative amount_cents")

    invalid_segment = df.filter(~F.col("segment_at_event").isin(list(VALID_SEGMENTS))).count()
    if invalid_segment > 0:
        return GateResult("range_outliers", False, f"{invalid_segment} events with an unknown segment value")
    return GateResult("range_outliers", True)


def gate_freshness(df, expected_month: str) -> GateResult:
    from pyspark.sql import functions as F

    n_this_month = df.filter(F.date_format(F.col("ts").cast("timestamp"), "yyyy-MM") == expected_month).count()
    if n_this_month == 0:
        return GateResult("freshness", False, f"no events found for expected month {expected_month}")
    return GateResult("freshness", True)


def run_all_gates(spark, month_file: str, known_customer_ids: set,
                   prior_segment_counts: dict | None, expected_month: str) -> list[GateResult]:
    df = spark.read.json(month_file)
    return [
        gate_required_fields(df),
        gate_duplicates(df),
        gate_referential_integrity(df, known_customer_ids),
        gate_range_outliers(df),
        gate_cardinality_drift(df, prior_segment_counts),
        gate_freshness(df, expected_month),
    ]
