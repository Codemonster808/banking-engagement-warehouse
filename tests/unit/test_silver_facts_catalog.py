"""Silver conservation, fact grain, and Glue catalog columns.

Invariants from src/transformation/silver.py, facts.py, and catalog.py
(and the gold-layer docs): unique bronze rows with required fields land
in silver; missing fields land in rejects; fact_engagement_daily is
customer × day × event_type; TABLES lists the columns gold actually writes.
"""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from transformation.catalog import TABLES  # noqa: E402
from transformation.facts import build_fact_engagement_daily  # noqa: E402
from transformation.silver import build_spark, silverize  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = build_spark("test-silver-facts")
    yield s
    s.stop()


def _row(**overrides):
    base = {
        "event_id": "e1",
        "customer_id": "c1",
        "event_type": "login",
        "segment_at_event": "mass",
        "ts": "2026-01-01T10:00:00Z",
        "amount_cents": 0,
    }
    base.update(overrides)
    return base


def test_silver_bronze_equals_silver_plus_rejects_for_unique_events(spark):
    df = spark.createDataFrame(
        [
            _row(event_id="ok-1"),
            _row(event_id="ok-2", customer_id="c2"),
            _row(event_id="bad-1", customer_id=None),
        ]
    )
    valid, rejects, stats = silverize(df)
    assert stats["bronze_rows"] == 3
    assert stats["silver_rows"] == 2
    assert stats["rejects"] == 1
    assert stats["reconciled"]
    assert valid.count() + rejects.count() == stats["bronze_rows"]


def test_duplicate_event_id_is_collapsed_before_the_split(spark):
    df = spark.createDataFrame(
        [
            _row(event_id="dup", ts="2026-01-01T10:00:00Z"),
            _row(event_id="dup", ts="2026-01-01T11:00:00Z"),
        ]
    )
    valid, rejects, stats = silverize(df)
    assert stats["bronze_rows"] == 2
    assert valid.count() + rejects.count() == 1
    assert not stats[
        "reconciled"
    ], "raw bronze count is not conserved across duplicates — unique events are"


def test_fact_grain_is_customer_day_event_type(spark, tmp_path):
    path = tmp_path / "silver.json"
    rows = [
        _row(event_id="a", customer_id="c1", event_type="login", ts="2026-01-01T10:00:00Z"),
        _row(event_id="b", customer_id="c1", event_type="login", ts="2026-01-01T18:00:00Z"),
        _row(
            event_id="c",
            customer_id="c1",
            event_type="card_txn",
            ts="2026-01-01T12:00:00Z",
            amount_cents=500,
        ),
        _row(event_id="d", customer_id="c2", event_type="login", ts="2026-01-02T09:00:00Z"),
    ]
    path.write_text("\n".join(json.dumps(r) for r in rows))

    fact = build_fact_engagement_daily(spark, str(path))
    collected = {
        (r.customer_id, str(r.event_date), r.event_type, r.n_events) for r in fact.collect()
    }
    assert ("c1", "2026-01-01", "login", 2) in collected
    assert ("c1", "2026-01-01", "card_txn", 1) in collected
    assert fact.count() == 3


def test_catalog_tables_match_gold_builders():
    assert set(TABLES) == {"dim_customer", "fact_engagement_daily", "dim_offer"}
    fact_cols = [c["Name"] for c in TABLES["fact_engagement_daily"]["columns"]]
    assert fact_cols == [
        "customer_id",
        "event_date",
        "event_type",
        "n_events",
        "total_amount_cents",
    ]
    dim_cols = [c["Name"] for c in TABLES["dim_customer"]["columns"]]
    assert dim_cols == ["customer_id", "segment", "valid_from", "valid_to", "is_current"]
    offer_cols = [c["Name"] for c in TABLES["dim_offer"]["columns"]]
    assert offer_cols == ["event_type", "n_events", "n_distinct_customers", "offer_id"]
