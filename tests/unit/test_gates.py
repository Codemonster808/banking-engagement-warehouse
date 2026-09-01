"""Each of the 6 seeded defect classes must be caught by its gate."""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from models.gates import (  # noqa: E402
    gate_cardinality_drift,
    gate_duplicates,
    gate_freshness,
    gate_range_outliers,
    gate_referential_integrity,
    gate_required_fields,
)


@pytest.fixture(scope="module")
def spark():
    from transformation.gold import build_spark

    s = build_spark("test-gates")
    yield s
    s.stop()


def _write_events(tmp_path, events: list[dict]) -> str:
    path = tmp_path / "events.jsonl"
    with path.open("w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")
    return str(path)


def _good_event(**overrides) -> dict:
    base = {
        "event_id": "e1",
        "customer_id": "cust_000001",
        "event_type": "login",
        "segment_at_event": "mass",
        "amount_cents": 100,
        "ts": "2024-01-15T00:00:00+00:00",
    }
    base.update(overrides)
    return base


def test_gate_catches_missing_required_field(spark, tmp_path):
    events = [
        _good_event(event_id="e1"),
        {"customer_id": "cust_000002", "ts": "2024-01-15T00:00:00+00:00"},
    ]
    path = _write_events(tmp_path, events)
    df = spark.read.json(path)
    result = gate_required_fields(df)
    assert result.passed is False


def test_gate_catches_duplicate_event_id(spark, tmp_path):
    events = [
        _good_event(event_id="dup-1"),
        _good_event(event_id="dup-1", customer_id="cust_000002"),
    ]
    path = _write_events(tmp_path, events)
    df = spark.read.json(path)
    result = gate_duplicates(df)
    assert result.passed is False


def test_gate_catches_referential_break(spark, tmp_path):
    events = [_good_event(event_id="e1", customer_id="cust_unknown")]
    path = _write_events(tmp_path, events)
    df = spark.read.json(path)
    result = gate_referential_integrity(df, known_customer_ids={"cust_000001"})
    assert result.passed is False


def test_gate_catches_negative_amount(spark, tmp_path):
    events = [_good_event(event_id="e1", amount_cents=-500)]
    path = _write_events(tmp_path, events)
    df = spark.read.json(path)
    result = gate_range_outliers(df)
    assert result.passed is False


def test_gate_catches_invalid_segment(spark, tmp_path):
    events = [_good_event(event_id="e1", segment_at_event="not_a_real_segment")]
    path = _write_events(tmp_path, events)
    df = spark.read.json(path)
    result = gate_range_outliers(df)
    assert result.passed is False


def test_gate_catches_cardinality_drift(spark, tmp_path):
    events = [_good_event(event_id=f"e{i}", segment_at_event="mass") for i in range(100)]
    path = _write_events(tmp_path, events)
    df = spark.read.json(path)
    prior_counts = {"mass": 10, "affluent": 90}  # prior month was 10% mass, this month is 100% mass
    result = gate_cardinality_drift(df, prior_counts)
    assert result.passed is False


def test_gate_catches_freshness_failure(spark, tmp_path):
    events = [_good_event(event_id="e1", ts="2023-06-01T00:00:00+00:00")]  # wrong month
    path = _write_events(tmp_path, events)
    df = spark.read.json(path)
    result = gate_freshness(df, expected_month="2024-01")
    assert result.passed is False


def test_all_gates_pass_on_clean_data(spark, tmp_path):
    events = [_good_event(event_id=f"e{i}", customer_id="cust_000001") for i in range(10)]
    path = _write_events(tmp_path, events)
    df = spark.read.json(path)
    assert gate_required_fields(df).passed
    assert gate_duplicates(df).passed
    assert gate_referential_integrity(df, known_customer_ids={"cust_000001"}).passed
    assert gate_range_outliers(df).passed
    assert gate_freshness(df, expected_month="2024-01").passed
