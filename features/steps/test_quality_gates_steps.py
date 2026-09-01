import json
import sys
from pathlib import Path

import pytest
from pytest_bdd import given, scenarios, then, when

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from models.gates import (  # noqa: E402
    gate_duplicates,
    gate_referential_integrity,
    gate_required_fields,
)

scenarios("../quality-gates.feature")


@pytest.fixture(scope="module")
def spark():
    from transformation.gold import build_spark

    s = build_spark("bdd-gates")
    yield s
    s.stop()


def _write(tmp_path, events):
    path = tmp_path / "events.jsonl"
    with path.open("w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")
    return str(path)


def _good(**overrides):
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


@given("a bronze batch with one event missing event_id", target_fixture="batch_path")
def missing_event_id(tmp_path):
    return _write(
        tmp_path,
        [_good(), {"customer_id": "cust_000002", "ts": "2024-01-15T00:00:00+00:00"}],
    )


@given("a bronze batch with two rows sharing event_id e1", target_fixture="batch_path")
def dup_ids(tmp_path):
    return _write(tmp_path, [_good(event_id="e1"), _good(event_id="e1", customer_id="cust_000002")])


@given("a bronze batch referencing customer_id unknown", target_fixture="batch_path")
def unknown_cust(tmp_path):
    return _write(tmp_path, [_good(customer_id="unknown")])


@when("the required_fields gate runs", target_fixture="gate_result")
def run_required(spark, batch_path):
    return gate_required_fields(spark.read.json(batch_path))


@when("the duplicates gate runs", target_fixture="gate_result")
def run_dups(spark, batch_path):
    return gate_duplicates(spark.read.json(batch_path))


@when(
    "the referential_integrity gate runs against known customer cust_000001",
    target_fixture="gate_result",
)
def run_ref(spark, batch_path):
    return gate_referential_integrity(spark.read.json(batch_path), {"cust_000001"})


@then("the gate fails")
def gate_fails(gate_result):
    assert gate_result.passed is False
