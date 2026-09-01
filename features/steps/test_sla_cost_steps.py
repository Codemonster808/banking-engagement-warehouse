"""
Step definitions for features/sla-cost.feature.

Wraps the real setup/assertions from tests/integration/test_cost_sla.py,
calling the actual orchestration.cost_sla functions against MiniStack.
"""
import sys
import uuid
from pathlib import Path

from pytest_bdd import given, scenarios, then, when

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from orchestration.cost_sla import check_sla, record_pipeline_run  # noqa: E402

scenarios("../sla-cost.feature")


@given("a pipeline run that processed 1 GiB in 10 seconds", target_fixture="cost_pipeline_id")
def gib_pipeline_run():
    return f"test-pipeline-{uuid.uuid4()}"


@when("the pipeline run is recorded", target_fixture="cost_result")
def record_run(cost_pipeline_id):
    return record_pipeline_run(
        cost_pipeline_id, "2024-01-01", bytes_processed=1_073_741_824, duration_seconds=10
    )


@then("the recorded cost is exactly 0.023 USD")
def cost_is_exact(cost_result):
    assert cost_result["cost_usd"] == 0.023  # exactly 1 GiB at $0.023/GB


@given("a pipeline that breached its 60 second SLA by running for 120 seconds", target_fixture="sla_pipeline_id")
def breaching_pipeline():
    return f"test-sla-{uuid.uuid4()}"


@when("the SLA breach is checked for the first time", target_fixture="first_check")
def first_check(sla_pipeline_id):
    return check_sla(sla_pipeline_id, "2024-01-01", duration_seconds=120, sla_seconds=60)


@then("the breach is detected and an alert is sent")
def breach_alert_sent(first_check):
    assert first_check["breached"] is True
    assert first_check["alert_sent"] is True


@when("the same pipeline breaches its SLA again for the same run_date", target_fixture="second_check")
def second_check(sla_pipeline_id, first_check):
    return check_sla(sla_pipeline_id, "2024-01-01", duration_seconds=125, sla_seconds=60)


@then("the breach is detected but the alert is deduped")
def breach_deduped(second_check):
    assert second_check["breached"] is True
    assert second_check["alert_sent"] is False
    assert "deduped" in second_check["reason"]
