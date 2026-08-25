import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from cost_sla import check_sla, record_pipeline_run  # noqa: E402


def test_cost_recorded_per_pipeline():
    pipeline_id = f"test-pipeline-{uuid.uuid4()}"
    result = record_pipeline_run(pipeline_id, "2024-01-01", bytes_processed=1_073_741_824, duration_seconds=10)
    assert result["cost_usd"] == 0.023  # exactly 1 GiB at $0.023/GB


def test_sla_not_breached_under_limit():
    result = check_sla("pipeline-x", "2024-01-01", duration_seconds=30, sla_seconds=60)
    assert result["breached"] is False


def test_sla_breach_sends_one_alert_then_dedups():
    pipeline_id = f"test-sla-{uuid.uuid4()}"
    first = check_sla(pipeline_id, "2024-01-01", duration_seconds=120, sla_seconds=60)
    assert first["breached"] is True
    assert first["alert_sent"] is True

    second = check_sla(pipeline_id, "2024-01-01", duration_seconds=125, sla_seconds=60)
    assert second["breached"] is True
    assert second["alert_sent"] is False
    assert "deduped" in second["reason"]
