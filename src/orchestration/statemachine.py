#!/usr/bin/env python3
"""
Deploys the Lambdas + Step Functions state machines that orchestrate the
daily pipeline run's SLA timer, and provides mark_started()/check_sla()
for src/pipeline.py to call at the start and end of a run. Real Choice/
Retry/Catch (asl/daily_pipeline_sla_check.json) around the SLA decision,
same pattern as fintech-txn-integrity-pipeline's daily job and
agentic-claims-copilot's budget gate — Step Functions orchestrates the
control flow, not the Spark compute that runs in between.
"""

import json
import sys
import time
import zipfile
from io import BytesIO
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils import aws  # noqa: E402

LAMBDAS_DIR = Path(__file__).resolve().parent / "lambdas"
ASL_DIR = Path(__file__).resolve().parents[2] / "asl"
ROLE_ARN = "arn:aws:iam::000000000000:role/dummy-role"

FUNCTIONS = {
    "bank-mark-started": "mark_started.py",
    "bank-check-sla": "check_sla_lambda.py",
}
STATE_MACHINES = {
    "bank-daily-pipeline-sla": "daily_pipeline_sla.json",
    "bank-daily-pipeline-sla-check": "daily_pipeline_sla_check.json",
}


def _zip_handler(file_name: str) -> bytes:
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.write(LAMBDAS_DIR / file_name, arcname=file_name)
    return buf.getvalue()


def _wait_active(lam, fn_name: str, timeout_s: float = 20) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if lam.get_function(FunctionName=fn_name)["Configuration"]["State"] == "Active":
            return
        time.sleep(0.5)
    raise TimeoutError(f"Lambda {fn_name} did not become Active in time")


def deploy() -> dict[str, str]:
    lam = aws.client("lambda")
    sfn = aws.client("stepfunctions")

    for fn_name, file_name in FUNCTIONS.items():
        zip_bytes = _zip_handler(file_name)
        handler = f"{file_name[:-3]}.handler"
        existing = {f["FunctionName"] for f in lam.list_functions().get("Functions", [])}
        if fn_name in existing:
            lam.update_function_code(FunctionName=fn_name, ZipFile=zip_bytes)
        else:
            lam.create_function(
                FunctionName=fn_name,
                Runtime="python3.12",
                Role=ROLE_ARN,
                Handler=handler,
                Code={"ZipFile": zip_bytes},
            )
        _wait_active(lam, fn_name)

    arns = {}
    existing_sms = {
        sm["name"]: sm["stateMachineArn"] for sm in sfn.list_state_machines()["stateMachines"]
    }
    for sm_name, asl_file in STATE_MACHINES.items():
        definition = (ASL_DIR / asl_file).read_text()
        if sm_name in existing_sms:
            sfn.update_state_machine(stateMachineArn=existing_sms[sm_name], definition=definition)
            arns[sm_name] = existing_sms[sm_name]
        else:
            resp = sfn.create_state_machine(name=sm_name, definition=definition, roleArn=ROLE_ARN)
            arns[sm_name] = resp["stateMachineArn"]
    return arns


_arn_cache: dict[str, str] | None = None


def _run_execution(sfn, arn: str, input_dict: dict, timeout_s: float = 15) -> dict:
    exec_resp = sfn.start_execution(stateMachineArn=arn, input=json.dumps(input_dict))
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        desc = sfn.describe_execution(executionArn=exec_resp["executionArn"])
        if desc["status"] != "RUNNING":
            return desc
        time.sleep(0.3)
    raise TimeoutError("execution did not finish in time")


def mark_started(run_id: str) -> None:
    global _arn_cache
    if _arn_cache is None:
        _arn_cache = deploy()
    sfn = aws.client("stepfunctions")
    _run_execution(sfn, _arn_cache["bank-daily-pipeline-sla"], {"run_id": run_id})


def check_sla(run_id: str, sla_seconds: int = 180) -> dict:
    global _arn_cache
    if _arn_cache is None:
        _arn_cache = deploy()
    sfn = aws.client("stepfunctions")
    desc = _run_execution(
        sfn,
        _arn_cache["bank-daily-pipeline-sla-check"],
        {"run_id": run_id, "sla_seconds": sla_seconds},
    )
    if desc["status"] != "SUCCEEDED":
        raise RuntimeError(f"SLA check execution failed: {desc}")
    return json.loads(desc.get("output", "{}"))


if __name__ == "__main__":
    arns = deploy()
    print(json.dumps(arns, indent=2))
