"""
Lambda-shaped handler: reads the run's start time, compares elapsed time
against the SLA, and — on breach — publishes a deduped SNS alert (same
conditional-write dedup pattern as cost_sla.check_sla, reimplemented
here as a self-contained Lambda handler since a Lambda zip can't import
the rest of this repo's src/ package).
"""
import time

import boto3

RUN_TABLE = "pipeline-run-timer"
ALERT_DEDUP_TABLE = "sla-alert-dedup"
ALERT_TOPIC = "quality-alerts"


def handler(event, context):
    endpoint = "http://127.0.0.1:4566"
    ddb = boto3.client("dynamodb", endpoint_url=endpoint, region_name="us-east-1")
    sns = boto3.client("sns", endpoint_url=endpoint, region_name="us-east-1")

    run_id = event["run_id"]
    sla_seconds = event.get("sla_seconds", 180)

    item = ddb.get_item(TableName=RUN_TABLE, Key={"run_id": {"S": run_id}}).get("Item")
    if not item:
        return {"breached": False, "reason": "no start time recorded"}

    started_at = int(item["started_at"]["N"])
    elapsed = int(time.time()) - started_at

    if elapsed <= sla_seconds:
        return {"breached": False, "elapsed_seconds": elapsed}

    dedup_key = f"{run_id}#sla"
    try:
        ddb.put_item(
            TableName=ALERT_DEDUP_TABLE,
            Item={"alert_id": {"S": dedup_key}},
            ConditionExpression="attribute_not_exists(alert_id)",
        )
    except ddb.exceptions.ConditionalCheckFailedException:
        return {"breached": True, "alert_sent": False, "elapsed_seconds": elapsed}

    topic_arn = sns.create_topic(Name=ALERT_TOPIC)["TopicArn"]
    sns.publish(TopicArn=topic_arn, Message=f"SLA breach: run {run_id} took {elapsed}s (limit {sla_seconds}s)")
    return {"breached": True, "alert_sent": True, "elapsed_seconds": elapsed}
