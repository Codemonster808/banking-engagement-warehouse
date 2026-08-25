"""Lambda-shaped handler: records the daily pipeline run's start time in DynamoDB."""
import time

import boto3

RUN_TABLE = "pipeline-run-timer"


def handler(event, context):
    endpoint = "http://127.0.0.1:4566"
    ddb = boto3.client("dynamodb", endpoint_url=endpoint, region_name="us-east-1")

    run_id = event["run_id"]
    started_at = int(time.time())
    ddb.put_item(
        TableName=RUN_TABLE,
        Item={"run_id": {"S": run_id}, "started_at": {"N": str(started_at)}},
    )
    return {"run_id": run_id, "started_at": started_at}
