#!/usr/bin/env python3
"""Idempotent creation of the AWS resources this repo needs, against MiniStack."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from common import aws  # noqa: E402

BUCKETS = ["bank-bronze", "bank-silver", "bank-gold"]
ALERT_TOPIC = "quality-alerts"
ALERT_QUEUE = "quality-oncall-queue"
COST_TABLE = "pipeline-cost"
ALERT_DEDUP_TABLE = "sla-alert-dedup"
RUN_TIMER_TABLE = "pipeline-run-timer"


def ensure_bucket(s3, name: str) -> None:
    existing = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
    if name not in existing:
        s3.create_bucket(Bucket=name)
        print(f"  created bucket: {name}")
    else:
        print(f"  bucket already exists: {name}")


def ensure_queue(sqs, name: str) -> str:
    try:
        url = sqs.get_queue_url(QueueName=name)["QueueUrl"]
        print(f"  queue already exists: {name}")
        return url
    except sqs.exceptions.QueueDoesNotExist:
        url = sqs.create_queue(QueueName=name)["QueueUrl"]
        print(f"  created queue: {name}")
        return url


def ensure_topic(sns, name: str) -> str:
    arn = sns.create_topic(Name=name)["TopicArn"]
    print(f"  topic ready: {arn}")
    return arn


def ensure_subscription(sns, sqs, topic_arn: str, queue_url: str) -> None:
    queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    existing = sns.list_subscriptions_by_topic(TopicArn=topic_arn)["Subscriptions"]
    if any(s["Endpoint"] == queue_arn for s in existing):
        print(f"  subscription already exists: {queue_arn}")
        return
    sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn, Attributes={"RawMessageDelivery": "true"})
    print(f"  subscribed {queue_arn} -> {topic_arn}")


def ensure_table(dynamodb, table_name: str, key_name: str) -> None:
    existing = dynamodb.list_tables()["TableNames"]
    if table_name in existing:
        print(f"  table already exists: {table_name}")
        return
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": key_name, "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": key_name, "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    print(f"  created table: {table_name}")


def main() -> None:
    print("S3 buckets:")
    s3 = aws.client("s3")
    for bucket in BUCKETS:
        ensure_bucket(s3, bucket)

    print("SQS queue:")
    sqs = aws.client("sqs")
    queue_url = ensure_queue(sqs, ALERT_QUEUE)

    print("SNS topic + subscription:")
    sns = aws.client("sns")
    topic_arn = ensure_topic(sns, ALERT_TOPIC)
    ensure_subscription(sns, sqs, topic_arn, queue_url)

    print("DynamoDB tables:")
    ddb = aws.client("dynamodb")
    ensure_table(ddb, COST_TABLE, "pipeline_id")
    ensure_table(ddb, ALERT_DEDUP_TABLE, "alert_id")
    ensure_table(ddb, RUN_TIMER_TABLE, "run_id")

    print("Bootstrap complete.")


if __name__ == "__main__":
    main()
