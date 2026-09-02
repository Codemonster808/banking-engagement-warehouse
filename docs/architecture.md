# Architecture

## ASCII — execution flow

```
  synthetic engagement events (login, card txn, offer, redemption)
             |
             v
  src/ingestion/data_gen.py --> src/ingestion/upload_bronze.py
             |
             v
        S3 bronze (raw)
             |
             v
  src/transformation/silver.py (PySpark)
    clean, dedupe, type -- plain Parquet/JSON, not Delta Lake
             |
             v
        S3 silver
             |
             v
  src/models/gates.py -- 6 data-quality gates, driven by
  src/transformation/pipeline.py
             |
      +------+------+
      |             |
    fail           pass
      |             |
      v             v
   SNS quality-alert   src/transformation/gold.py --> dim_customer (SCD Type 2)
      |                src/transformation/facts.py --> fact_engagement_daily, dim_offer
      v                     |
   SQS on-call queue        v
   (deduped alerts)   src/utils/warehouse.py :: DuckDB (Redshift stand-in)
                             |
                             v
                       src/serving/api.py :: Flask
                         /cohort  /sla/status  /cost/by-pipeline

  src/orchestration/statemachine.py
    orchestrates the daily run + an SLA timer via Lambdas:
    src/orchestration/lambdas/mark_started.py
    src/orchestration/lambdas/check_sla_lambda.py
             |
             v
  src/orchestration/cost_sla.py
    tag-based per-pipeline $/day --> DynamoDB cost table
```

## Mermaid (same flow)

```mermaid
flowchart LR
    EVT[Synthetic engagement events\nlogin, txn, offer, redemption] --> BRONZE[(S3 bronze\nraw)]
    BRONZE --> SILVER_JOB[PySpark: clean, dedupe, type]
    SILVER_JOB --> SILVER[(S3 silver\nplain JSON, not Delta)]
    SILVER --> QGATE{Quality gates\n6 defect classes}
    QGATE -->|fail| SNS[SNS: quality-alert]
    SNS --> SQSQ[SQS: on-call queue\nderuped]
    QGATE -->|pass| GOLD_JOB[PySpark: build dims + facts]
    GOLD_JOB --> DIMCUST[(dim_customer\nSCD Type 2)]
    GOLD_JOB --> FACT[(fact_engagement_daily)]
    DIMCUST --> RS[(Redshift serving layer)]
    FACT --> RS
    RS --> API[Flask API: /cohort /sla/status /cost/by-pipeline]
    SF[Step Functions: daily orchestration + SLA timer] --> SILVER_JOB
    SF --> GOLD_JOB
    COST[Cost attribution Lambda\ntag-based] --> DDB[(DynamoDB\ncost table)]
    API --> DDB
```

## Data flow notes

- Quality gates sit **between** silver and gold — nothing reaches the dimensional model without passing all 6 checks.
- `dim_customer` is the only SCD Type 2 table; `fact_engagement_daily` is append-only and joins to whichever dimension version was valid at event time.
- The cost attribution Lambda runs independently of the daily pipeline, tagging S3/Redshift usage by pipeline label and writing daily deltas to DynamoDB.
