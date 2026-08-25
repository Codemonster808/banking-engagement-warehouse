# Architecture

```mermaid
flowchart LR
    EVT[Synthetic engagement events\nlogin, txn, offer, redemption] --> BRONZE[(S3 bronze\nraw)]
    BRONZE --> SILVER_JOB[PySpark: clean, dedupe, type]
    SILVER_JOB --> SILVER[(Delta silver)]
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
