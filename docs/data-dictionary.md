# Data dictionary — banking-engagement-warehouse

All names from `scripts/resources.json` / Spark writers. Synthetic data only.

## Bronze / silver / gold (S3)

| Location | Grain | Lineage |
|---|---|---|
| `s3://bank-bronze/` | raw monthly JSON events | `src/ingestion/upload_bronze.py` |
| `s3://bank-silver/clean/month=NN/` | typed, gated events | `src/transformation/silver.py` after all 6 gates pass |
| `s3://bank-gold/dim_customer/` | one row per customer-segment version | `gold.py:build_dim_customer` |
| `s3://bank-gold/fact_engagement_daily/` | customer × date × event_type | `facts.py:build_fact_engagement_daily` via `pipeline.py` |
| `s3://bank-gold/dim_offer/` | offer event types | `facts.py:build_dim_offer` |

### `dim_customer` columns

`customer_id`, `segment`, `valid_from`, `valid_to`, `is_current` — see
`catalog.py` (authoritative for Glue; must match Spark).

### `fact_engagement_daily` columns

`customer_id` (string), `event_date` (date), `event_type` (string),
`n_events` / `total_amount_cents` (bigint).

## DynamoDB

| Table | Key | Purpose |
|---|---|---|
| `pipeline-cost` | pipeline / run | $/day attribution |
| `sla-alert-dedup` | `alert_id` = `{pipeline_id}#{run_date}` | one SLA page per pipeline-day |
| `pipeline-run-timer` | run_id | SLA stopwatch |

## Glue

Database `bank_gold`. Tables registered by `src/transformation/catalog.py`.
