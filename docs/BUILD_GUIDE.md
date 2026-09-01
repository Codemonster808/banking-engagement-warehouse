# Build Guide — banking-engagement-warehouse

Estimated total: ~26 hours across 2-3 weeks of evenings.

## Glossary

- **Bronze/silver/gold**: a common layering pattern — bronze is raw data, silver is cleaned, gold is business-ready (dimensional) data.
- **SCD Type 2** (Slowly Changing Dimension): a way of storing history in a dimension table by adding a new row (with `valid_from`/`valid_to`) instead of overwriting a changed value.
- **Data quality gate**: an automated check that must pass before data is allowed to move to the next layer.
- **Delta Lake**: a storage format on top of Parquet that adds transactions and time-travel to Spark tables.
- **MiniStack / DuckDB**: see the glossary in `fintech-txn-integrity-pipeline/docs/BUILD_GUIDE.md` — same setup, reused here.

## 0. Before you start (30 min)

```bash
docker --version   # native Docker Engine, not Docker Desktop
python3 --version  # 3.12+
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

```bash
docker compose up -d
curl http://localhost:4566/_health   # expected: no errors
```

## 1. Get the environment running (1 h) → checkpoint: `make check-env`

Same MiniStack setup as `fintech-txn-integrity-pipeline`. Redshift has no free local equivalent — the gold layer is served by DuckDB reading Parquet from S3 (`common/warehouse.py`), copied from that repo.

```bash
docker compose up -d
python3 scripts/bootstrap.py
make check-env   # "OK: services reachable"
```

## 2. Generate synthetic data (2 h) → checkpoint: `make check-data`

Generate 24 months of synthetic events: `login`, `card_txn`, `offer_shown`, `offer_redeemed`, each with a `customer_id` and a `segment` that changes for ~5% of customers each month (this is what SCD2 needs to capture).

```bash
python3 src/ingestion/data_gen.py --months 24 --customers 50000 --out data/bronze/
make check-data   # "OK: 24 months, ~5% segment churn/month confirmed"
```

## 3. Build bronze → silver (3 h) → checkpoint: `make check-silver`

Write `src/transformation/silver.py`: read bronze, dedupe by event ID, cast types, drop clearly malformed rows to a `silver_rejects` path.

```bash
make check-silver   # asserts row counts reconcile: bronze == silver + silver_rejects
```

## 4. Build the 6 quality gates (4-5 h) → checkpoint: `make check-gates`

Implement one gate at a time, in `src/gates/`:
1. Null check on required fields
2. Cardinality drift (segment count changes >20% month over month unexpectedly)
3. Referential integrity (every `fact` row has a matching `dim_customer` row)
4. Duplicate detection post-dedup
5. Out-of-range outliers (negative amounts, future timestamps)
6. Freshness (silver data for "today" must exist before gold runs)

```bash
make check-gates   # inject one bad record per defect class; each must be caught and block promotion
```

**Troubleshooting**
- A gate passes when it shouldn't → check it runs on the *silver* data, not a cached sample.
- Freshness gate always fails locally → confirm your synthetic clock/`--as-of` flag matches "today" in the pipeline config.

## 5. Build gold: SCD Type 2 (4-6 h) → checkpoint: `make check-scd2`

Write `src/transformation/gold.py`: for each customer, if `segment` changed since the last known row, close the old row (`valid_to = event_date`) and insert a new one (`valid_from = event_date, valid_to = null`).

```bash
make check-scd2   # asserts no overlapping valid_from/valid_to ranges for any customer
```

## 6. Prove backfill correctness (3 h) → checkpoint: `pytest tests/data_quality/test_scd2_backfill.py`

Run the full 24 months once. Save a hash of `dim_customer`. Delete and reprocess months 1-24 again from bronze. Compare hashes.

```bash
pytest tests/data_quality/test_scd2_backfill.py   # must pass: reprocessed history is byte-identical
```

This is the single most important checkpoint in the repo — do not skip it.

## 7. Build cost attribution + SLA (3 h) → checkpoint: `make check-cost`

Tag each pipeline run with a `pipeline_id`. The cost Lambda estimates $/day per tag from MiniStack request counts and object sizes, writes to DynamoDB. The SLA timer in Step Functions fires an SNS alert (deduped in SQS) if the daily run exceeds a threshold.

```bash
make check-cost   # asserts cost rows exist per pipeline_id and SLA alert fires when run is artificially slowed
```

## 8. Terraform Azure stub (1-2 h)

Write a minimal `terraform/azure/main.tf` mapping one bronze→silver step to ADLS + Data Factory equivalents. Never run `apply` — this is a literacy stub, not a working second cloud.

```bash
terraform -chdir=terraform/azure init -backend=false
terraform -chdir=terraform/azure validate   # should print "Success! The configuration is valid."
```

**Note:** `terraform plan` (not just `validate`) genuinely requires a real Azure subscription — the `azurerm` provider authenticates against Azure AD before it can plan anything, even brand-new resources. Without real credentials, `plan` fails with an auth error, not a clean plan. `validate` (schema/syntax correctness, no network call) is what this repo can honestly demonstrate without an Azure account; if you have one, `plan` will work too.

## 9. Measure, model, ship (3 h)

```bash
make bench
```
Fill `docs/impact-model.md` with real sources, then the README's two metric tables.

## Troubleshooting index

| Symptom | Likely cause | Fix |
|---|---|---|
| SCD2 hash mismatch on reprocess | non-deterministic ordering in the Spark job | add an explicit `orderBy` before assigning `valid_from` |
| Gate false positives on every run | threshold too tight for synthetic data's natural variance | widen the threshold, document why in the gate's docstring |

## Total estimated effort: ~26 hours (2-3 weeks of evenings)
