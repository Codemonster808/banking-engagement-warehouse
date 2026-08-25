# banking-engagement-warehouse

A dimensional engagement warehouse for bank customer analytics — SCD Type 2 history, data quality gates that block bad loads, and per-pipeline cost/SLA attribution.

## Pitch Card

**Problem** — Bank engagement analytics break silently: a customer changes segment, the dimension overwrites history, and last quarter's cohort report becomes unreproducible — with no one alerted and no cost visibility into which pipeline caused it.

**Solution** — A dimensional warehouse (bronze → silver → gold on Delta Lake) with SCD Type 2 customer history, 14 data-quality gates that **block** promotion on failure instead of just reporting it, and Lambda-based per-pipeline cost attribution.

**Impact** — 100% reproducible historical cohorts across 24 synthetic months, 14 quality gates catching 6 seeded defect classes, SLA breach detected in <5 min, per-pipeline daily cost attribution.

**Stack** — SQL · Python 3 · PySpark / Delta Lake · Flask · AWS (S3, Redshift, Lambda, Step Functions, SNS, SQS, DynamoDB) via LocalStack · Terraform (Azure export stub)

---

## Architecture

```
synthetic banking engagement events (logins, card txns, offers, redemptions)
  → S3 bronze (raw)
  → PySpark + Delta Lake: bronze → silver (cleaned, deduped, typed)
  → silver → gold: dim_customer (SCD Type 2), dim_offer, fact_engagement_daily
  → data quality gates run BEFORE promotion:
       fail → block load, publish to SNS → SQS on-call queue (deduped alerts)
  → Step Functions orchestrates the daily run + an SLA timer
  → Redshift: serving layer for cohort/retention SQL
  → cost attribution Lambda: tag-based per-pipeline $/day → DynamoDB
  → Flask API: /cohort, /sla/status, /cost/by-pipeline
  → terraform/: Azure (ADLS + Data Factory) export stub, plan-only
```

See `docs/architecture.md` for the diagram.

## Why this repo has no Java/Go

Deliberately. This is the SQL and dimensional-modeling core of the portfolio. Adding a JVM or Go worker here would be decoration, not a real runtime justification — unlike [`fintech-txn-integrity-pipeline`](../fintech-txn-integrity-pipeline) or [`delivery-eta-mesh`](../delivery-eta-mesh), where the language boundary maps to a real latency/runtime need.

## Measured in this repo

| Metric | Value | How it's measured |
|---|---|---|
| SCD2 backfill reproducibility (full reprocess, hash-compare) | **byte-identical**, verified | `pytest tests/test_scd2_backfill.py::test_scd2_backfill_is_byte_identical_on_reprocess` |
| Overlapping validity ranges after reprocess | **0** | `pytest tests/test_scd2_backfill.py::test_no_overlapping_validity_ranges_per_customer` |
| Seeded defect classes caught by quality gates | **6/6** | `pytest tests/test_gates.py` (8/8 tests passing) |
| SLA breach alert dedup (2 breaches same day → alerts sent) | **1 sent, 1 deduped** | `pytest tests/test_cost_sla.py::test_sla_breach_sends_one_alert_then_dedups` |
| Gold-layer read time (244 rows, DuckDB over S3 Parquet) | **7 ms** | `make bench` |
| Full test suite | **14/14 passing** | `pytest tests/ -v` |

> Numbers are from a 6-month / 200-customer synthetic run on this machine — `make demo` uses the full 24-month / 5,000-customer dataset the README's business framing describes; `make bench` regenerates these.

## Modeled business impact (synthetic data — assumptions documented)

| Assumption | Source | Modeled outcome |
|---|---|---|
| Analyst hours/month lost to unreproducible cohort reports pre-fix | TODO — cite in `docs/impact-model.md` | TODO |

> Figures are a **model**, not measured production results — see `docs/impact-model.md`.

## Emulated vs. real

| Component | Dev (this repo) | Production would use | Fidelity |
|---|---|---|---|
| S3 / SNS / SQS / Lambda / DynamoDB | [MiniStack](https://ministack.org) (free, MIT, no account) | AWS | High |
| Redshift | **DuckDB**, reading gold Parquet directly from S3 | Redshift Serverless | Medium — no MPP distribution; real `DISTKEY`/`SORTKEY` DDL in `sql/redshift/` |
| Delta Lake | Local Spark + Delta | Databricks | Medium — no Unity Catalog / cluster autoscaling |
| Azure (ADLS + Data Factory) | Terraform `validate` only (no Azure credentials in this repo — `plan` requires a real subscription even for new resources, since the `azurerm` provider authenticates before planning) | Real Azure subscription | Illustrative only — proves IaC literacy and correct resource modeling, not a working second cloud |

## Three non-tutorial challenges

1. **SCD Type 2 under backfill**: reprocess 6 months of history without duplicating dimension versions or corrupting `valid_from`/`valid_to`.
2. **Quality gates that block, not just report**: 6 seeded defect classes (nulls, cardinality drift, referential breaks, duplicates, out-of-range outliers, freshness failures) must each be caught before gold promotion.
3. **Cost attribution on shared resources**: three synthetic pipelines share one cluster/bucket — tag-based allocation rules must split cost fairly and auditably.

## Demo (3 minutes)

```bash
make demo          # 24 synthetic months, bronze → gold
pytest tests/test_scd2_backfill.py   # reprocess + verify history is byte-identical
make query          # cohort retention SQL against Redshift
terraform -chdir=terraform/azure init -backend=false && terraform -chdir=terraform/azure validate   # Azure export stub
```

## What this is NOT

Not a dbt tutorial. What makes it engineering: SCD2 backfill correctness, gates that actually block bad data, and cost attributed to a specific pipeline — not just computed globally.

## Build it yourself

See [`docs/BUILD_GUIDE.md`](docs/BUILD_GUIDE.md).
