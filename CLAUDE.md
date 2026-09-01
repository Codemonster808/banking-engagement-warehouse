# CLAUDE.md — banking-engagement-warehouse

Operating constitution. Architecture lives in `docs/architecture.md` and
`docs/adr/`. Read specs and features before changing pipeline behavior.

## 1. Domain context

Dimensional engagement warehouse: bronze → silver → gold. "Correct" means:

- **Exactly one `is_current=true` row per customer** in `dim_customer`.
  Validity ranges never overlap. A segment change closes the previous row
  (`valid_to`, `is_current=false`) and opens a new one. Full reprocess of
  the same bronze must be **byte-identical**
  (`tests/data_quality/test_scd2_backfill.py`).
- **Six quality gates block silver promotion** — they do not merely alert.
  A failing month revokes that month's prior silver output. Gates live in
  `src/models/gates.py` (required fields, duplicates, referential
  integrity, range/segment outliers, cardinality drift >30 pp, freshness).
- **Gold is three tables in one `pipeline.py` run**: `dim_customer`,
  `fact_engagement_daily` (grain: customer × day × `event_type`),
  `dim_offer`. `facts.py` remains a standalone CLI for ad-hoc rebuilds.
- **SLA** `SLA_SECONDS=180`; **cost** `COST_PER_GB=0.023`. A second SLA
  breach the same `pipeline_id#run_date` must not double-alert
  (`sla-alert-dedup`).

## 2. Exact commands

Every Makefile recipe: `set -a && source ./env.sh --quiet && set +a`.
Host MiniStack port is `MINISTACK_PORT` / `AWS_ENDPOINT_URL` (default
local `http://localhost:4583` so this repo can run next to the others).

```bash
source env.sh
docker compose up -d
make check-env
make demo              # 3 months × 200 customers + catalog
make demo-full         # 24 × 5000 — slow
make test              # generates bronze seed 42, pytest ignoring e2e
make e2e
make catalog
make terraform-validate
make bench
make query
```

## 3. Naming conventions

**Buckets:** `bank-bronze`, `bank-silver` (`clean/month=NN/`), `bank-gold`
(`dim_customer/`, `fact_engagement_daily/`, `dim_offer/`), rejects on gate fail.

**DDB:** `pipeline-cost`, `sla-alert-dedup` (`alert_id` =
`{pipeline_id}#{run_date}`), `pipeline-run-timer`.

**Glue:** database `bank_gold` — schemas in `catalog.py` are copied by
hand from Spark builders (intentional drift detection).

## 4. Schema and data rules

Synthetic only (`--seed 42`). `VALID_SEGMENTS` / `VALID_EVENT_TYPES` in
`gates.py` are the contract. Do not introduce real PII.

## 5. Do not touch without asking

`.env`; `LLM_PROVIDER=minimax`; deleting MiniStack buckets/tables by
hand (use `scripts/bootstrap.py`); `scripts/iam_setup.py` /
`scripts/secrets_setup.py` if present.

## 6. Specs and features

`docs/specs/`, `docs/adr/`, `features/*.feature` (pytest-bdd).

Notebooks and dbt folders are intentionally absent — exploration does
not feed production jobs.
