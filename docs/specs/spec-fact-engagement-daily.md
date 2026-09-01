# Spec: fact_engagement_daily

## Objetivo de negocio

Agregar engagement a grano analítico sin re-escanear cada evento en SQL
ad hoc.

## Fuentes de entrada

Silver promovido (`s3a://bank-silver/clean/*/*.json`).

## Transformaciones

`build_fact_engagement_daily`: `event_date` desde `ts`; groupBy
`customer_id`, `event_date`, `event_type`; `n_events`,
`total_amount_cents` (coalesce 0). Cableado en `pipeline.py` en la misma
corrida que `dim_customer`.

## Salida esperada

`s3://bank-gold/fact_engagement_daily/`. Catalog Glue debe listar las
mismas columnas que el builder (`catalog.py` no infiere Parquet).

## Casos borde

Eventos sin `amount_cents` → 0 en el sum. `dim_offer` solo
`offer_shown` / `offer_redeemed`.

## Criterios de aceptación

e2e escribe las tres tablas gold; `docs/RUNBOOK.md` § catálogo vs Spark.
