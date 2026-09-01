# Spec: cost and SLA attribution

## Objetivo de negocio

Varios pipelines comparten bucket/cluster. Hay que saber $/día y si el
run pasó de 180 s, con alerta deduplicada por día.

## Fuentes de entrada

Bytes procesados, `run_id`, `pipeline_id`, `run_date`.

## Transformaciones

`cost_sla.py`: `cost_usd = GiB * COST_PER_GB` (0.023).
`pipeline.py` llama `check_sla(..., sla_seconds=SLA_SECONDS)` (180).
Dedupe de alerta: `alert_id = "{pipeline_id}#{run_date}"`.

## Salida esperada

1 GiB → exactamente 0.023. Segunda brecha el mismo día → 0 alerts extra.

## Criterios de aceptación

`features/sla-cost.feature`, `tests/integration/test_cost_sla.py`.
