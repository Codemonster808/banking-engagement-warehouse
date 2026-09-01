# Spec: quality gates

## Objetivo de negocio

Promover bronze→silver con defectos silencia cohortes. Los 6 gates
**bloquean** el mes; no son dashboards.

## Fuentes de entrada

JSON bronze mensual. `known_customer_ids`. Shares de segmento del mes
previo. `expected_month` (`yyyy-MM`).

## Transformaciones

`src/models/gates.py:run_all_gates`: required_fields, duplicates,
referential_integrity, range_outliers (amount negativo o segmento fuera de
`VALID_SEGMENTS`), cardinality_drift (más de 0.30 pp vs mes previo),
freshness (cero filas del mes esperado).

## Salida esperada

Lista de `GateResult`. Un fail → no `bank-silver/clean/month=NN/`.

## Casos borde

Sin mes previo: drift pasa con detail `no prior month`. Segmento
`dormant` es válido; `vip` no.

## Criterios de aceptación

`features/quality-gates.feature`, `tests/unit/test_gates.py` (6/6 clases).
