# Spec: SCD Type 2 dim_customer

## Objetivo de negocio

Un cambio de segmento no debe reescribir la historia. Los reportes de
cohortes del trimestre pasado deben ser reproducibles.

## Fuentes de entrada

Bronze events (`customer_id`, `segment_at_event`, `ts`).

## Transformaciones

`src/transformation/gold.py:build_dim_customer`: window por customer
ordenado por `event_ts`; transiciones cuando `segment_at_event != lag`;
cierra fila anterior (`valid_to`, `is_current=false`).

## Salida esperada

`s3://bank-gold/dim_customer/`. Una fila vigente por cliente. Rangos
sin overlap. Reproceso byte-idéntico.

## Casos borde

Customer sin cambio de segmento: una sola fila abierta. Backfill de 6
meses no duplica versiones.

## Criterios de aceptación

`features/scd2.feature`, `tests/data_quality/test_scd2_backfill.py`.
