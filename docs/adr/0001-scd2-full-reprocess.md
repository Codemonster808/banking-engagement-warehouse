# ADR 0001 — SCD2 full reprocess vs Type 1 vs MERGE incremental

## Contexto

El segmento del cliente cambia. Type 1 borra historia. Snapshots
crecen sin bound.

## Decisión

SCD Type 2 con window functions sobre todo el bronze de la ventana.
Reprocesar desde cero es la garantía de byte-idéntico en este repo.

## Alternativas consideradas

- **Type 1 overwrite**: cohortes del pasado mienten.
- **Snapshot diario completo**: simple, almacenamiento y queries peores.
- **`MERGE INTO` incremental**: correcto en producción a escala; aquí el
  dataset cabe en un `local[2]` y el test de reproducibilidad es más
  fuerte con rebuild total. El límite está nombrado, no oculto.

## Consecuencias

Costo de compute lineal con meses. No fingir un merge incremental en
README.
