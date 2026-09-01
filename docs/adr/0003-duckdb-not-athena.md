# ADR 0003 — DuckDB como stand-in de Redshift (no Athena)

## Contexto

Hay que servir SQL sobre Parquet gold en local.

## Decisión

DuckDB + httpfs (`utils/warehouse.py`). DDL `DISTKEY`/`SORTKEY` vive en
`sql/` como contrato de prod.

## Alternativas consideradas

- **Athena sobre MiniStack**: `get-query-results` es mock hardcodeado —
  no se puede afirmar "query real".
- **Postgres local**: otro motor, otro dialecto, no demuestra Spectrum/COPY.

## Consecuencias

No hay MPP. Números de latencia son DuckDB, no Redshift. Fidelity table
en el README lo dice.
