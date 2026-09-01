# ADR 0002 — Gates que bloquean vs gates que alertan

## Contexto

Calidad "en el dashboard" llega tarde: gold ya está sucio.

## Decisión

`run_all_gates` antes de escribir silver. Un fail no promociona el mes
y revoca silver previo de ese mes.

## Alternativas consideradas

- **Solo SNS/alert**: el analista ve el ticket cuando el cohort ya salió.
- **Quarantine parcial de filas**: más fiel a fintech; aquí el grano es
  el mes de carga bancaria, no el evento de pago.

## Consecuencias

Un mes malo deja hueco en silver a propósito. Completeness del warehouse
es "meses que pasaron gates", no "todos los archivos bronze".
