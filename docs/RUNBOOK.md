# Runbook — aprender el warehouse (P2)

Complementa `docs/BUILD_GUIDE.md`. Spark es más lento de observar que P1: usa la escala chica (3 meses × 200 clientes). `make demo-full` es 24×5000 — solo para métricas del README.

---

## 0. Setup por terminal

```bash
cd /home/lesaint/Documentos/life_plans/banking-engagement-warehouse
source env.sh
docker compose up -d
make check-env
python3 scripts/bootstrap.py
python3 scripts/aws_inspect.py all
```

Si otro repo dejó MiniStack arriba, está bien. Si P1 dejó el gate en :8080, no estorba aquí (P2 no usa ese puerto). Igual: un Spark a la vez.

---

## 1. Flujo paso a paso

### 1.1 Datos bronze locales

```bash
python3 src/ingestion/data_gen.py --months 3 --customers 200 --out data --seed 42
ls data/month=*.jsonl
python3 -c "print(sum(1 for _ in open('data/month=00.jsonl')), 'events in month 00')"
```

También se escribe `_ground_truth_segments.json` — eso es lo que SCD2 tiene que reconstruir.

### 1.2 Subir a S3 (todavía no hay silver)

```bash
python3 src/ingestion/upload_bronze.py --in data
python3 scripts/aws_inspect.py s3
```

**Qué inspeccionar:** `bank-bronze` con keys `month=00.jsonl`, `month=01.jsonl`, `month=02.jsonl`. `bank-silver` y `bank-gold` vacíos.

### 1.3 Pipeline: gates → silver → gold

```bash
python3 src/transformation/pipeline.py --customers 200
python3 scripts/aws_inspect.py s3
python3 scripts/aws_inspect.py sqs
python3 src/transformation/catalog.py     # registra dim_customer/fact_engagement_daily/dim_offer en Glue
make query
```

`pipeline.py` ya construye las tres tablas gold en la misma corrida — `dim_customer` (SCD2), `fact_engagement_daily` y `dim_offer` — sobre los mismos meses promovidos a silver; `src/transformation/facts.py` ya no es un paso separado obligatorio, queda como CLI standalone por si quieres reconstruir solo esas dos tablas con un `--silver-glob` distinto sin rehacer gates/silver/dim_customer.

`--customers` **tiene que coincidir** con data_gen. Si pones 5000 sobre un gen de 200, el gate de integridad referencial ve “huérfanos” al revés (IDs conocidos de más no fallan; IDs en eventos fuera del set sí). Usa el mismo número.

**Qué inspeccionar:** `bank-silver/clean/month=00/` existe solo si los 6 gates pasaron. `bank-gold/dim_customer/`, `bank-gold/fact_engagement_daily/` y `bank-gold/dim_offer/` se escriben todos por `pipeline.py`. `src/transformation/catalog.py` no lee ni valida el Parquet — solo registra el schema que ya sabemos que Spark escribió (`src/transformation/gold.py`/`src/transformation/facts.py`); si esos cambian, `src/transformation/catalog.py` hay que actualizarlo a mano, no se infiere.

---

## 2. Explorar con AWS CLI

`aws` respeta `AWS_ENDPOINT_URL` (exportado por `env.sh`) sin flags extra.

```bash
# S3 — capas bronze/silver/gold, sin el resumen de aws_inspect.py
aws s3 ls s3://bank-bronze/
aws s3 ls s3://bank-silver/ --recursive
aws s3 ls s3://bank-gold/dim_customer/ --recursive

# SNS — quién está suscrito a las alertas de calidad
TOPIC_ARN=$(aws sns list-topics --query "Topics[?contains(TopicArn,'quality-alerts')].TopicArn" --output text)
aws sns list-subscriptions-by-topic --topic-arn "$TOPIC_ARN"

# SQS — la cola de on-call, después de romper un mes a propósito (sección 2)
QUEUE_URL=$(aws sqs get-queue-url --queue-name quality-oncall-queue --query QueueUrl --output text)
aws sqs get-queue-attributes --queue-url "$QUEUE_URL" --attribute-names All

# DynamoDB — costo y timers por corrida, no solo el conteo de aws_inspect.py
aws dynamodb scan --table-name pipeline-cost --max-items 5
aws dynamodb scan --table-name pipeline-run-timer --max-items 5

# Lambda — mark_started / check_sla, invocados desde src/orchestration/statemachine.py
aws lambda get-function --function-name bank-check-sla --query 'Configuration.[State,Runtime]'

# Step Functions — dos máquinas: una marca el inicio, otra evalúa el SLA
aws stepfunctions list-state-machines --query "stateMachines[*].name" --output text
SM_ARN=$(aws stepfunctions list-state-machines --query "stateMachines[?name=='bank-daily-pipeline-sla-check'].stateMachineArn | [0]" --output text)
aws stepfunctions list-executions --state-machine-arn "$SM_ARN" --max-results 3

# Glue Data Catalog — el schema real del gold layer, no un path + suposición
aws glue get-databases --query 'DatabaseList[*].Name'
aws glue get-tables --database-name bank_gold --query 'TableList[*].Name'
aws glue get-table --database-name bank_gold --name dim_customer --query 'Table.StorageDescriptor.[Location,Columns]'
```

**Qué mirar que `aws_inspect.py` no te muestra:** el `Body` real de los mensajes en `quality-oncall-queue` (la razón del bloqueo, no solo el conteo), y `pipeline-run-timer` en DynamoDB — el reloj que usa `check_sla_lambda.py` para decidir si hubo breach.

---

## 3. Romper a propósito — un mes bloqueado

Copia un mes, corrompe `customer_id` a null en algunas filas, re-sube y re-corre:

```bash
python3 - <<'PY'
from pathlib import Path
import json
src = Path("data/month=01.jsonl")
dst = Path("data/month=01.corrupt.jsonl")
out = []
for i, line in enumerate(src.read_text().splitlines()):
    e = json.loads(line)
    if i < 20:
        e["customer_id"] = None
    out.append(json.dumps(e))
dst.write_text("\n".join(out) + "\n")
print("wrote", dst, "with 20 null customer_ids")
PY

# replace the bronze object
python3 -c "
from pathlib import Path
import sys
sys.path.insert(0,'src')
from common import aws
s3 = aws.client('s3')
s3.upload_file('data/month=01.corrupt.jsonl', 'bank-bronze', 'month=01.jsonl')
print('uploaded corrupt month=01')
"

python3 src/transformation/pipeline.py --customers 200
python3 scripts/aws_inspect.py s3
python3 scripts/aws_inspect.py sqs
```

**Qué debe pasar:**
- Gate `required_fields` falla para `month=01`
- Ese mes **no** se promociona a silver (`revoke_silver_promotion` borra silver stale si había corrida previa OK)
- SNS publica a `quality-alerts` → cola `quality-oncall-queue` (visible ≥ 1)

Luego restaura el archivo bueno con `python3 src/ingestion/upload_bronze.py --in data` (el jsonl original sigue en `data/month=01.jsonl`).

---

## 4. Errores

| Error | Significado |
|---|---|
| `QueueDoesNotExist` | No hiciste `source env.sh` o `bootstrap.py` |
| Spark OOM / lento | Escala demasiado grande o otro repo usando RAM. `demo` chico; `docker compose down` en el otro. |
| `Can not create a Path from an empty string` | Parquet en la raíz del bucket — el pipeline ya usa subpaths. |
| Gates pasan pero gold “viejo” | Bug que ya se corrigió con `revoke_silver_promotion`. Si lo ves, silver stale. |

---

## 5. Ejercicios

**1. Lee la razón real del bloqueo, no solo que existe**

Provoca el mes corrupto (sección 3), luego `aws sqs receive-message --queue-url $QUEUE_URL --max-number-of-messages 1` sobre `quality-oncall-queue`.

<details><summary>Verificar</summary>

El `Body` es el mensaje literal que publicó `pipeline.py` vía SNS (`sns.publish(... Message=f"Quality gate blocked promotion of {month}: {reasons}")`) — incluye qué gate falló (`required_fields`) y qué mes. En un on-call real, este es el mensaje que verías en tu cliente SQS/Slack antes de tocar código.
</details>

**2. Confirma con DynamoDB, no con logs, si un mes ya se promovió**

Corre el pipeline dos veces sobre el mismo mes limpio y compara `aws dynamodb scan --table-name pipeline-run-timer` antes y después de la segunda corrida.

<details><summary>Verificar</summary>

`pipeline-run-timer` tiene una fila por `run_id`, con timestamps de inicio/fin — la segunda corrida crea una fila nueva, no sobrescribe la primera. Si buscas "¿cuánto tarda esto normalmente?" para calibrar una alerta SLA real, este historial —no los logs de consola— es la fuente de verdad.
</details>

**3. Sigue una ejecución del gate SLA paso a paso con el CLI**

`aws stepfunctions get-execution-history` sobre la ejecución más reciente de **`bank-daily-pipeline-sla-check`** (ojo: no `bank-daily-pipeline-sla` — esa solo marca el inicio, sin `Choice`) — identifica el `Choice` que decide si hubo breach.

<details><summary>Verificar</summary>

`bank-daily-pipeline-sla-check` sí tiene un `ChoiceStateEntered`/`ChoiceStateExited` después del `TaskSucceeded` (invocación a `bank-check-sla`) — esa es la rama que decide breach o no, basada en `elapsed_seconds` vs `SLA_SECONDS`. `bank-daily-pipeline-sla` (sin "-check") solo corre `mark_started`: un único `Task`, sin `Choice` — confírmalo comparando el historial de las dos máquinas por nombre exacto, no por substring.
</details>

**4. Compara el schema de Glue contra el código Spark que realmente escribió el Parquet, no al revés**

`aws glue get-table --database-name bank_gold --name fact_engagement_daily --query 'Table.StorageDescriptor.Columns'`, y compáralo columna por columna contra `build_fact_engagement_daily` en `src/transformation/facts.py`.

<details><summary>Verificar</summary>

Deberían coincidir exactamente: `customer_id`/`event_date`/`event_type`/`n_events`/`total_amount_cents` — porque `src/transformation/catalog.py` no infiere el schema del Parquet, lo copia a mano de los mismos builders de Spark. Eso es una decisión deliberada, no pereza: si mañana alguien agrega una columna en `facts.py` y no toca `catalog.py`, el catálogo queda desincronizado — y **ese** es justo el tipo de drift que un catálogo de datos real está para exponer, no para ocultar. Prueba el caso: agrega una columna falsa a `build_dim_offer` sin tocar `catalog.py`, corre `make demo`, y compara — el catálogo no se entera solo.
</details>

**5. Confirma con tus propios ojos que Athena está mockeado (y por qué Glue solo no alcanza)**

Con Glue ya catalogando el gold layer real, intenta `aws athena start-query-execution --query-string "SELECT COUNT(*) FROM dim_customer" --query-execution-context Database=bank_gold --result-configuration OutputLocation=s3://bank-gold/athena-results/`, espera con `get-query-execution`, y luego `get-query-results`.

<details><summary>Verificar</summary>

El estado llega a `SUCCEEDED` — pero `get-query-results` devuelve `{"result": "mock_value"}`, no un conteo real de `dim_customer`. Es el hallazgo que sacó a Athena del alcance de este repo (ver tabla *Emulated vs. real* del README): MiniStack expone la API de Athena y la deja "tener éxito", sin ejecutar la query. `make query` (DuckDB, sección 1) sigue siendo la única capa de consulta real aquí — Glue es catálogo puro, no motor de queries.
</details>

---

## 6. Quality report

```bash
make e2e
cat docs/quality-report.md
```

---

## 7. Cerrar

```bash
docker compose down
```
