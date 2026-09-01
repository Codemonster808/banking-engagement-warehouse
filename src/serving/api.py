#!/usr/bin/env python3
"""Flask serving layer: cohort retention, SLA status, cost by pipeline."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from flask import Flask, jsonify  # noqa: E402

from orchestration.cost_sla import cost_by_pipeline  # noqa: E402
from utils import aws, warehouse  # noqa: E402

app = Flask(__name__)


def _dim_customer_con():
    con = warehouse.connect()
    try:
        warehouse.read_parquet(con, "s3://bank-gold/dim_customer/**/*.parquet", "dim_customer")
    except Exception:
        con.execute("CREATE OR REPLACE VIEW dim_customer AS SELECT NULL AS segment WHERE FALSE")
    return con


@app.get("/health")
def health():
    return jsonify({"status": "ok"})


@app.get("/cohort")
def cohort():
    con = _dim_customer_con()
    rows = con.execute("""
        SELECT segment,
               COUNT(*) FILTER (WHERE is_current) AS customers_currently_in_segment,
               COUNT(DISTINCT customer_id) AS customers_ever_in_segment
        FROM dim_customer
        GROUP BY segment
        ORDER BY customers_currently_in_segment DESC
    """).fetchall()
    return jsonify([{"segment": r[0], "current": r[1], "ever": r[2]} for r in rows])


@app.get("/sla/status")
def sla_status():
    ddb = aws.client("dynamodb")
    paginator = ddb.get_paginator("scan")
    breaches = 0
    for page in paginator.paginate(TableName="sla-alert-dedup"):
        breaches += page.get("Count", 0)
    return jsonify({"sla_breaches_recorded": breaches})


@app.get("/cost/by-pipeline")
def cost_by_pipeline_endpoint():
    return jsonify(cost_by_pipeline())


if __name__ == "__main__":
    app.run(port=5000)
