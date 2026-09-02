#!/usr/bin/env python3
"""Registers the gold layer in the Glue Data Catalog.

    source env.sh
    python3 src/catalog.py

Today `dim_customer`, `fact_engagement_daily`, and `dim_offer` are read
by path (`s3a://bank-gold/<table>/`) — nobody but the Spark code that
wrote them knows their schema. This registers that schema in Glue, the
same catalog a real Athena/Redshift Spectrum/EMR job would query
against instead of hardcoding a path and a column list.

Column types here are taken directly from the Spark builders
(src/gold.py:build_dim_customer, src/facts.py:build_fact_engagement_daily
and build_dim_offer) — not guessed. If those builders change a column,
this file needs to change with them; there's no schema inference here
on purpose, so a drift is a diff you'll actually see in review.

Verified in this session: MiniStack's Glue is real (create-database /
create-table / get-tables round-trip the schema you give them
correctly). Athena is NOT — `start-query-execution` against this data
returns a mocked `{"result": "mock_value"}` instead of running the
query, so this repo does not use Athena. Glue here is catalog-only
metadata; DuckDB (see `make query`) remains the query layer.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import TypedDict

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from utils import aws  # noqa: E402

DATABASE_NAME = "bank_gold"


class TableSpec(TypedDict):
    """One gold table as Glue needs it: where it lives and its column list."""

    location: str
    columns: list[dict[str, str]]


# Glue column types use the Hive/Presto type names, not Spark's.
TABLES: dict[str, TableSpec] = {
    "dim_customer": {
        "location": "s3://bank-gold/dim_customer/",
        "columns": [
            {"Name": "customer_id", "Type": "string"},
            {"Name": "segment", "Type": "string"},
            {"Name": "valid_from", "Type": "timestamp"},
            {"Name": "valid_to", "Type": "timestamp"},
            {"Name": "is_current", "Type": "boolean"},
        ],
    },
    "fact_engagement_daily": {
        "location": "s3://bank-gold/fact_engagement_daily/",
        "columns": [
            {"Name": "customer_id", "Type": "string"},
            {"Name": "event_date", "Type": "date"},
            {"Name": "event_type", "Type": "string"},
            {"Name": "n_events", "Type": "bigint"},
            {"Name": "total_amount_cents", "Type": "bigint"},
        ],
    },
    "dim_offer": {
        "location": "s3://bank-gold/dim_offer/",
        "columns": [
            {"Name": "event_type", "Type": "string"},
            {"Name": "n_events", "Type": "bigint"},
            {"Name": "n_distinct_customers", "Type": "bigint"},
            {"Name": "offer_id", "Type": "string"},
        ],
    },
}


def ensure_database(glue) -> None:
    existing = {db["Name"] for db in glue.get_databases().get("DatabaseList", [])}
    if DATABASE_NAME in existing:
        print(f"  database {DATABASE_NAME}: already exists")
        return
    glue.create_database(DatabaseInput={"Name": DATABASE_NAME})
    print(f"  database {DATABASE_NAME}: created")


def ensure_table(glue, table_name: str, location: str, columns: list[dict]) -> None:
    table_input = {
        "Name": table_name,
        "StorageDescriptor": {
            "Columns": columns,
            "Location": location,
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": (
                    "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                )
            },
        },
        "TableType": "EXTERNAL_TABLE",
    }
    existing = {t["Name"] for t in glue.get_tables(DatabaseName=DATABASE_NAME).get("TableList", [])}
    if table_name in existing:
        glue.update_table(DatabaseName=DATABASE_NAME, TableInput=table_input)
        print(f"  table {table_name}: updated ({len(columns)} columns)")
    else:
        glue.create_table(DatabaseName=DATABASE_NAME, TableInput=table_input)
        print(f"  table {table_name}: created ({len(columns)} columns)")


def main() -> None:
    glue = aws.client("glue")
    print(f"Glue Data Catalog — database {DATABASE_NAME}:")
    ensure_database(glue)
    for table_name, spec in TABLES.items():
        ensure_table(glue, table_name, spec["location"], spec["columns"])
    print()
    print("Verify: aws glue get-tables --database-name bank_gold")


if __name__ == "__main__":
    main()
