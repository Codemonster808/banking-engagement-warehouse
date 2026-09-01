#!/usr/bin/env python3
"""Produces benchmarks/results.json from a real pipeline run against MiniStack."""

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from utils import warehouse  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="benchmarks/results.json")
    args = parser.parse_args()

    con = warehouse.connect()
    start = time.perf_counter()
    warehouse.read_parquet(con, "s3://bank-gold/dim_customer/**/*.parquet", "dim_customer")
    row_count = con.execute("SELECT COUNT(*) FROM dim_customer").fetchone()[0]
    query_seconds = time.perf_counter() - start

    overlap_check_start = time.perf_counter()
    overlaps = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT a.customer_id
            FROM dim_customer a JOIN dim_customer b
              ON a.customer_id = b.customer_id
             AND a.valid_from < COALESCE(b.valid_to, TIMESTAMP '9999-12-31')
             AND COALESCE(a.valid_to, TIMESTAMP '9999-12-31') > b.valid_from
             AND a.valid_from != b.valid_from
        )
    """).fetchone()[0]
    overlap_check_seconds = time.perf_counter() - overlap_check_start

    results = {
        "dim_customer_rows": row_count,
        "gold_read_seconds": round(query_seconds, 3),
        "overlap_check_seconds": round(overlap_check_seconds, 3),
        "overlapping_validity_ranges_found": overlaps,
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(results, indent=2))
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
