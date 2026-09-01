#!/usr/bin/env python3
"""Uploads locally-generated bronze month files to s3://bank-bronze/ —
data_gen.py writes local JSONL for fast iteration; this is the step that
actually lands it where the rest of the pipeline (silver.py, gold.py)
reads it from, per the architecture diagram."""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils import aws  # noqa: E402

BUCKET = "bank-bronze"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--in", dest="in_dir", default="data")
    args = parser.parse_args()

    s3 = aws.client("s3")
    month_files = sorted(Path(args.in_dir).glob("month=*.jsonl"))
    if not month_files:
        raise SystemExit(
            f"no month=*.jsonl files found in {args.in_dir} — run src/data_gen.py first"
        )

    for f in month_files:
        s3.upload_file(str(f), BUCKET, f.name)
        print(f"  uploaded {f.name} -> s3://{BUCKET}/{f.name}")

    print(f"uploaded {len(month_files)} bronze month files")


if __name__ == "__main__":
    main()
