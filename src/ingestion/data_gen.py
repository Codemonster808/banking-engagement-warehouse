#!/usr/bin/env python3
"""
Generate 24 months of synthetic banking engagement events (login, card_txn,
offer_shown, offer_redeemed). Each customer has a `segment` that changes for
~5% of customers each month — this is exactly what SCD Type 2 needs to
capture correctly under backfill.
"""

import argparse
import json
import sys
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils.synth import seeded_rng  # noqa: E402

SEGMENTS = ["mass", "affluent", "private", "student", "dormant"]
EVENT_TYPES = ["login", "card_txn", "offer_shown", "offer_redeemed"]


def gen_customer_segment_history(
    rng, customer_id: str, n_months: int, start: datetime
) -> list[dict]:
    """Returns a list of {segment, valid_from} — the ground truth SCD2 should reconstruct."""
    history = [{"segment": rng.choice(SEGMENTS), "valid_from": start}]
    for month in range(1, n_months):
        month_start = start + timedelta(days=30 * month)
        if rng.random() < 0.05:  # ~5% churn/month
            new_segment = rng.choice([s for s in SEGMENTS if s != history[-1]["segment"]])
            history.append({"segment": new_segment, "valid_from": month_start})
    return history


def segment_at(history: list[dict], ts: datetime) -> str:
    current = history[0]["segment"]
    for entry in history:
        if entry["valid_from"] <= ts:
            current = entry["segment"]
        else:
            break
    return current


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--months", type=int, default=24)
    parser.add_argument("--customers", type=int, default=5000)
    parser.add_argument("--out", required=True)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    rng = seeded_rng(args.seed)
    start = datetime(2024, 1, 1, tzinfo=UTC)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    customer_ids = [f"cust_{i:06d}" for i in range(args.customers)]
    histories = {
        cid: gen_customer_segment_history(rng, cid, args.months, start) for cid in customer_ids
    }

    # Save ground truth separately — this is what test_scd2_backfill.py
    # and the "measured" precision numbers get compared against.
    ground_truth = {
        cid: [{"segment": h["segment"], "valid_from": h["valid_from"].isoformat()} for h in hist]
        for cid, hist in histories.items()
    }
    (out_dir / "_ground_truth_segments.json").write_text(json.dumps(ground_truth))

    n_events_total = 0
    for month in range(args.months):
        month_start = start + timedelta(days=30 * month)
        month_file = out_dir / f"month={month:02d}.jsonl"
        with month_file.open("w") as f:
            n_events_this_month = args.customers * 4  # ~4 events/customer/month on average
            for _ in range(n_events_this_month):
                cid = rng.choice(customer_ids)
                event_ts = month_start + timedelta(
                    days=rng.randint(0, 29), seconds=rng.randint(0, 86399)
                )
                event = {
                    "event_id": str(uuid.uuid4()),
                    "customer_id": cid,
                    "event_type": rng.choice(EVENT_TYPES),
                    "segment_at_event": segment_at(histories[cid], event_ts),
                    "amount_cents": rng.randint(100, 100_000) if rng.random() < 0.4 else None,
                    "ts": event_ts.isoformat(),
                }
                f.write(json.dumps(event) + "\n")
                n_events_total += 1

    print(
        f"wrote {n_events_total} events across {args.months} months "
        f"for {args.customers} customers to {out_dir}"
    )
    n_churned = sum(1 for h in histories.values() if len(h) > 1)
    print(
        f"  {n_churned} customers ({n_churned / args.customers:.1%}) changed segment at least once"
    )


if __name__ == "__main__":
    main()
