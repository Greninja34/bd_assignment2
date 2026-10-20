#!/usr/bin/env python3
"""Independent cross-check of the Spark outputs (plain Python, no Spark/Kafka).

Re-derives everything straight from the raw dataset with the stdlib and
compares it with what the pipeline produced. If this passes, the pipeline
(Kafka hops + Spark jobs) did not lose, add or distort a single record.

    python3 verify_reference.py --raw order.jsonl \
        --metrics order_metrics.jsonl \
        --restaurants restaurant_summary.csv --cities city_summary.csv

Exit code 0 = all checks passed.
"""
import argparse
import csv
import json
import math
import sys
from collections import OrderedDict
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal

TS_FMT = "%Y-%m-%d %H:%M:%S"
ID_COLS = ["order_id", "customer_id", "restaurant_id", "delivery_partner_id", "city"]
TS_COLS = ["order_timestamp", "accepted_timestamp", "picked_up_timestamp",
           "expected_delivery_timestamp", "delivered_timestamp"]
NUM_COLS = ["distance_km", "order_amount", "delivery_fee"]
REQUIRED = ID_COLS + TS_COLS + NUM_COLS + ["status"]


def r2(x):
    """Round half-up to 2 dp (what Spark's round() does)."""
    if x is None:
        return None
    return float(Decimal(repr(float(x))).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def scalar_to_str(v):
    # mimic reading a JSON scalar as a string
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, float) and v == int(v) and abs(v) < 1e15:
        return repr(v)
    return str(v)


def reference_metrics(raw_path):
    seen, out, reasons = set(), OrderedDict(), {}
    n_in = 0
    with open(raw_path, encoding="utf-8") as fh:
        for line in fh:
            line = line.rstrip("\r\n")
            if not line.strip():
                continue
            n_in += 1

            def reject(why):
                reasons[why] = reasons.get(why, 0) + 1

            try:
                d = json.loads(line)
                if not isinstance(d, dict):
                    raise ValueError
            except Exception:
                reject("malformed_json"); continue
            if any(d.get(c) is None or str(d.get(c)).strip() == "" for c in REQUIRED):
                reject("missing_required_field"); continue
            try:
                num = {c: float(d[c]) for c in NUM_COLS}
                if any(math.isnan(v) or math.isinf(v) for v in num.values()):
                    raise ValueError
            except Exception:
                reject("invalid_number"); continue
            try:
                ts = {c: datetime.strptime(str(d[c]), TS_FMT) for c in TS_COLS}
            except Exception:
                reject("invalid_timestamp"); continue
            if num["distance_km"] <= 0:
                reject("distance_km<=0"); continue
            if num["order_amount"] <= 0:
                reject("order_amount<=0"); continue
            if str(d["status"]).strip() != "DELIVERED":
                reject("status_not_delivered"); continue

            key = tuple(scalar_to_str(d[c]) for c in REQUIRED)
            if key in seen:
                reject("exact_duplicate"); continue
            seen.add(key)

            m = lambda a, b: (ts[a] - ts[b]).total_seconds() / 60.0
            prep = m("picked_up_timestamp", "accepted_timestamp")
            deliv = m("delivered_timestamp", "picked_up_timestamp")
            fulfil = m("delivered_timestamp", "order_timestamp")
            speed = num["distance_km"] / (deliv / 60.0) if deliv != 0 else None
            delay = max(0.0, m("delivered_timestamp", "expected_delivery_timestamp"))
            status = "ON_TIME" if delay == 0 else "DELAYED"
            cat = ("ON_TIME" if delay == 0 else "SLIGHT_DELAY" if delay <= 15
                   else "MODERATE_DELAY" if delay <= 30 else "SEVERE_DELAY")
            out[d["order_id"], key] = {
                "order_id": d["order_id"], "customer_id": d["customer_id"],
                "restaurant_id": d["restaurant_id"],
                "delivery_partner_id": d["delivery_partner_id"], "city": d["city"],
                "order_amount": r2(num["order_amount"]), "delivery_fee": r2(num["delivery_fee"]),
                "distance_km": r2(num["distance_km"]),
                "preparation_time_min": r2(prep), "delivery_time_min": r2(deliv),
                "fulfillment_time_min": r2(fulfil), "delivery_speed_kmph": r2(speed),
                "delay_min": r2(delay), "delivery_status": status, "delay_category": cat,
            }
    return n_in, reasons, list(out.values())


def aggregate(rows, key, fields):
    groups = OrderedDict()
    for r in rows:
        groups.setdefault(r[key], []).append(r)
    res = {}
    for k, g in groups.items():
        n = len(g)
        delayed = sum(1 for r in g if r["delivery_status"] == "DELAYED")
        row = {"order_count": n}
        for out_name, src in fields.items():
            row[out_name] = r2(sum(r[src] for r in g) / n)
        row["delayed_order_count"] = delayed
        row["delay_rate_pct"] = r2(100.0 * delayed / n)
        res[k] = row
    return res


def close(a, b, tol=0.011):
    if a is None or b is None:
        return a is None and b is None
    return abs(float(a) - float(b)) <= tol


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", required=True, help="the team's original order.jsonl")
    ap.add_argument("--metrics", required=True, help="order_metrics.jsonl (Spark Job 1 output)")
    ap.add_argument("--restaurants", required=True)
    ap.add_argument("--cities", required=True)
    args = ap.parse_args()

    n_in, reasons, ref = reference_metrics(args.raw)
    print(f"raw records            : {n_in}")
    for k, v in sorted(reasons.items()):
        print(f"  rejected {k:<26}: {v}")
    print(f"expected output orders : {len(ref)}")

    fails = []

    # ---- order_metrics.jsonl
    with open(args.metrics, encoding="utf-8") as fh:
        got = [json.loads(l) for l in fh if l.strip()]
    if len(got) != len(ref):
        fails.append(f"order_metrics.jsonl has {len(got)} rows, expected {len(ref)}")
    got_by_id = {g["order_id"]: g for g in got}
    if len(got_by_id) != len(got):
        fails.append("order_metrics.jsonl contains duplicate order_id values")
    for r in ref:
        g = got_by_id.get(r["order_id"])
        if g is None:
            fails.append(f"missing order {r['order_id']}"); continue
        for col, exp in r.items():
            act = g.get(col)
            ok = close(act, exp) if isinstance(exp, float) or exp is None else act == exp
            if not ok:
                fails.append(f"{r['order_id']}.{col}: got {act!r}, expected {exp!r}")

    # ---- summaries (computed from the reference metrics, like Spark Job 2)
    rest_ref = aggregate([r for r in ref], "restaurant_id",
                         {"avg_order_amount": "order_amount",
                          "avg_preparation_time_min": "preparation_time_min",
                          "avg_delivery_time_min": "delivery_time_min"})
    city_ref = aggregate([r for r in ref], "city",
                         {"avg_order_amount": "order_amount",
                          "avg_delivery_time_min": "delivery_time_min"})

    def check_csv(path, key, expected):
        with open(path, newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        if {r[key] for r in rows} != set(expected):
            fails.append(f"{path}: {key} set differs: got {sorted(r[key] for r in rows)} "
                         f"expected {sorted(expected)}")
        for r in rows:
            exp = expected.get(r[key])
            if exp is None:
                continue
            for col, ev in exp.items():
                if not close(r[col], ev):
                    fails.append(f"{path} {r[key]}.{col}: got {r[col]}, expected {ev}")
        print(f"{path}: {len(rows)} rows checked")

    check_csv(args.restaurants, "restaurant_id", rest_ref)
    check_csv(args.cities, "city", city_ref)

    if fails:
        print(f"\nFAILED - {len(fails)} mismatch(es):")
        for f in fails[:40]:
            print("  -", f)
        return 1
    print("\nALL CHECKS PASSED - pipeline output matches the independent re-computation.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
