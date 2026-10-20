#!/usr/bin/env python3
"""Publish every line of a local file to a Kafka topic, one message per line.

Used for BOTH producers of the pipeline:
    Producer 1 (VM1):  order.jsonl         -> topic `orders`
    Producer 2 (VM4):  order_metrics.jsonl -> topic `order_metrics`

The line is sent byte-for-byte. We do NOT parse/clean here: malformed and
dirty records must travel through Kafka untouched and be dropped by Spark
Job 1 (that is where the cleaning rules live).
"""
import argparse
import collections
import json
import sys

from kafka import KafkaProducer


def best_effort_key(raw: bytes):
    """Use order_id as the partition key when the line happens to be valid
    JSON with one; otherwise no key (round-robin). Never raises."""
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict) and obj.get("order_id") not in (None, ""):
            return str(obj["order_id"]).encode("utf-8")
    except Exception:
        pass
    return None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--input", required=True, help="local .jsonl file to publish")
    ap.add_argument("--topic", required=True)
    ap.add_argument("--bootstrap", required=True,
                    help="comma separated host:port list, e.g. 10.0.0.2:9092,10.0.0.3:9092")
    args = ap.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap.split(","),
        acks="all",            # wait for all in-sync replicas (survives a broker loss)
        retries=10,
        linger_ms=20,
    )

    per_partition = collections.Counter()
    errors = []

    def on_ok(md):
        per_partition[md.partition] += 1

    def on_err(exc):
        errors.append(exc)

    sent = skipped_blank = 0
    with open(args.input, "rb") as fh:
        for raw in fh:
            raw = raw.rstrip(b"\r\n")
            if not raw.strip():          # blank line is not a record
                skipped_blank += 1
                continue
            producer.send(args.topic, value=raw, key=best_effort_key(raw)) \
                    .add_callback(on_ok).add_errback(on_err)
            sent += 1

    producer.flush()
    producer.close()

    if errors:
        print(f"ERROR: {len(errors)} message(s) failed, first: {errors[0]!r}", file=sys.stderr)
        return 1

    print(f"published {sent} records to topic '{args.topic}' "
          f"(skipped {skipped_blank} blank lines)")
    for p in sorted(per_partition):
        print(f"  partition {p}: {per_partition[p]} records")
    return 0


if __name__ == "__main__":
    sys.exit(main())
