#!/usr/bin/env python3
"""Drain a Kafka topic (from the beginning) into a local .jsonl file.

Used for BOTH consumers of the pipeline:
    Consumer 1 (VM4):  topic `orders`        -> orders_landed.jsonl
    Consumer 2 (VM1):  topic `order_metrics` -> order_metrics_landed.jsonl

Design choices (all deliberate):
  * Partitions are assigned manually and read from offset 0 up to the
    end offsets seen at start-up. No consumer group => no committed
    offsets => re-running always re-lands the *whole* topic, so the
    script is safe to run again.
  * The output is written to <out>.tmp and renamed at the end, so Spark
    can never pick up a half-written file.
  * Records are written byte-for-byte, one per line; nothing is parsed.
  * Spark never reads Kafka directly (assignment rule) - it reads the file.
"""
import argparse
import collections
import os
import sys
import time

from kafka import KafkaConsumer, TopicPartition


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--topic", required=True)
    ap.add_argument("--out", required=True, help="landing file, e.g. orders_landed.jsonl")
    ap.add_argument("--bootstrap", required=True)
    ap.add_argument("--idle-timeout", type=float, default=30.0,
                    help="give up if no data arrives for this many seconds (default 30)")
    args = ap.parse_args()

    consumer = KafkaConsumer(
        bootstrap_servers=args.bootstrap.split(","),
        group_id=None,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )

    # wait for topic metadata (the topic may have been created a moment ago)
    parts = None
    for _ in range(30):
        parts = consumer.partitions_for_topic(args.topic)
        if parts:
            break
        time.sleep(1)
    if not parts:
        print(f"ERROR: topic '{args.topic}' not found on {args.bootstrap}", file=sys.stderr)
        return 1

    tps = [TopicPartition(args.topic, p) for p in sorted(parts)]
    consumer.assign(tps)
    consumer.seek_to_beginning(*tps)
    start = consumer.beginning_offsets(tps)
    end = consumer.end_offsets(tps)
    expected = sum(end[tp] - start[tp] for tp in tps)
    print(f"topic '{args.topic}': {len(tps)} partitions, {expected} records to read")

    got = collections.Counter()
    done = {tp: end[tp] <= start[tp] for tp in tps}
    tmp = args.out + ".tmp"
    last_data = time.time()

    with open(tmp, "wb") as fh:
        while not all(done.values()):
            batch = consumer.poll(timeout_ms=1000)
            if batch:
                last_data = time.time()
            for tp, records in batch.items():
                for rec in records:
                    if rec.offset >= end[tp]:      # appended after we started
                        done[tp] = True
                        continue
                    fh.write(rec.value.rstrip(b"\r\n") + b"\n")
                    got[tp.partition] += 1
                    if rec.offset + 1 >= end[tp]:
                        done[tp] = True
            if time.time() - last_data > args.idle_timeout:
                print(f"WARNING: idle for {args.idle_timeout}s before reaching the end "
                      f"offsets; stopping with what we have", file=sys.stderr)
                break

    consumer.close()
    os.replace(tmp, args.out)

    total = sum(got.values())
    print(f"landed {total}/{expected} records -> {args.out}")
    for p in sorted(got):
        print(f"  partition {p}: {got[p]} records")
    return 0 if total == expected else 2


if __name__ == "__main__":
    sys.exit(main())
