#!/usr/bin/env python3
"""Spark Job 1 - Order Enrichment  (runs on VM4)

    orders_landed.jsonl  --Spark-->  order_metrics.jsonl

Cleaning rules (assignment section 5) - a record is DROPPED when:
    1. the JSON record is malformed
    2. a required field is missing / empty / not a valid number
    3. any required timestamp is invalid
    4. distance_km  <= 0
    5. order_amount <= 0
    6. status != DELIVERED
  and exact duplicate records are counted only once.

For every surviving order we add (all rounded to 2 dp):
    preparation_time_min   = picked_up  - accepted
    delivery_time_min      = delivered  - picked_up
    fulfillment_time_min   = delivered  - order
    delivery_speed_kmph    = distance_km / (delivery_time_min / 60)
    delay_min              = max(0, delivered - expected_delivery)
    delivery_status        = ON_TIME if delay_min = 0 else DELAYED
    delay_category         = ON_TIME / SLIGHT_DELAY (<=15) /
                             MODERATE_DELAY (<=30) / SEVERE_DELAY (>30)

Run:
    spark-submit --master local[*] spark_job1.py \
        --input orders_landed.jsonl --output order_metrics.jsonl
"""
import argparse
import glob
import json
import os
import shutil

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType, StructField, StructType

ID_COLS = ["order_id", "customer_id", "restaurant_id", "delivery_partner_id", "city"]
TS_COLS = ["order_timestamp", "accepted_timestamp", "picked_up_timestamp",
           "expected_delivery_timestamp", "delivered_timestamp"]
NUM_COLS = ["distance_km", "order_amount", "delivery_fee"]
REQUIRED = ID_COLS + TS_COLS + NUM_COLS + ["status"]          # all 14 input fields

# Parse every field as a *string*: a wrong type must make the record
# "invalid" (our rule), not crash the parser or silently become null.
RAW_SCHEMA = StructType([StructField(c, StringType(), True) for c in REQUIRED])


def _is_json_object(line):
    """Strict well-formedness test. Spark's own reader is lenient (it accepts
    `{...}}` and truncated tails in some cases), so we do not rely on it."""
    try:
        return isinstance(json.loads(line), dict)
    except Exception:
        return False

OUT_COLS = ["order_id", "customer_id", "restaurant_id", "delivery_partner_id", "city",
            "order_amount", "delivery_fee", "distance_km",
            "preparation_time_min", "delivery_time_min", "fulfillment_time_min",
            "delivery_speed_kmph", "delay_min", "delivery_status", "delay_category"]


def write_single_file(df, fmt, final_path, **options):
    """Spark writes a *directory* of part files; the assignment wants one
    plain file. Write one partition to a temp dir, move the part file out."""
    tmp_dir = final_path + "._spark_tmp"
    shutil.rmtree(tmp_dir, ignore_errors=True)
    writer = df.coalesce(1).write.mode("overwrite")
    for k, v in options.items():
        writer = writer.option(k, v)
    getattr(writer, fmt)(tmp_dir)
    parts = glob.glob(os.path.join(tmp_dir, "part-*"))
    if len(parts) != 1:
        raise RuntimeError(f"expected exactly one part file in {tmp_dir}, found {parts}")
    if os.path.exists(final_path):
        os.remove(final_path)
    shutil.move(parts[0], final_path)
    shutil.rmtree(tmp_dir, ignore_errors=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="orders_landed.jsonl")
    ap.add_argument("--output", default="order_metrics.jsonl")
    ap.add_argument("--ts-format", default="yyyy-MM-dd HH:mm:ss",
                    help="timestamp pattern of the dataset (Spark datetime pattern)")
    args = ap.parse_args()

    spark = (SparkSession.builder
             .appName("A2-SparkJob1-OrderEnrichment")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")  # bad ts -> null, no exception
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # ---- 1. read: one JSON record per line (blank lines are not records)
    is_json_object = F.udf(_is_json_object, "boolean")
    lines = spark.read.text(args.input).where(F.trim(F.col("value")) != "")
    raw = (lines
           .select(is_json_object("value").alias("_json_ok"),
                   F.from_json("value", RAW_SCHEMA).alias("r"))
           .select("_json_ok", "r.*")
           .cache())
    total_in = raw.count()

    # ---- 2. typed view + validity flags
    df = raw
    for c in TS_COLS:
        df = df.withColumn(c + "_ts", F.to_timestamp(F.col(c), args.ts_format))
    for c in NUM_COLS:
        df = df.withColumn(c + "_num", F.col(c).cast("double"))

    def is_blank(c):
        return F.col(c).isNull() | (F.trim(F.col(c)) == "")

    def bad_num(c):                      # null, NaN or +/-Infinity
        x = F.col(c + "_num")
        return x.isNull() | F.isnan(x) | (F.abs(x) == F.lit(float("inf")))

    missing = F.lit(False)
    for c in REQUIRED:
        missing = missing | is_blank(c)
    bad_numeric = F.lit(False)
    for c in NUM_COLS:
        bad_numeric = bad_numeric | bad_num(c)
    bad_ts = F.lit(False)
    for c in TS_COLS:
        bad_ts = bad_ts | F.col(c + "_ts").isNull()

    reason = (F.when(~F.col("_json_ok"), "malformed_json")
              .when(missing, "missing_required_field")
              .when(bad_numeric, "invalid_number")
              .when(bad_ts, "invalid_timestamp")
              .when(F.col("distance_km_num") <= 0, "distance_km<=0")
              .when(F.col("order_amount_num") <= 0, "order_amount<=0")
              .when(F.trim(F.col("status")) != "DELIVERED", "status_not_delivered"))
    df = df.withColumn("_reject_reason", reason)

    print("\n=== Spark Job 1: cleaning summary ===")
    print(f"input records read            : {total_in}")
    (df.where(F.col("_reject_reason").isNotNull())
       .groupBy("_reject_reason").count().orderBy("_reject_reason").show(truncate=False))

    # ---- 3. valid rows, then drop exact duplicates
    valid = df.where(F.col("_reject_reason").isNull())
    n_valid = valid.count()
    deduped = valid.dropDuplicates(REQUIRED)       # exact duplicate = all 14 fields equal
    n_unique = deduped.count()
    print(f"valid records                 : {n_valid}")
    print(f"exact duplicates removed      : {n_valid - n_unique}")
    print(f"unique valid orders (output)  : {n_unique}")

    # ---- 4. enrichment (minutes = seconds / 60; round only at the very end)
    def minutes(later, earlier):
        return (F.col(later + "_ts").cast("double") - F.col(earlier + "_ts").cast("double")) / 60.0

    d = (deduped
         .withColumn("_prep", minutes("picked_up_timestamp", "accepted_timestamp"))
         .withColumn("_deliv", minutes("delivered_timestamp", "picked_up_timestamp"))
         .withColumn("_fulfil", minutes("delivered_timestamp", "order_timestamp"))
         .withColumn("_speed", F.when(F.col("_deliv") != 0,
                                      F.col("distance_km_num") / (F.col("_deliv") / 60.0)))
         .withColumn("_delay", F.greatest(F.lit(0.0),
                                          minutes("delivered_timestamp", "expected_delivery_timestamp"))))

    delivery_status = F.when(F.col("_delay") == 0, "ON_TIME").otherwise("DELAYED")
    delay_category = (F.when(F.col("_delay") == 0, "ON_TIME")
                      .when(F.col("_delay") <= 15, "SLIGHT_DELAY")
                      .when(F.col("_delay") <= 30, "MODERATE_DELAY")
                      .otherwise("SEVERE_DELAY"))

    out = (d.select(
                F.col("order_id"), F.col("customer_id"), F.col("restaurant_id"),
                F.col("delivery_partner_id"), F.col("city"),
                F.round("order_amount_num", 2).alias("order_amount"),
                F.round("delivery_fee_num", 2).alias("delivery_fee"),
                F.round("distance_km_num", 2).alias("distance_km"),
                F.round("_prep", 2).alias("preparation_time_min"),
                F.round("_deliv", 2).alias("delivery_time_min"),
                F.round("_fulfil", 2).alias("fulfillment_time_min"),
                F.round("_speed", 2).alias("delivery_speed_kmph"),
                F.round("_delay", 2).alias("delay_min"),
                delivery_status.alias("delivery_status"),
                delay_category.alias("delay_category"))
           .select(*OUT_COLS)
           .orderBy("order_id"))

    # ---- 5. write ONE jsonl file (nulls kept so every line has the same keys)
    write_single_file(out, "json", args.output, ignoreNullFields="false")
    print(f"\nwrote {n_unique} records -> {args.output}")
    out.show(5, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
