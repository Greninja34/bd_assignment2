#!/usr/bin/env python3
"""Spark Job 2 - Operations Analytics  (runs on VM1)

    order_metrics_landed.jsonl --Spark--> restaurant_summary.csv
                                          city_summary.csv

Exactly two analytics (assignment section 6):
  A. per restaurant_id : order_count, avg_order_amount, avg_preparation_time_min,
                         avg_delivery_time_min, delayed_order_count, delay_rate_pct
  B. per city          : order_count, avg_order_amount, avg_delivery_time_min,
                         delayed_order_count, delay_rate_pct

    delay_rate_pct = 100 * delayed_order_count / order_count
All numeric results are rounded to 2 dp. Exact duplicate records (e.g. the
topic was published twice) are counted only once.

Run:
    spark-submit --master local[*] spark_job2.py \
        --input order_metrics_landed.jsonl --outdir .
"""
import argparse
import glob
import os
import shutil

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

CORRUPT = "_corrupt_record"
SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("restaurant_id", StringType()),
    StructField("delivery_partner_id", StringType()),
    StructField("city", StringType()),
    StructField("order_amount", DoubleType()),
    StructField("delivery_fee", DoubleType()),
    StructField("distance_km", DoubleType()),
    StructField("preparation_time_min", DoubleType()),
    StructField("delivery_time_min", DoubleType()),
    StructField("fulfillment_time_min", DoubleType()),
    StructField("delivery_speed_kmph", DoubleType()),
    StructField("delay_min", DoubleType()),
    StructField("delivery_status", StringType()),
    StructField("delay_category", StringType()),
    StructField(CORRUPT, StringType()),
])
DATA_COLS = [f.name for f in SCHEMA.fields if f.name != CORRUPT]


def write_single_file(df, final_path):
    tmp_dir = final_path + "._spark_tmp"
    shutil.rmtree(tmp_dir, ignore_errors=True)
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(tmp_dir)
    parts = glob.glob(os.path.join(tmp_dir, "part-*"))
    if len(parts) != 1:
        raise RuntimeError(f"expected exactly one part file in {tmp_dir}, found {parts}")
    if os.path.exists(final_path):
        os.remove(final_path)
    shutil.move(parts[0], final_path)
    shutil.rmtree(tmp_dir, ignore_errors=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="order_metrics_landed.jsonl")
    ap.add_argument("--outdir", default=".")
    args = ap.parse_args()

    spark = (SparkSession.builder
             .appName("A2-SparkJob2-OperationsAnalytics")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.read.schema(SCHEMA)
           .option("mode", "PERMISSIVE")
           .option("columnNameOfCorruptRecord", CORRUPT)
           .json(args.input)
           .cache())
    n_read = raw.count()

    # Job 1 already cleaned the data, so we only defend against corrupt lines
    # and then collapse exact duplicates.
    orders = (raw.where(F.col(CORRUPT).isNull() & F.col("order_id").isNotNull())
                 .select(*DATA_COLS)
                 .dropDuplicates())
    n_orders = orders.count()
    print("\n=== Spark Job 2 ===")
    print(f"records read          : {n_read}")
    print(f"unique orders analysed: {n_orders}")

    delayed = F.sum(F.when(F.col("delivery_status") == "DELAYED", 1).otherwise(0))

    # ---- A. restaurant performance
    restaurant = (orders.groupBy("restaurant_id")
        .agg(F.count("*").alias("order_count"),
             F.round(F.avg("order_amount"), 2).alias("avg_order_amount"),
             F.round(F.avg("preparation_time_min"), 2).alias("avg_preparation_time_min"),
             F.round(F.avg("delivery_time_min"), 2).alias("avg_delivery_time_min"),
             delayed.cast("long").alias("delayed_order_count"))
        .withColumn("delay_rate_pct",
                    F.round(100.0 * F.col("delayed_order_count") / F.col("order_count"), 2))
        .orderBy("restaurant_id"))

    # ---- B. city delivery performance
    city = (orders.groupBy("city")
        .agg(F.count("*").alias("order_count"),
             F.round(F.avg("order_amount"), 2).alias("avg_order_amount"),
             F.round(F.avg("delivery_time_min"), 2).alias("avg_delivery_time_min"),
             delayed.cast("long").alias("delayed_order_count"))
        .withColumn("delay_rate_pct",
                    F.round(100.0 * F.col("delayed_order_count") / F.col("order_count"), 2))
        .orderBy("city"))

    r_path = os.path.join(args.outdir, "restaurant_summary.csv")
    c_path = os.path.join(args.outdir, "city_summary.csv")
    write_single_file(restaurant, r_path)
    write_single_file(city, c_path)

    print(f"\nrestaurant_summary.csv ({restaurant.count()} restaurants):")
    restaurant.show(50, truncate=False)
    print(f"city_summary.csv ({city.count()} cities):")
    city.show(50, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
