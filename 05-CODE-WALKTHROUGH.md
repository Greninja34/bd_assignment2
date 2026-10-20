# 05 — Code walkthrough and viva drill

If the TA says *"show me your producer"* or *"why did you write this line?"*, this is your crib sheet.
There are only **five code files** and **two config files** — small enough that every one of you should
be able to explain every one. (`§` → [`04-CONCEPTS-GUIDE.md`](04-CONCEPTS-GUIDE.md).)

| File | Role | Lines |
|---|---|---|
| `common/file_to_kafka.py` | Producer 1 **and** Producer 2 | ~80 |
| `common/kafka_to_file.py` | Consumer 1 **and** Consumer 2 | ~95 |
| `member4_vm4/spark_job1.py` | Spark Job 1 – clean + enrich | ~200 |
| `member1_vm1/spark_job2.py` | Spark Job 2 – restaurant + city analytics | ~130 |
| `common/verify_reference.py` | independent check in plain Python | ~210 |
| `member2_vm2/broker1.properties`, `member3_vm3/broker2.properties` | the two brokers | ~50 each |

> Honest framing for the TA: the two producers are the same program with different arguments, and so
> are the two consumers. That's deliberate (one tested implementation, two uses), and the arguments
> (`--topic`, `--input`/`--out`) are what make it Producer 1 vs Producer 2.

---

## 1. `common/file_to_kafka.py` — the producer

```python
producer = KafkaProducer(
    bootstrap_servers=args.bootstrap.split(","),   # ["ip2:9092", "ip3:9092"]  (§3.5)
    acks="all",            # wait for all in-sync replicas (survives a broker loss)   (§4.4)
    retries=10,            # re-send on transient errors                              (§9.1)
    linger_ms=20,          # wait up to 20 ms to batch records together               (§7)
)
```
```python
with open(args.input, "rb") as fh:                # "rb": read BYTES – we never decode the line
    for raw in fh:
        raw = raw.rstrip(b"\r\n")                 # drop the line ending only
        if not raw.strip():                       # a blank line is not a record
            skipped_blank += 1; continue
        producer.send(args.topic, value=raw, key=best_effort_key(raw)) \
                .add_callback(on_ok).add_errback(on_err)
```
- **`value=raw`** – the exact bytes of the line. No parsing, no cleaning (§9.2).
- **`key=best_effort_key(raw)`** – `order_id` if the line is valid JSON with one, else `None`. A key
  means the same order always goes to the same partition; `None` lets the client spread records. The
  function **never raises**: a malformed line must still be sent.
- **`send()` is asynchronous** → returns a future immediately; `add_callback` counts per-partition
  successes (that's the `partition 0: 33 / partition 1: 29`-style printout), `add_errback` collects failures.
- **`producer.flush()`** blocks until every buffered record is acknowledged — without it the script
  could exit with records still in memory. Then `close()`.
- Exit code 1 if any record failed → a failed publish can't go unnoticed.

**Questions you'll get:** *Why `acks=all`?* (§4.4) · *Why not parse here?* (§9.2) · *What's the
key for?* (§7) · *What does `flush` do?* · *Why are the partitions 33/29 and not 31/31?* (key hashing +
random for the two lines that have no `order_id` — a split isn't meant to be equal, and the exact numbers can vary slightly between runs.)

---

## 2. `common/kafka_to_file.py` — the consumer

```python
consumer = KafkaConsumer(
    bootstrap_servers=args.bootstrap.split(","),
    group_id=None,              # NO consumer group → nothing is committed or remembered   (§8.1)
    enable_auto_commit=False,
    auto_offset_reset="earliest",
)
```
```python
parts = consumer.partitions_for_topic(args.topic)           # which partitions exist? (retried: topic may be brand new)
tps = [TopicPartition(args.topic, p) for p in sorted(parts)]
consumer.assign(tps)                                        # WE choose the partitions  (not subscribe)
consumer.seek_to_beginning(*tps)                            # start at offset 0
end = consumer.end_offsets(tps)                             # where each partition ends RIGHT NOW
expected = sum(end[tp] - start[tp] for tp in tps)           # = records we should read
```
- **`assign` + `seek_to_beginning`** = deterministic: every run reads the whole topic from offset 0.
- **`end_offsets` snapshot** = the finish line. We stop when each partition's position reaches it,
  rather than "when the stream goes quiet", so the result is exact (§8.1).

```python
while not all(done.values()):
    batch = consumer.poll(timeout_ms=1000)                  # fetch what's available → {partition: [records]}
    for tp, records in batch.items():
        for rec in records:
            if rec.offset >= end[tp]:                       # written after our snapshot – ignore
                done[tp] = True; continue
            fh.write(rec.value.rstrip(b"\r\n") + b"\n")     # one record = one line
            got[tp.partition] += 1
            if rec.offset + 1 >= end[tp]:                   # reached the last record of this partition
                done[tp] = True
    if time.time() - last_data > args.idle_timeout: ... break   # safety net: don't hang forever
```
```python
with open(tmp, "wb") as fh: ...     # tmp = out + ".tmp"
os.replace(tmp, args.out)           # atomic rename → Spark never sees a half-written file
...
return 0 if total == expected else 2   # exit code reports "landed 62/62" honestly
```
**Questions:** *Why no consumer group?* (re-runnable; groups would remember offsets and a 2nd run could
read nothing — §8.1) · *What's an end offset?* (§3.2) · *Why write to `.tmp` then rename?* (atomic — a
crash can't leave a partial file) · *Why does the landed file have the lines in a different order?* (two
partitions, no global order — §3.4) · *Why doesn't Spark read Kafka directly?* (assignment rule + the
landing-zone benefits, §13.1)

---

## 3. `member4_vm4/spark_job1.py` — Spark Job 1

Read it top to bottom in six blocks.

### Block 1 — constants
```python
REQUIRED = ID_COLS + TS_COLS + NUM_COLS + ["status"]          # all 14 input fields
RAW_SCHEMA = StructType([StructField(c, StringType(), True) for c in REQUIRED])
```
All fields are declared **string** on purpose (§12.1): the value always survives parsing, and *our* rules
decide whether it's valid.

### Block 2 — session
```python
.config("spark.sql.session.timeZone", "UTC")                # date maths unaffected by DST   (§12.3)
.config("spark.sql.legacy.timeParserPolicy", "CORRECTED")    # unparsable date → null, not an exception
.config("spark.sql.shuffle.partitions", "4")                 # tiny data → 4 not 200 shuffle partitions (§11.4)
```

### Block 3 — read and strict JSON check
```python
is_json_object = F.udf(_is_json_object, "boolean")           # Python json.loads, must be an object  (§12.1)
lines = spark.read.text(args.input).where(F.trim(F.col("value")) != "")
raw = (lines.select(is_json_object("value").alias("_json_ok"),
                    F.from_json("value", RAW_SCHEMA).alias("r"))
            .select("_json_ok", "r.*")
            .cache())                                        # reuse the parsed data for several counts (§11.3)
total_in = raw.count()                                       # first ACTION → the file is actually read here
```
A UDF is slower than built-ins but gives **strict** malformed-JSON detection; Spark's own JSON reader
accepted `{…}}` in our stress test.

### Block 4 — validity flags
```python
missing     = OR over REQUIRED of  (col is null  OR  trim(col) == "")
bad_numeric = OR over NUM_COLS of  (num is null OR NaN OR ±Infinity)       # "abc".cast("double") is null
bad_ts      = OR over TS_COLS  of  (to_timestamp(col, "yyyy-MM-dd HH:mm:ss") is null)
```
The loops build one big boolean expression. Nothing executes yet (lazy, §11.3).

### Block 5 — the reason chain (the heart of the cleaning)
```python
reason = (F.when(~F.col("_json_ok"), "malformed_json")
          .when(missing, "missing_required_field")
          .when(bad_numeric, "invalid_number")
          .when(bad_ts, "invalid_timestamp")
          .when(F.col("distance_km_num") <= 0, "distance_km<=0")
          .when(F.col("order_amount_num") <= 0, "order_amount<=0")
          .when(F.trim(F.col("status")) != "DELIVERED", "status_not_delivered"))
```
First matching branch wins; **valid rows end up with a null reason**. The order is not cosmetic:
`NULL <= 0` is `NULL` → treated as false, so null/invalid numbers must be rejected *before* the `<= 0`
tests (§12.2). The `groupBy("_reject_reason").count()` prints the rejection table.

Then:
```python
valid   = df.where(F.col("_reject_reason").isNull())
deduped = valid.dropDuplicates(REQUIRED)            # exact duplicate = all 14 fields equal   (§9.3)
```

### Block 6 — enrichment, output
```python
def minutes(later, earlier):
    return (F.col(later + "_ts").cast("double") - F.col(earlier + "_ts").cast("double")) / 60.0
```
timestamp → seconds since 1970 → difference → minutes (§12.4).
```python
_speed = F.when(F.col("_deliv") != 0, F.col("distance_km_num") / (F.col("_deliv") / 60.0))   # no ÷0
_delay = F.greatest(F.lit(0.0), minutes("delivered_timestamp", "expected_delivery_timestamp")) # max(0, …)
delivery_status = F.when(F.col("_delay") == 0, "ON_TIME").otherwise("DELAYED")
delay_category  = (F.when(_delay == 0, "ON_TIME").when(_delay <= 15, "SLIGHT_DELAY")
                    .when(_delay <= 30, "MODERATE_DELAY").otherwise("SEVERE_DELAY"))
```
All derived from **unrounded** values; `F.round(..., 2)` is applied only in the final `select` (§12.4–12.5).
`write_single_file(out, "json", args.output, ignoreNullFields="false")` does `coalesce(1)` → temp dir →
move `part-*` → final name (§12.7).

**Questions:** *Why strings for everything?* · *Why a UDF?* · *What does `.cache()` do?* · *Why does the
`when` order matter?* · *Where do duplicates get removed?* · *What's a wide transformation and which
ones are here?* (`dropDuplicates`, `groupBy`, `orderBy`) · *How would you handle delivery_time = 0?*
(speed → null, rather than a divide-by-zero) · *Why `coalesce(1)`, not `repartition(1)`?* (`coalesce`
avoids a full shuffle) · *What are the `.crc` and `_SUCCESS` files?* (Hadoop output-committer artifacts)

---

## 4. `member1_vm1/spark_job2.py` — Spark Job 2

```python
raw = spark.read.schema(SCHEMA)...json(args.input).cache()        # typed schema: Job 2 knows what Job 1 wrote
orders = raw.where(corrupt is null AND order_id is not null).select(*DATA_COLS).dropDuplicates()
```
Job 2's input is machine-generated by our own Job 1, so Spark's normal JSON reader is enough — it only
*defends* against corrupt lines — and `dropDuplicates()` (all columns) makes it safe if the topic had been
published twice.
```python
delayed = F.sum(F.when(F.col("delivery_status") == "DELAYED", 1).otherwise(0))   # conditional count

restaurant = (orders.groupBy("restaurant_id")
    .agg(F.count("*").alias("order_count"),
         F.round(F.avg("order_amount"), 2).alias("avg_order_amount"),
         F.round(F.avg("preparation_time_min"), 2)...,
         F.round(F.avg("delivery_time_min"), 2)...,
         delayed.cast("long").alias("delayed_order_count"))
    .withColumn("delay_rate_pct", F.round(100.0 * F.col("delayed_order_count") / F.col("order_count"), 2))
    .orderBy("restaurant_id"))
```
`groupBy` → one row per restaurant; `agg` → several aggregates in one pass; `delay_rate_pct` is a
column derived from two aggregates (hence `withColumn` after `agg`). The city table is identical with
`groupBy("city")` and without the preparation-time average (that column isn't asked for).

**Questions:** *Why average the already-rounded numbers?* (Job 2's only input is the 2-dp file from Job 1 —
that's the data it has.) · *Why `sum(when(...))` and not `filter+count`?* (one pass, per group) ·
*Is `groupBy` narrow or wide?* (wide – shuffle)

---

## 5. `common/verify_reference.py` — why it exists

Pure Python, standard library only: re-reads the raw `order.jsonl`, applies the *same rules independently*
(`json`, `datetime`, `Decimal`), and compares the result with the files the pipeline produced
(tolerance 0.011, rounding done half-up with `Decimal` to match Spark). If two separately-written
implementations agree on 44 orders and 10 summary rows, the logic is very probably right. It also reports its
own reject-by-reason table, which should equal Spark's. **It is a checking tool, not part of the pipeline** —
say so clearly.

---

## 6. The two config files

Open `broker1.properties` with the TA in mind and be ready to explain, **in this order**: roles → quorum →
listeners vs advertised → storage → replication settings (§6). The single most valuable sentence is:
*"`listeners` is what I bind to; `advertised.listeners` is what I tell clients to connect to, so it's the
ZeroTier IP."*

---

# Viva drill

**How to use it:** read a question aloud, answer without looking, then check. Cover for each other: the
question is hard if the *answerer* is the member who didn't touch that VM.

### A. The system
1. Draw the pipeline and name what runs on each VM. *(VM1: Producer 1, Consumer 2, Spark 2 · VM2: controller + broker 1 · VM3: broker 2 · VM4: Consumer 1, Spark 1, Producer 2)*
2. Why a landing file between Kafka and Spark? *(§13.1)*
3. Why can't Spark read Kafka directly in this assignment — and what would be different if it did? *(rule; streaming would be another model; here: fixed, re-runnable batch input)*
4. How does data get from VM4 to VM1? *(only through the `order_metrics` topic)*
5. What's ZeroTier for? *(§2)*

### B. Kafka
6. Topic, partition, offset — define each. *(§3.2)*
7. Why 2 partitions? Why replication factor 2? *(§3.2, §4.1)*
8. What are numbers like `orders:0:33`, `orders:1:29`? *(end offsets = records per partition = 62)*
9. Is order guaranteed across partitions? *(no — only within one, §3.4)*
10. What's retention, and why does it matter to our consumer? *(§3.3)*
11. What does `bootstrap.servers` do? Why list both brokers? *(§3.5)*
12. What's a leader / follower / ISR? Read this line: `Partition: 0 Leader: 2 Replicas: 2,1 Isr: 2,1`. *(§4.2)*
13. `acks=0/1/all`? Which do we use and why? *(§4.4)*
14. What does `min.insync.replicas=1` do? What if it were 2? *(writes would fail while VM3 is down)*

### C. KRaft and configuration
15. KRaft vs ZooKeeper — one sentence. *(§5.1)*
16. Who's the controller in your cluster? What if it dies? *(VM2; out of scope; needs 3 controllers)*
17. What does `kafka-storage.sh format` do and why the same UUID on both VMs? *(§5.3)*
18. `listeners` vs `advertised.listeners`. What breaks if advertised is `localhost`? *(§6.2)*
19. Why isn't `log.dirs` under `/tmp`? *(§6.3)*
20. Why `offsets.topic.replication.factor=2`? *(§6.4)*

### D. Producers and consumers
21. What does `acks="all"` change in the code? Why `flush()`? *(§7)*
22. Why does the producer send raw lines instead of cleaning? *(§9.2)*
23. Why no consumer group? What happens if you run the consumer twice? *(§8.1)*
24. How does the consumer know when to stop? *(end offsets snapshot)*
25. If Producer 1 ran twice, what changes in the final outputs? *(landed files double, results unchanged – dedupe, §9.3)*

### E. Spark
26. What does `--master 'local[*]'` mean? Why no HDFS? *(§11.1)*
27. Transformation vs action; name three of each from our code. *(§11.3)*
28. Which of our operations cause a shuffle? *(`dropDuplicates`, `groupBy`, `orderBy`)*
29. Walk through the cleaning rules in order. Why that order? *(§12.2)*
30. What does `cast("double")` return for `"abc"`? *(null)*
31. How do you compute `delay_min`, `delivery_status`, `delay_category`? Do it for a row. *(§12.4)*
32. What's `delivery_speed_kmph` for T080001? *(4 ÷ (26/60) = 9.23)*
33. Why does Spark write a directory? How do you get one file? *(§12.7)*
34. Why `.cache()`? *(§11.3)*
35. Rounding: Spark vs Python? *(half-up vs banker's, §12.5)*

### F. The results and the failure test
36. 62 records in — how many out, and where did the rest go? *(44; 6 status, 4 missing, 2 timestamp, 1 distance, 1 amount, 4 duplicates)*
37. Run the failure test and narrate: what changes in `describe`, and what doesn't? *(Leader→1, Isr→1; Replicas unchanged)*
38. Why can we still write with VM3 down? *(§4.4)*
39. Why does the assignment say stop VM3 and not VM2? *(§5.4, §10)*
40. After VM3 returns, how long until `Isr` is `1,2` again, and why isn't it instant? *(follower must fetch and catch up – seconds)*

### G. Trap questions
- *"Your script did all this?"* — There are no wrapper scripts; every cluster command was typed and each is explained in `01-STEPS.md`. The only programs are the five Python files above.
- *"Is it exactly-once?"* — No: at-least-once delivery (acks=all + retries) with idempotent, de-duplicated processing, so results are effectively-once. (§9)
- *"Why not delete rows in the producer?"* — Because the assignment puts cleaning in Spark Job 1 and the rejected counts have to be visible.
- *"Prove your numbers."* — `verify_reference.py` and the hop-by-hop counts 62 → 62 → 44 → 44.

### H. Type-from-memory tasks (do these on the call, timed)
1. Format and start a broker (4 commands, from `random-uuid` to `jps`).
2. Create `orders` with 2 partitions / RF 2 and describe it.
3. Publish the dataset and land it again, check `wc -l`.
4. Run Spark Job 1 and read the cleaning summary aloud.
5. Stop and restart VM3's broker while narrating `Leader` and `Isr`.
