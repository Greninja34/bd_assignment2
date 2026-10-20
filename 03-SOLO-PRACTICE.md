# 03 — Solo practice: the whole pipeline on ONE machine

**Goal:** each of the four of you runs *every* stage yourself once, so you can explain any part in
the viva — not just the part your VM owns. No ZeroTier, no teammates needed: you run both Kafka
brokers on your own VM (or laptop) on different ports and play all the roles.

Time: about **60–90 minutes** the first time, ~20 minutes once you've done it (which is how long
the "type it from memory" demo should take).

Requirements: Java 11, Kafka 3.9.2 (`kafka-topics.sh` on `PATH`), Spark 3.3.4 with
`PYSPARK_PYTHON=/usr/bin/python3.10`, and this folder at `~/assignment2` (`git clone git@github.com:Greninja34/bd_assignment2.git ~/assignment2`). Nothing else is installed.

Everything below is typed by hand; read the *Why* lines — they're the viva answers.

---

## 0. One-time setup

```bash
python3 -m venv ~/a2venv
~/a2venv/bin/pip install kafka-python
mkdir -p /tmp/a2-practice ~/a2-solo && cd ~/a2-solo
```
*Why a venv:* isolates `kafka-python` from the system Python. We call `~/a2venv/bin/python`
directly instead of "activating" it, so it's always clear which interpreter runs.

---

## 1. Start a two-broker KRaft cluster on your machine

Two properties files are provided: `practice/solo-broker1.properties` (controller + broker 1,
ports 9092/9093) and `practice/solo-broker2.properties` (broker 2, port 9094). **Open both and read
them** — they're the same as the team's files minus the ZeroTier IPs.

```bash
export CID=$(kafka-storage.sh random-uuid)       # the cluster's identity (§5.3)
echo $CID

kafka-storage.sh format -t $CID -c ~/assignment2/practice/solo-broker1.properties
kafka-storage.sh format -t $CID -c ~/assignment2/practice/solo-broker2.properties
```
*Why format twice:* each broker has its own data directory (`/tmp/a2-practice/kraft-1`, `-2`); both
must carry the **same cluster ID** or broker 2 won't join.

```bash
kafka-server-start.sh -daemon ~/assignment2/practice/solo-broker1.properties
sleep 8
kafka-server-start.sh -daemon ~/assignment2/practice/solo-broker2.properties
sleep 8
jps -lm | grep -i kafka
ss -tlnp | grep -E '9092|9093|9094'
```
*Why this order:* broker 2 must be able to reach the controller (inside broker 1) when it starts.
Expect two `kafka.Kafka …solo-broker{1,2}.properties` lines and three listening ports.

```bash
export BOOT=127.0.0.1:9092,127.0.0.1:9094
kafka-broker-api-versions.sh --bootstrap-server $BOOT 2>/dev/null | grep "id:"       # id: 1 and id: 2
kafka-metadata-quorum.sh --bootstrap-server 127.0.0.1:9092 describe --status | head -5   # LeaderId: 1
```

---

## 2. Create the topics

```bash
kafka-topics.sh --bootstrap-server $BOOT --create --topic orders --partitions 2 --replication-factor 2
kafka-topics.sh --bootstrap-server $BOOT --create --topic order_metrics --partitions 2 --replication-factor 2
kafka-topics.sh --bootstrap-server $BOOT --describe
```
**Stop and read the output.** For each partition find `Leader`, `Replicas`, `Isr`. You should be
able to say: "partition 0 of `orders` is led by broker X, has a copy on both brokers, and both are in
sync."

---

## 3. Stage 1 — Producer 1 → Consumer 1 → Spark Job 1 → Producer 2

```bash
# Producer 1: dataset -> topic orders
~/a2venv/bin/python ~/assignment2/common/file_to_kafka.py \
    --input ~/assignment2/member1_vm1/order.jsonl --topic orders --bootstrap "$BOOT"

# Consumer 1: topic orders -> local file
~/a2venv/bin/python ~/assignment2/common/kafka_to_file.py \
    --topic orders --bootstrap "$BOOT" --out orders_landed.jsonl
wc -l orders_landed.jsonl                              # 62

# Spark Job 1
export PYSPARK_PYTHON=/usr/bin/python3.10 PYSPARK_DRIVER_PYTHON=/usr/bin/python3.10
spark-submit --master 'local[*]' ~/assignment2/member4_vm4/spark_job1.py \
    --input orders_landed.jsonl --output order_metrics.jsonl 2>/dev/null
wc -l order_metrics.jsonl                              # 44

# Producer 2: Spark output -> topic order_metrics
~/a2venv/bin/python ~/assignment2/common/file_to_kafka.py \
    --input order_metrics.jsonl --topic order_metrics --bootstrap "$BOOT"
```

**Experiments — do these, each teaches one concept:**

1. **Peek inside Kafka** with the console tools:
   ```bash
   kafka-console-consumer.sh --bootstrap-server $BOOT --topic orders --from-beginning --timeout-ms 8000 \
       --property print.partition=true --property print.offset=true | head -8
   ```
   You see `Partition:0 Offset:0 {…}`. Note offsets start at 0 *per partition* and the two partitions
   interleave (§3.2).
2. **Offsets = record counts:**
   `kafka-get-offsets.sh --bootstrap-server $BOOT --topic orders --time -1` → e.g. `orders:0:33`, `orders:1:29` (they must add up to 62).
   The numbers are the next offset to be written, i.e. how many records each partition holds (§3.2).
3. **Run Producer 1 a second time**, then re-run Consumer 1 and Spark Job 1. `orders_landed.jsonl` now
   has 124 lines — but `order_metrics.jsonl` still has **44**. Why? (Exact duplicates are dropped
   — §9.3.) Reset the topics afterwards (section 6).
4. **Look at what Spark rejected**: remove `2>/dev/null` and read the cleaning summary. Match each
   count to the offending lines in `order.jsonl` (table in `02-EXPECTED-RESULTS.md`).

---

## 4. Stage 2 — Consumer 2 → Spark Job 2

```bash
~/a2venv/bin/python ~/assignment2/common/kafka_to_file.py \
    --topic order_metrics --bootstrap "$BOOT" --out order_metrics_landed.jsonl
spark-submit --master 'local[*]' ~/assignment2/member1_vm1/spark_job2.py \
    --input order_metrics_landed.jsonl --outdir . 2>/dev/null
cat restaurant_summary.csv city_summary.csv
```
Compare with `02-EXPECTED-RESULTS.md`. Then:
```bash
python3 ~/assignment2/common/verify_reference.py --raw ~/assignment2/member1_vm1/order.jsonl \
    --metrics order_metrics_landed.jsonl --restaurants restaurant_summary.csv --cities city_summary.csv
```
Expect `ALL CHECKS PASSED`.

**Experiment:** hand-compute one restaurant. Take R01's rows from `order_metrics_landed.jsonl`,
average `order_amount`, count rows with `delivery_status == DELAYED`, divide. It must equal the CSV.

---

## 5. Broker-failure test — this is the one the TA will watch

```bash
kafka-topics.sh --bootstrap-server $BOOT --describe --topic orders          # BEFORE: Isr 1,2
kafka-topics.sh --bootstrap-server $BOOT --create --topic failover_probe --partitions 2 --replication-factor 2

# stop ONLY broker 2 (kafka-server-stop.sh would stop both – they share this machine)
kill $(jps -lm | awk '/solo-broker2/{print $1}')
sleep 20; jps -lm | grep -i kafka                    # only solo-broker1 left

kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --describe --topic orders # DURING: Leader 1, Isr 1
kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic orders --from-beginning --timeout-ms 15000 | wc -l   # 62
kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic order_metrics --from-beginning --timeout-ms 15000 | wc -l  # 44
for i in 1 2 3 4 5; do echo "probe-$i"; done | \
  kafka-console-producer.sh --bootstrap-server 127.0.0.1:9092 --topic failover_probe --producer-property acks=all
kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic failover_probe --from-beginning --timeout-ms 10000 | wc -l  # 5

# bring broker 2 back
kafka-server-start.sh -daemon ~/assignment2/practice/solo-broker2.properties
sleep 30
kafka-topics.sh --bootstrap-server $BOOT --describe --topic orders          # AFTER: Isr 1,2 again
kafka-topics.sh --bootstrap-server $BOOT --delete --topic failover_probe
```
Each console consumer ends with `ERROR Error processing message, terminating consumer process …
TimeoutException`. That's just `--timeout-ms` stopping it after 15 s of silence — not a failure; the
`wc -l` number is what counts.

*In the team run you simply use `kafka-server-stop.sh` on VM3 — it's alone on its machine.*

**Questions to answer out loud while you watch:** Why did `Leader` change? Why did `Isr` shrink but
`Replicas` didn't? Why can we still write? What would happen with `min.insync.replicas=2`? What if
we had stopped broker 1 instead? (§4, §10)

---

## 6. Reset and clean up

Empty the topics for a fresh run:
```bash
kafka-topics.sh --bootstrap-server $BOOT --delete --topic orders
kafka-topics.sh --bootstrap-server $BOOT --delete --topic order_metrics
```
Shut everything down and remove the practice data when you're done:
```bash
kafka-server-stop.sh                    # stops both brokers (same machine)
sleep 8; jps                            # no Kafka
rm -rf /tmp/a2-practice ~/a2-solo
```

---

## 7. Self-check before the team call

You're ready if you can, **without looking**:
- [ ] bring up two brokers and say what each line of the properties file does
- [ ] create the topics and read a `--describe` line aloud
- [ ] run all six pipeline commands and state the expected count after each (62 / 62 / 44 / 44 / 44 / 6+4 rows)
- [ ] run the failure test and narrate Leader / Isr changes
- [ ] explain one cleaning rule's code in `spark_job1.py` (see `05-CODE-WALKTHROUGH.md`)
