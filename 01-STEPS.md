# 01 — Team run: every command, step by step

No wrapper scripts. Every step is a command you type, followed by **what it does** so that
whoever types it can explain it in the viva. (`§n` = section n of
[`04-CONCEPTS-GUIDE.md`](04-CONCEPTS-GUIDE.md).)

Tags: **[M1]**…**[M4]** = which member types it (M1 = VM1 … M4 = VM4); **[all]** = everyone.
Do the phases in order; each ends with a ✅ check that must pass before moving on.

> **New to this?** Do [`03-SOLO-PRACTICE.md`](03-SOLO-PRACTICE.md) first — the same pipeline on
> one machine with two local brokers. Each of you should do it once before the team call.

---

## Fill-in sheet (write these on the call; you'll type them a lot)

| | Value |
|---|---|
| ZeroTier network ID | `________________` |
| VM1 ZeroTier IP (Member 1) | `___.___.___.___` |
| VM2 ZeroTier IP (Member 2) | `___.___.___.___` |
| VM3 ZeroTier IP (Member 3) | `___.___.___.___` |
| VM4 ZeroTier IP (Member 4) | `___.___.___.___` |
| Cluster ID (from `kafka-storage.sh random-uuid`) | `______________________` |

In **every terminal on every VM** you'll use, define these once (shell variables — they only
live in that terminal; retype after opening a new one):
```bash
export VM2_IP=<VM2's ZeroTier IP>
export VM3_IP=<VM3's ZeroTier IP>
export BOOT="$VM2_IP:9092,$VM3_IP:9092"
echo $BOOT
```
`BOOT` is the **bootstrap list**: both brokers, so a client can still start if one is down (§3.5).

---

## Phase 0 — Check what's already installed  [all]

You've done the Kafka, Spark and ZeroTier guides, so this is only a check:
```bash
java -version                        # 11.x
kafka-topics.sh --version            # 3.9.2          (VM2, VM3 required; VM1, VM4 handy)
spark-submit --version 2>&1 | grep -E "version 3.3.4"   # (VM1, VM4 only)
echo $PYSPARK_PYTHON                 # /usr/bin/python3.10   (VM1, VM4) – must not be empty
jps                                  # nothing Kafka-ish should be running from the old demo
```
If an old single-node Kafka from the course demo is running on VM2/VM3: `kafka-server-stop.sh`.

**Get this folder onto every VM, at `~/assignment2`** (all commands below assume that path), and
(VM1, VM4) create the Kafka Python client:
```bash
git clone git@github.com:Greninja34/bd_assignment2.git ~/assignment2             # [all] see README.md (or Download ZIP → unzip → rename)
python3 -m venv ~/a2venv                                  # [M1, M4]
~/a2venv/bin/pip install kafka-python                     # [M1, M4]
~/a2venv/bin/python -c "import kafka; print(kafka.__version__)"
```
- `venv` keeps `kafka-python` away from the system Python. We call `~/a2venv/bin/python`
  explicitly instead of "activating" the venv, so there's never doubt about which Python runs.
- Spark keeps using Python 3.10 (via `PYSPARK_PYTHON`); the Kafka scripts use the venv Python.
  They're deliberately separate.

---

## Phase 1 — ZeroTier network  [M2 creates, all join]  (§2)

You did this in the ZeroTier guide. Quick recap:
```bash
sudo zerotier-cli join <NETWORK_ID>             # [all]   → 200 join OK
```
**[M2]** authorise the 4 devices in the ZeroTier dashboard. Then **[all]**:
```bash
ip -4 -br a | grep -i zt                         # your ZeroTier IP – write it in the fill-in sheet
ping -c 3 <another VM's ZeroTier IP>             # do the whole 4×4 matrix
```
✅ everyone can ping everyone. Firewall check: `sudo ufw status` should say `inactive`
(else `sudo ufw allow 9092/tcp` and `sudo ufw allow 9093/tcp`).

---

## Phase 2 — Kafka cluster: VM2 first, then VM3  (§3–§6)

### 2.1 [M2] Make the cluster ID — one value for the whole cluster
```bash
kafka-storage.sh random-uuid
```
Copy the output into the group chat → every broker must be formatted with **this same ID** (§5.3).

### 2.2 [M2] Configure and start VM2: controller + broker 1
```bash
cp ~/assignment2/member2_vm2/broker1.properties /opt/kafka/config/kraft/a2-broker1.properties
sed -i 's/<VM2_IP>/'"$VM2_IP"'/g' /opt/kafka/config/kraft/a2-broker1.properties
grep -v '^#' /opt/kafka/config/kraft/a2-broker1.properties | grep -v '^$'      # read it! no <…> may remain
```
- `cp` puts our config next to Kafka's own. `sed` swaps the `<VM2_IP>` placeholder for the real IP
  (Kafka's properties files can't read shell variables).
- **Read the printed file** and be able to say what `process.roles`, `controller.quorum.voters`,
  `listeners` and `advertised.listeners` mean (§6).

```bash
ls -A /opt/kafka/data/kraft            # should be empty or not exist
```
If it contains files from the course's single-node demo, remove that directory
(`rm -rf /opt/kafka/data/kraft`) — it was formatted with a *different* cluster ID.
```bash
mkdir -p /opt/kafka/data/kraft
kafka-storage.sh format -t <CLUSTER_ID> -c /opt/kafka/config/kraft/a2-broker1.properties
kafka-server-start.sh -daemon /opt/kafka/config/kraft/a2-broker1.properties
```
- `format` writes `meta.properties` (cluster ID + node ID) and the empty metadata log into
  `log.dirs`. Kafka refuses to start with an unformatted directory (§5.3).
- `-daemon` runs it in the background; logs go to `/opt/kafka/logs/`.

```bash
jps                                            # a "Kafka" process
ss -tlnp | grep -E '9092|9093'                 # both ports LISTEN
tail -5 /opt/kafka/logs/server.log             # "Kafka Server started"
```
✅ announce **"VM2 up"**.

### 2.3 [M3] Configure and start VM3: broker 2
```bash
cp ~/assignment2/member3_vm3/broker2.properties /opt/kafka/config/kraft/a2-broker2.properties
sed -i -e 's/<VM2_IP>/'"$VM2_IP"'/g' -e 's/<VM3_IP>/'"$VM3_IP"'/g' /opt/kafka/config/kraft/a2-broker2.properties
grep -v '^#' /opt/kafka/config/kraft/a2-broker2.properties | grep -v '^$'
nc -zv $VM2_IP 9093                            # VM3 must reach VM2's controller port
mkdir -p /opt/kafka/data/kraft
kafka-storage.sh format -t <SAME CLUSTER_ID> -c /opt/kafka/config/kraft/a2-broker2.properties
kafka-server-start.sh -daemon /opt/kafka/config/kraft/a2-broker2.properties
jps; ss -tlnp | grep 9092
```
- Differences from VM2: `process.roles=broker` (no controller), `node.id=2`, advertises **VM3's** IP.
- `nc -zv` ("is this port open?") is the fastest way to tell a network problem from a Kafka problem.

### 2.4 ✅ Check the cluster  [any VM with the Kafka CLI]
```bash
kafka-broker-api-versions.sh --bootstrap-server $BOOT 2>/dev/null | grep "id:"
kafka-metadata-quorum.sh --bootstrap-server $VM2_IP:9092 describe --status | head -5
```
Expect: two broker lines (`id: 1` and `id: 2`), and `LeaderId: 1` for the quorum. From **VM1 and
VM4** also run `nc -zv $VM2_IP 9092; nc -zv $VM3_IP 9092` — clients on those VMs must reach both.

---

## Phase 3 — Create the two topics  [M2]  (§3, §4)

```bash
kafka-topics.sh --bootstrap-server $BOOT --create --topic orders --partitions 2 --replication-factor 2
kafka-topics.sh --bootstrap-server $BOOT --create --topic order_metrics --partitions 2 --replication-factor 2
kafka-topics.sh --bootstrap-server $BOOT --describe
```
- Both topics: **2 partitions, replication factor 2**, on the **same** cluster (assignment §8).
- (A harmless warning about `.` / `_` metric-name collisions is printed — ignore it.)
- 📸 Screenshot the `--describe` output.

✅ each topic shows `PartitionCount: 2  ReplicationFactor: 2`, and every partition line has
`Replicas: 1,2` (or `2,1`) and `Isr: 1,2` (or `2,1`). Reading one line: `Leader` is the broker
serving that partition now; `Replicas` are all brokers holding a copy; `Isr` are the ones fully
caught up (§4.2). Announce **"topics created"**.

---

## Phase 4 — Stage 1: orders → enriched order metrics

### 4.1 [M1 on VM1] Producer 1 – publish the dataset
```bash
~/a2venv/bin/python ~/assignment2/common/file_to_kafka.py \
    --input ~/assignment2/member1_vm1/order.jsonl --topic orders --bootstrap "$BOOT"
```
Sends **every line of `order.jsonl` unchanged**, bad ones included — cleaning is Spark's job (§9.2).
Expect `published 62 records to topic 'orders'` and a split like `partition 0: 33 / partition 1: 29` (the exact numbers can differ by a record or two: two lines in the file have no `order_id`, so they go to a random partition)
(§7: records spread over both partitions). Announce **"orders published"**.

### 4.2 [M4 on VM4] Consumer 1 – Kafka → local file
```bash
mkdir -p ~/a2-out && cd ~/a2-out
~/a2venv/bin/python ~/assignment2/common/kafka_to_file.py \
    --topic orders --bootstrap "$BOOT" --out orders_landed.jsonl
wc -l orders_landed.jsonl
head -2 orders_landed.jsonl
```
Reads partitions 0 and 1 from offset 0 up to the end and writes plain lines to a local file
(§8). Expect `landed 62/62` and `62 orders_landed.jsonl`. This file — not Kafka — is what Spark reads.

### 4.3 [M4 on VM4] Spark Job 1 – clean and enrich
```bash
export PYSPARK_PYTHON=/usr/bin/python3.10 PYSPARK_DRIVER_PYTHON=/usr/bin/python3.10
spark-submit --master 'local[*]' ~/assignment2/member4_vm4/spark_job1.py \
    --input orders_landed.jsonl --output order_metrics.jsonl 2>/dev/null
wc -l order_metrics.jsonl
head -2 order_metrics.jsonl
```
- `--master 'local[*]'`: run Spark inside one JVM on this VM using all cores — no cluster, no HDFS (§11.1).
- `2>/dev/null` hides Spark's log noise so the job's own summary is readable. **Drop it** when
  something fails, to see the errors.
- Expect the summary: `input records read: 62` · rejects by reason (6 / 4 / 2 / 1 / 1) ·
  `valid 48` · `exact duplicates removed 4` · `unique valid orders 44`, and `44 order_metrics.jsonl`.

### 4.4 [M4 on VM4] Producer 2 – publish the Spark output
```bash
~/a2venv/bin/python ~/assignment2/common/file_to_kafka.py \
    --input order_metrics.jsonl --topic order_metrics --bootstrap "$BOOT"
```
✅ `published 44 records to topic 'order_metrics'`. Announce **"order_metrics published"**.
(The assignment forbids copying the file VM4→VM1; Kafka is the only transport.)

---

## Phase 5 — Stage 2: analytics  [M1 on VM1]

```bash
mkdir -p ~/a2-out && cd ~/a2-out
~/a2venv/bin/python ~/assignment2/common/kafka_to_file.py \
    --topic order_metrics --bootstrap "$BOOT" --out order_metrics_landed.jsonl
wc -l order_metrics_landed.jsonl

export PYSPARK_PYTHON=/usr/bin/python3.10 PYSPARK_DRIVER_PYTHON=/usr/bin/python3.10
spark-submit --master 'local[*]' ~/assignment2/member1_vm1/spark_job2.py \
    --input order_metrics_landed.jsonl --outdir . 2>/dev/null

cat restaurant_summary.csv
cat city_summary.csv
```
✅ `landed 44/44`; 6 restaurant rows (R01–R06) and 4 city rows; values match
[`02-EXPECTED-RESULTS.md`](02-EXPECTED-RESULTS.md). Announce **"stage 2 done"**.

---

## Phase 6 — Broker-failure test  [M3 on VM3, with M2 helping]  (§4, §10)

Do this **after** Phase 5. The assignment says: stop **VM3** (not VM2), then show the replicated
partitions are still available through the surviving broker.

Use `$VM2_IP:9092` as the **only** bootstrap server while VM3 is down (so it's obvious the survivor
is answering).

**6.1 Before — take a baseline** [M3, any VM with the CLI]
```bash
kafka-topics.sh --bootstrap-server $BOOT --describe --topic orders        # note Leader / Isr per partition
kafka-topics.sh --bootstrap-server $BOOT --describe --topic order_metrics
kafka-get-offsets.sh --bootstrap-server $BOOT --topic orders --time -1        # e.g. orders:0:33  orders:1:29 → the numbers must sum to 62
kafka-get-offsets.sh --bootstrap-server $BOOT --topic order_metrics --time -1 # sum 44
kafka-topics.sh --bootstrap-server $BOOT --create --topic failover_probe --partitions 2 --replication-factor 2
```
- `--time -1` = "latest offset" per partition = how many records each partition holds.
- The probe topic is created **now** because creating an RF-2 topic needs both brokers alive.

**6.2 Stop VM3's broker** [M3 on VM3]
```bash
kafka-server-stop.sh
sleep 5; jps                                  # no Kafka listed any more
```
📸 Wait ~20 s so the cluster notices.

**6.3 During the outage — ask the survivor only** [M2 on VM2, or M3 on VM3]
```bash
kafka-topics.sh --bootstrap-server $VM2_IP:9092 --describe --topic orders
kafka-topics.sh --bootstrap-server $VM2_IP:9092 --describe --topic order_metrics
```
Every partition now: `Leader: 1`, `Isr: 1` (while `Replicas: …,…` still lists both). 📸
Leadership moved to the surviving replica; the ISR shrank because broker 2 is not in sync (§4.3).

**6.4 Existing data still readable** [M3]
```bash
kafka-console-consumer.sh --bootstrap-server $VM2_IP:9092 --topic orders --from-beginning --timeout-ms 15000 | wc -l          # 62
kafka-console-consumer.sh --bootstrap-server $VM2_IP:9092 --topic order_metrics --from-beginning --timeout-ms 15000 | wc -l   # 44
```
(After 15 s of silence the console consumer exits and prints
`ERROR Error processing message, terminating consumer process … TimeoutException` — that's how
`--timeout-ms` ends it, not a failure. The number from `wc -l` is what counts.) 📸

**6.5 New data can still be written** [M3]
```bash
for i in 1 2 3 4 5; do echo "probe-$i"; done | \
  kafka-console-producer.sh --bootstrap-server $VM2_IP:9092 --topic failover_probe --producer-property acks=all
kafka-console-consumer.sh --bootstrap-server $VM2_IP:9092 --topic failover_probe --from-beginning --timeout-ms 10000 | wc -l   # 5
```
`acks=all` + `min.insync.replicas=1` ⇒ the write is acknowledged once the single in-sync replica
has it (§4.4).

**6.6 Bring VM3 back** [M3 on VM3]
```bash
jps                                            # make sure the old process is really gone first
kafka-server-start.sh -daemon /opt/kafka/config/kraft/a2-broker2.properties
```
Wait ~30 s, then [any VM]:
```bash
kafka-topics.sh --bootstrap-server $BOOT --describe --topic orders
```
✅ `Isr` is `1,2` again on every partition (the follower caught up and rejoined). 📸

**6.7 Clean up** [M2]
```bash
kafka-topics.sh --bootstrap-server $BOOT --delete --topic failover_probe
```

---

## Phase 7 — Independent verification  [M3]  (§13)

Collect in one folder: `order.jsonl` (raw), `order_metrics.jsonl` (from VM4),
`order_metrics_landed.jsonl`, `restaurant_summary.csv`, `city_summary.csv` (from VM1).
```bash
python3 ~/assignment2/common/verify_reference.py \
    --raw order.jsonl --metrics order_metrics.jsonl \
    --restaurants restaurant_summary.csv --cities city_summary.csv

diff <(sort order_metrics.jsonl) <(sort order_metrics_landed.jsonl) && echo "same records after the Kafka hop"
wc -l order_metrics.jsonl order_metrics_landed.jsonl                    # 44  44
```
`verify_reference.py` recomputes everything from the raw file with plain Python (no Spark, no Kafka)
and compares row by row (§13.3). ✅ **ALL CHECKS PASSED**. `sort` before `diff` because two partitions
interleave, so the line *order* in the landed file isn't fixed.

---

## Phase 8 — Have ready for the TA

- [ ] The five files: `orders_landed.jsonl` + `order_metrics.jsonl` (VM4) · `order_metrics_landed.jsonl`,
      `restaurant_summary.csv`, `city_summary.csv` (VM1)
- [ ] `--describe` screenshot (2 partitions, RF 2, Isr 1,2)
- [ ] Failure test screenshots: before → VM3 down (Leader 1, Isr 1) → data read → back (Isr 1,2)
- [ ] `verify_reference.py` = ALL CHECKS PASSED
- [ ] **Every member can type any command above from memory and explain it** — rehearse with the
      drill in [`05-CODE-WALKTHROUGH.md`](05-CODE-WALKTHROUGH.md)

---

## Re-running / resetting

**Clean re-run of the data path** (e.g. before the demo): empty both topics, then repeat Phases 4–5.
```bash
kafka-topics.sh --bootstrap-server $BOOT --delete --topic orders
kafka-topics.sh --bootstrap-server $BOOT --delete --topic order_metrics
sleep 5
# …then the two --create commands from Phase 3…
```
Skipping the reset isn't fatal — both Spark jobs drop exact duplicates, so the *results* are
unchanged — but the landed files would contain every record twice.

**Fresh cluster / wrong cluster ID:** stop both brokers (`kafka-server-stop.sh`), delete
`/opt/kafka/data/kraft` on both, pick a new ID, redo Phase 2.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `NoBrokersAvailable` / client can't connect | wrong IP in `BOOT`, device not authorised in ZeroTier, firewall | `echo $BOOT`; `nc -zv $VM2_IP 9092`; `ping`; Phase 1 |
| Client connects, then hangs or times out on send | broker *advertises* an address clients can't reach (e.g. `localhost`) | `grep advertised /opt/kafka/config/kraft/a2-broker*.properties` must show the ZeroTier IP; fix, restart broker (§6.2) |
| Broker 2 won't start: *cluster ID mismatch* / *Unexpected cluster id* | VM3 data dir formatted with a different ID (often from the old demo) | stop Kafka, `rm -rf /opt/kafka/data/kraft`, `mkdir -p`, re-run `format` with the shared ID |
| Broker 2 starts but never registers | VM3 can't reach `VM2_IP:9093` | `nc -zv $VM2_IP 9093` from VM3; firewall; VM2 up first |
| `--create`: "replication factor 2 larger than available brokers" | broker 2 not up | Phase 2.4 check; start VM3 |
| `format` refuses: *already formatted* | directory has a `meta.properties` | intended; delete the dir if you really want a fresh cluster |
| `Address already in use` | a Kafka is already running | `jps`; `kafka-server-stop.sh` |
| Spark: `Python worker failed` / cloudpickle | PySpark running on Python 3.11/3.12 | `echo $PYSPARK_PYTHON` must be `/usr/bin/python3.10` |
| Spark: `Unsupported class file major version` | wrong Java | Java 11 |
| `kafka_to_file.py` exits with code 2 (`landed 40/62`) | producer still running / idle timeout | wait for "published"; re-run (it re-lands the whole topic) |
| `ModuleNotFoundError: kafka.vendor.six.moves` | old kafka-python on Python 3.12 | `~/a2venv/bin/pip install -U kafka-python` |
| `order_metrics_landed.jsonl` has 88 lines | `order_metrics` was published twice | reset topics, redo Phases 4–5 (results were still right) |
| `kafka-server-stop.sh`: "No kafka server to stop" | nothing running under this user | `jps` |
