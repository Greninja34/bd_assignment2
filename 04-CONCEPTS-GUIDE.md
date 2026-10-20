# 04 — Concepts guide: what's really going on in this assignment

Read this to *understand* the assignment, not just run it. Each section ends with **"Say it in the
viva"** — the answer in two or three sentences. Sections are numbered because the other documents
point here (`§4.3` etc.).

| § | Topic |
|---|---|
| 1 | The big picture — what problem each tool solves |
| 2 | Networking: ZeroTier |
| 3 | Kafka fundamentals |
| 4 | Replication, leaders, ISR, acks |
| 5 | KRaft: Kafka without ZooKeeper |
| 6 | The broker config, line by line |
| 7 | Producers |
| 8 | Consumers |
| 9 | Delivery guarantees and duplicates |
| 10 | What happens when things fail |
| 11 | Spark fundamentals |
| 12 | Spark in this assignment |
| 13 | Data-pipeline patterns we used |
| 14 | Glossary |

---

## 1. The big picture

A food-delivery company produces a stream of order records. Two different problems:

1. **Moving data reliably between machines that don't know about each other** → **Kafka**. A
   producer writes to a *topic*; any number of consumers read it later, at their own pace. Kafka
   stores the data durably and, with replication, survives a machine dying.
2. **Computing over a pile of data** (clean it, add columns, group and average) → **Spark**. Spark
   reads a dataset, applies a described transformation, and writes a result.

The assignment glues them in a **two-stage batch pipeline**:

```
order.jsonl ─Producer1→ [Kafka: orders] ─Consumer1→ orders_landed.jsonl ─Spark1→ order_metrics.jsonl
                                                                                    │ Producer2
 restaurant_summary.csv ◄─Spark2─ order_metrics_landed.jsonl ◄─Consumer2─ [Kafka: order_metrics] ◄┘
 city_summary.csv
```

Kafka is the *transport* between stages; Spark is the *computation* inside each stage; the local
files between them are the **landing zone** (§13.1) so that Spark never talks to Kafka directly.

**Batch, not streaming.** Spark runs once over a fixed file and finishes. (Spark *could* read Kafka
as a stream; the assignment explicitly doesn't want that.)

**Say it in the viva:** "Kafka moves the data reliably between VMs; Spark does the batch computation;
a local file sits between them so each Spark job is a plain, re-runnable batch job over fixed input."

---

## 2. Networking: ZeroTier

Our four VMs sit on four different home/college networks, usually behind NAT, so they can't open
connections to each other. **ZeroTier** builds a *virtual private LAN* over the internet:

- You create a network (ID = 16 hex chars). Each VM runs the ZeroTier client and *joins* it.
- You *authorise* each device in the dashboard (otherwise anyone who knew the ID could join).
- ZeroTier gives each VM a virtual interface (`ztXXXX`) with an IP in a private range. VMs can now
  reach each other at those IPs as if on one switch.

Why it matters for Kafka: **every address you configure must be the ZeroTier IP**, because that's
the only address the other VMs can reach (§6.2).

Debug in this order: `ping <zt-ip>` (is the overlay working?) → `nc -zv <zt-ip> 9092` (is the port
reachable?) → only then suspect Kafka. `nc` ("netcat") with `-z` just tests whether a TCP port accepts
connections.

**Say it in the viva:** "ZeroTier is an overlay network: it gives VMs on different networks private
IPs in one virtual LAN, which is what lets the brokers and clients reach each other."

---

## 3. Kafka fundamentals

### 3.1 Brokers and the cluster
A **broker** is one Kafka server process: it stores data on disk and serves reads/writes. A
**cluster** is several brokers working together. Ours: broker 1 (VM2) and broker 2 (VM3). Each broker
has a numeric `node.id`.

### 3.2 Topics, partitions, offsets — the core model
- A **topic** is a named stream of records (`orders`, `order_metrics`).
- A topic is split into **partitions**. A partition is an **ordered, append-only log** on disk.
  New records go to the end; nothing is modified in place.
- Every record in a partition gets an **offset**: 0, 1, 2, … assigned in append order. The offset is
  only meaningful *within one partition*: "partition 1, offset 7" identifies a record uniquely.
- The **end offset** (a.k.a. log-end offset) of a partition is the offset the *next* record will get —
  so it equals the number of records stored (when nothing has been deleted). That's why
  `kafka-get-offsets.sh --time -1` printing e.g. `orders:0:33` and `orders:1:29` means 62 records in total.

Why partitions? **Parallelism and scale**: different partitions can live on different brokers and be
read by different consumers. We use 2 because the assignment says so; it also lets both brokers lead one
partition each, which is exactly what you see in `--describe`.

### 3.3 Retention
Kafka is not a queue that deletes on read. Records stay until **retention** removes them (default 7 days,
`log.retention.hours=168`) — so you can re-read the same topic from offset 0 as often as you like. Our
consumers rely on that: each run re-lands the whole topic.

### 3.4 Ordering
Kafka guarantees order **within a partition only**. Records of one topic spread over two partitions have
no global order — which is why `orders_landed.jsonl` may list records in a different order than
`order.jsonl`, and why we `sort` before comparing files.

### 3.5 Bootstrap servers and metadata
A client is given a `bootstrap.servers` list. It connects to **one** of them just to ask "who are all the
brokers, and which broker leads which partition?" (that's **metadata**). Then it connects **directly**
to the right brokers, using the addresses the brokers *advertise* (§6.2). Hence:
- the bootstrap list is only an entry point — we list both brokers so a client can still start if one is
  down;
- if the advertised address is wrong, the first contact works and every later one fails — the classic
  "connects, then hangs" symptom.

**Say it in the viva:** "A topic is split into partitions; each partition is an append-only log with
sequential offsets. Order is guaranteed per partition. Data is retained by time, not deleted on read,
so consumers can re-read from offset 0."

---

## 4. Replication, leaders, ISR, acks

Replication is what makes the broker-failure test possible.

### 4.1 Leader and followers
With **replication factor 2**, each partition has two **replicas**, one per broker. Exactly one is the
**leader**: all produces and consumes for that partition go through the leader. The other is a
**follower**, which continuously fetches new records from the leader and appends them to its own copy.
You can't have RF larger than the number of brokers — hence RF = 2 with two brokers.

Leaders are spread across brokers, so with two partitions you typically see partition 0 led by one broker
and partition 1 by the other (balanced load).

### 4.2 Reading a `--describe` line
```
Topic: orders  Partition: 0  Leader: 2  Replicas: 2,1  Isr: 2,1
```
| Field | Meaning |
|---|---|
| `Leader: 2` | broker 2 is serving this partition right now |
| `Replicas: 2,1` | the partition is stored on brokers 2 and 1 (the first is the *preferred* leader) |
| `Isr: 2,1` | **In-Sync Replicas**: replicas that are caught up with the leader |

### 4.3 The ISR — the key idea
A replica is *in sync* if it has kept up with the leader recently (`replica.lag.time.max.ms`, default
30 s). If a follower dies or falls behind, the leader drops it from the ISR; when it returns and catches
up, it rejoins. In the failure test:

1. Healthy: `Replicas: 2,1  Isr: 2,1`.
2. Stop broker 2: partitions it led need a new leader. The controller promotes a replica **from the ISR**
   — broker 1. Now `Leader: 1`, and broker 2 is out of sync, so `Isr: 1`. **`Replicas` is unchanged**
   (it's the assignment, not the live state).
3. Restart broker 2: it fetches what it missed from the leader, catches up, and `Isr` returns to `1,2`.

### 4.4 `acks` and `min.insync.replicas` — what "written" means
The producer's `acks` decides when a write counts as successful:

| `acks` | success when | risk |
|---|---|---|
| `0` | sent, no confirmation | can lose data silently |
| `1` | the leader wrote it | lost if the leader dies before followers copy it |
| `all` | **every replica currently in the ISR** wrote it | strongest |

`min.insync.replicas` (we use 1) is a floor: with `acks=all`, if the ISR has fewer members than this,
the write is **rejected**. With the ISR = {1} and the floor = 1, writes still succeed during the outage.
Had we set it to 2, writes would fail while broker 2 is down — safer against data loss, less available.
That's the classic **consistency vs availability** trade-off.

### 4.5 Leader election
When a leader's broker dies, the **controller** (§5) elects a new leader **only from the ISR**. Allowing a
non-ISR replica to lead ("unclean leader election", off by default) could serve a log missing recent
records. A graceful stop (`kafka-server-stop.sh`) is even smoother: the broker first asks the controller
to move its leaderships away, then shuts down.

**Say it in the viva:** "Each partition has a leader and a follower on different brokers. When VM3 stops,
the controller promotes VM2's in-sync replica to leader, so the data stays readable and writable. The ISR
shrinks to broker 1 and returns to 1,2 when VM3 comes back."

---

## 5. KRaft: Kafka without ZooKeeper

### 5.1 What problem it solves
Someone has to keep the cluster's **metadata**: which topics exist, which broker leads which partition,
which brokers are alive. Old Kafka stored this in a separate **ZooKeeper** cluster. **KRaft**
("Kafka Raft") moves it *into Kafka itself*: the metadata is a replicated log (an internal topic,
`__cluster_metadata`) kept consistent with the **Raft** consensus algorithm. One system instead of two
(you saw this in the course's KRaft guide, §6 "ZooKeeper vs KRaft").

### 5.2 Controllers and brokers
- **Controller**: a node in the *controller quorum*; the quorum elects one *active controller* that
  makes metadata decisions (create topic, elect leaders, track broker liveness).
- **Broker**: stores topic data; it reads metadata from the controller.
- `process.roles` says which hats a node wears: `broker`, `controller`, or both ("combined mode").

Raft needs a **majority** of voters alive: with 1 voter the majority is 1; with 3 voters the cluster
survives 1 failure. Real clusters use 3 dedicated controllers. **We have one** (on VM2), which is exactly
why the assignment says: stop VM3 only — losing the only controller is out of scope.

### 5.3 Cluster ID and formatting
Before first start, every node's data directory must be **formatted** with the cluster's UUID:
```bash
kafka-storage.sh format -t <CLUSTER_ID> -c server.properties
```
This writes `meta.properties` containing `cluster.id` and `node.id`, plus the initial metadata log.
On startup the broker compares it with what the controller expects; a **different UUID ⇒ the broker
refuses to join**. So: generate one ID (`random-uuid`), use it on **both** VMs. If a VM still has data from
the course's single-node demo, it holds a different ID — delete that directory and re-format.

### 5.4 Our topology
```
VM2: process.roles=broker,controller  node.id=1   ← the only controller, also a broker
VM3: process.roles=broker             node.id=2   ← broker; asks VM2 for metadata
```
VM3 has `controller.quorum.voters=1@<VM2>:9093` so it knows where the controller is.

**Say it in the viva:** "KRaft stores cluster metadata inside Kafka using Raft instead of ZooKeeper. VM2 is
broker and the single controller; VM3 is a broker only. Each node's disk is formatted with the same cluster
UUID, otherwise it won't join."

---

## 6. The broker config, line by line

(`member2_vm2/broker1.properties` and `member3_vm3/broker2.properties` — open them next to this.)

### 6.1 Identity and roles
| Property | Meaning |
|---|---|
| `process.roles` | `broker,controller` on VM2; `broker` on VM3 |
| `node.id` | unique node number (1, 2); also what `Leader:`/`Isr:` print |
| `controller.quorum.voters=1@<VM2_IP>:9093` | the voters as `id@host:port`. Same on every node |

### 6.2 `listeners` vs `advertised.listeners` — the #1 source of bugs
| Property | Meaning | Ours |
|---|---|---|
| `listeners` | what this process **binds to** (where it listens) | `PLAINTEXT://0.0.0.0:9092` (+ `CONTROLLER://0.0.0.0:9093` on VM2) |
| `advertised.listeners` | what it **tells clients** to connect to | `PLAINTEXT://<own ZeroTier IP>:9092` |

`0.0.0.0` means "all network interfaces" — needed so connections arriving via the ZeroTier interface are
accepted. But clients can't connect *to* 0.0.0.0; they need a real address, and that's the advertised one.
If it said `localhost`, a client on VM1 would reach VM2 for metadata, be told "talk to localhost", and try
its *own* port 9092. Also: `inter.broker.listener.name`, `controller.listener.names`,
`listener.security.protocol.map` just name the listeners and say they're unencrypted (`PLAINTEXT`).

### 6.3 Storage
`log.dirs=/opt/kafka/data/kraft` – where partitions and metadata are kept. The default is under `/tmp`,
which many systems clear on reboot — a classic cause of "my topics vanished".

### 6.4 Replication-related settings
| Property | Value | Why |
|---|---|---|
| `num.partitions` | 2 | default partition count |
| `default.replication.factor` | 2 | default RF |
| `offsets.topic.replication.factor` | 2 | internal `__consumer_offsets` topic: if RF stayed 1 and it lived on VM3, stopping VM3 would break consumer groups |
| `transaction.state.log.*` | 2 / min.isr 1 | same reasoning for the internal transaction topic |
| `min.insync.replicas` | 1 | writes survive with one replica (§4.4) |
| `auto.create.topics.enable` | false | we create topics explicitly, never by typo |

---

## 7. Producers

A **producer** sends records to a topic. Our `common/file_to_kafka.py` uses `kafka-python`'s
`KafkaProducer`. What happens on `producer.send(topic, value, key)`:

1. **Serialize** – Kafka transports bytes; we pass raw bytes (each line of the file) untouched.
2. **Choose a partition** – if there's a **key**, partition = `hash(key) mod #partitions` (the same key
   always lands in the same partition, preserving per-key order). With no key the client spreads records
   across partitions. We use `order_id` as the key when the line is valid JSON with one; otherwise none.
   This is why you saw a roughly even split (e.g. 33 / 29).
3. **Batch** – records are buffered per partition and sent together (`linger_ms=20` waits briefly to fill
   batches → much better throughput).
4. **Send to the partition's leader** and wait for `acks` (we use `all`, §4.4).
5. **Retries** – transient errors are retried (`retries=10`).

`producer.flush()` blocks until everything buffered has been acknowledged; `close()` then releases
resources. Without `flush()` a short script could exit before the records actually left.

`send()` is **asynchronous** — it returns a future immediately. We attach a *callback* to count successes
per partition and an *errback* to collect failures; the script exits non-zero if any failed.

**Why Producer 1 does not parse or clean:** the cleaning rules belong to Spark Job 1, and the dirty
records must survive the Kafka hop to be counted and rejected there (§9.2).

**Say it in the viva:** "The producer batches records, picks a partition by key hash, sends to that
partition's leader with acks=all, and we flush before exit so nothing is left in the buffer."

---

## 8. Consumers

A **consumer** reads records from partitions, tracking its **position** (next offset to read) per partition.

### 8.1 Two ways to consume
| | **Consumer group** (`subscribe`) | **Manual assignment** (`assign`) — what we use |
|---|---|---|
| Who picks partitions | Kafka's group coordinator splits them among members | we pick them |
| Where the position is kept | committed in the internal `__consumer_offsets` topic | in the program only |
| Re-run behaviour | continues where the group left off (a second run may read **nothing**) | we choose: start at offset 0 |
| Good for | long-running scalable services | **re-runnable batch landing** |

Our `kafka_to_file.py`:
1. asks for the topic's partitions (`partitions_for_topic`);
2. `assign`s all of them and `seek_to_beginning`;
3. records each partition's **end offset at start** (`end_offsets`);
4. polls until every partition's position reaches that end offset;
5. writes each record's bytes + newline to `<out>.tmp`, then renames to the final name — so a crash can
   never leave a half-written file for Spark to pick up.

It reports `landed N/N`; fewer than expected ⇒ exit code 2. Stopping at a **known end offset** (rather than
"until quiet") makes the result deterministic.

### 8.2 `poll()`
`poll(timeout_ms)` is the heart of every consumer: fetch whatever is available (up to the timeout) and
return it grouped by partition. All the reading logic lives in the loop around it.

### 8.3 Consumer groups (you did these in the course demo)
If you did use a group: members of the *same* group split partitions (each partition → one member), a
*different* group gets its own full copy of the stream, and when members join/leave Kafka **rebalances**.
We didn't need it — one reader per landed file.

**Say it in the viva:** "Consumers pull. I assign both partitions manually and read from the beginning to
the end offset I captured at start, then write the lines to a local file atomically. No consumer group, so
no committed offsets and re-runs always re-land the whole topic."

---

## 9. Delivery guarantees and duplicates

### 9.1 The three semantics
- **At-most-once** – never duplicated, may be lost.
- **At-least-once** – never lost, may be duplicated. A producer that retries after a timeout may resend a
  record the broker had in fact already stored. *(Our setup: `acks=all` + retries.)* To my knowledge
  `kafka-python` doesn't implement Kafka's idempotent producer, so we treat duplicates as possible.
- **Exactly-once** – needs idempotent/transactional producers or an idempotent sink.

### 9.2 Why cleaning is in Spark, not the producer
The assignment's Spark Job 1 "must ignore" malformed/invalid rows. If the producer silently dropped them, the
rejected counts would be invisible and the landed file would no longer be a faithful copy of the source.
**Transport faithfully, clean explicitly, and report what was dropped** (62 in → 14 rejected → 4 duplicates →
44 out).

### 9.3 Dedupe makes everything safe
Both Spark jobs apply `dropDuplicates` on exact records. So even if a record is delivered twice — a retry,
or the whole topic published twice by mistake — the **results are unchanged**. This turns at-least-once
delivery into *effectively-once* results. ("Exact duplicate" = all 14 fields equal; two rows with the same
`order_id` but different content are *not* exact duplicates — in your data those are always one good row plus
one corrupted copy, and the corrupted copy is dropped by the cleaning rules.)

---

## 10. What happens when things fail

| Failure | Effect in our cluster | Why |
|---|---|---|
| **VM3 broker stops** (the test) | Reads and writes continue via VM2; `Leader: 1`, `Isr: 1` | replicas on VM2 are in sync and get promoted; `min.insync.replicas=1` (§4) |
| VM3 returns | follower catches up; `Isr: 1,2` | follower fetches from the leader |
| **VM2 stops** (excluded) | metadata/controller gone: no leader elections, topic changes or new broker registrations; VM3 cannot be the controller | only one controller voter; surviving would need 3 controllers |
| A consumer crashes mid-run | nothing lost; re-run re-lands from offset 0 | records retained; manual assignment; atomic rename |
| A producer crashes mid-run | some records published, some not | re-run publishes everything; dedupe makes the repeats harmless |
| Wrong `advertised.listeners` | clients connect to bootstrap then hang | clients are told an unreachable address (§6.2) |
| Different cluster ID | broker won't join | §5.3 |

Common wrong assumption: "if VM3 is down, `Replicas` shows only broker 1". No: `Replicas` is the *assignment*;
only `Isr` and `Leader` reflect live state.

---

## 11. Spark fundamentals

### 11.1 Architecture and `local[*]`
A Spark application has a **driver** (runs your program, plans the work) and **executors** (do the work in
parallel). On a cluster these are separate machines; with `--master 'local[*]'` everything runs in **one JVM
on this machine**, with a thread per CPU core (`*`). That's exactly right for our small file and means no
Hadoop/YARN/HDFS is involved — the assignment forbids HDFS.

### 11.2 DataFrames
A **DataFrame** is a distributed table with named, typed columns — like a SQL table or pandas DataFrame, but
processed in parallel and *planned lazily*. You built these in the Spark SQL hands-on.

### 11.3 Lazy evaluation: transformations vs actions
- **Transformations** (`select`, `filter/where`, `withColumn`, `groupBy`, `dropDuplicates`, `orderBy`) only
  *describe* a computation; nothing runs.
- **Actions** (`count`, `show`, `collect`, `write`) trigger execution of everything needed.
Spark's **Catalyst optimizer** turns the description into an efficient physical plan.
`cache()` keeps a DataFrame in memory after its first computation, so later actions reuse it instead of
re-reading and re-parsing the file (we cache the parsed input because we count it several times).

### 11.4 Narrow vs wide, shuffles, stages
- **Narrow** transformations (filter, withColumn, select) work on each partition independently.
- **Wide** ones (`groupBy`, `dropDuplicates`, `orderBy`, joins) must **shuffle** rows between partitions so
  matching rows meet. A shuffle ends a *stage* and writes intermediate data, so it's the expensive part.
- `spark.sql.shuffle.partitions` (default 200) sets how many partitions a shuffle produces. For a 62-row file
  200 is silly, so we set 4.

### 11.5 Spark's data partitions vs Kafka partitions
Same word, different thing. **Kafka partitions** = pieces of a topic across brokers. **Spark partitions** =
pieces of a dataset processed in parallel. They only relate if Spark reads Kafka — which we don't.

**Say it in the viva:** "Spark builds a lazy plan from DataFrame transformations and only runs it when an
action like count or write is called. Wide operations like dropDuplicates or groupBy cause a shuffle. We run
in local mode, so no cluster or HDFS is involved."

---

## 12. Spark in this assignment

### 12.1 Reading JSON lines safely (Job 1)
We read the file as **plain text** (`spark.read.text`) — one row per line, column `value` — rather than
letting Spark parse JSON directly. Why:
- Spark's JSON reader is *lenient*: it accepts some malformed lines (we saw `{…}}` with a stray brace pass).
  The assignment says malformed JSON must be ignored, so we test each line with a strict Python check inside
  a **UDF** (user-defined function) that tries `json.loads` and requires an object. UDFs are slower than
  built-in functions (rows travel to a Python process and back) — fine for this size, and the price of strictness.
- Then `from_json(value, schema)` parses valid lines into columns. We declare **every field as a string** on
  purpose: if a number arrives as `"abc"`, a typed schema might turn it to null or crash; with strings the
  value survives and *our* rule decides it's invalid.

### 12.2 The cleaning rules as a `when` chain
```python
reason = (F.when(~json_ok,          "malformed_json")
           .when(missing,           "missing_required_field")
           .when(bad_numeric,       "invalid_number")
           .when(bad_ts,            "invalid_timestamp")
           .when(distance <= 0,     "distance_km<=0")
           .when(amount <= 0,       "order_amount<=0")
           .when(trim(status) != "DELIVERED", "status_not_delivered"))
```
`when(...).when(...)` is SQL `CASE WHEN`: the **first matching branch wins**, and rows matching none get
`NULL` — so *valid rows have a null reason*. Order matters for two reasons:
1. we get one clear reason per rejected row (a record both missing a field and having a bad timestamp is
   counted as "missing");
2. **SQL null logic**: comparing `NULL <= 0` yields `NULL`, which `when` treats as *false*. So we must
   catch null/invalid numbers **before** the `<= 0` tests, otherwise a missing distance would slip through as
   "not ≤ 0".

### 12.3 Casting and parsing dates
- `cast("double")` on text that isn't a number gives **`NULL`** (Spark's default, non-ANSI mode) → detected as
  "invalid_number". We also reject `NaN` and ±Infinity.
- `to_timestamp(col, "yyyy-MM-dd HH:mm:ss")` returns `NULL` for strings that don't fit (`"not-a-timestamp"`,
  `"bad"`) → "invalid_timestamp". The setting `spark.sql.legacy.timeParserPolicy=CORRECTED` makes mismatches
  return null instead of raising.
- Session time zone is set to UTC so date maths can't be affected by daylight-saving gaps.

### 12.4 Time arithmetic and the formulas
Casting a timestamp to `double` gives seconds since 1970. So
`minutes = (later − earlier)/60`.

| Column | Formula |
|---|---|
| `preparation_time_min` | picked_up − accepted |
| `delivery_time_min` | delivered − picked_up |
| `fulfillment_time_min` | delivered − order |
| `delivery_speed_kmph` | `distance_km / (delivery_time_min / 60)` (null if time is 0, never divide by zero) |
| `delay_min` | `max(0, delivered − expected_delivery)` → `greatest(0.0, …)` |
| `delivery_status` | `delay_min = 0 → ON_TIME`, else `DELAYED` |
| `delay_category` | 0 → ON_TIME · ≤15 → SLIGHT · ≤30 → MODERATE · >30 → SEVERE |

**Worked example – T080001:** accepted 08:56, picked up 09:12 → **16**; delivered 09:38 → delivery **26**;
ordered 08:54 → fulfillment **44**; speed = 4.0 ÷ (26/60) = **9.23 km/h**; expected 09:38 = delivered 09:38 →
delay **0** → ON_TIME.

We compute from **unrounded** values and round only when writing, so rounding error never changes a status
or a category.

### 12.5 Rounding
Spark's `round(x, 2)` is **half-up** (2.345 → 2.35). Python's built-in `round` is *banker's* on binary floats
and can disagree on exact halves. Our independent verifier therefore uses `Decimal` half-up to match Spark.
Numeric values stay numeric (`12.5`, not `"12.50"`).

### 12.6 `dropDuplicates` and `groupBy` (Job 1 & 2)
- `dropDuplicates([...all 14 fields])` keeps one row per distinct combination — an exact-duplicate filter.
- Job 2: `groupBy("restaurant_id").agg(count, avg, sum(when(DELAYED,1).otherwise(0)))` — "count delayed rows" is
  a conditional sum — then `delay_rate_pct = 100 × delayed / count`. The city table is the same with
  `groupBy("city")`.
- Job 2 reads **typed** columns (it knows the schema Job 1 wrote), and also dedupes, so republishing the
  Kafka topic by accident wouldn't change any average.

### 12.7 Writing one file
`df.write.json(path)` produces a **directory** containing `part-00000-….json`, `_SUCCESS` and hidden `.crc`
files — one part per Spark partition. The assignment wants a single plain file, so:
`coalesce(1)` (merge to one partition *without* a full shuffle) → write to a temp directory → move the lone
`part-*` file to the final name → delete the temp directory. `ignoreNullFields=false` keeps `null`s so every
JSON line has the same keys. `orderBy` before writing makes the output deterministic.

**Say it in the viva:** "Job 1 reads each line as text, rejects malformed JSON strictly, parses fields as
strings, then a CASE-WHEN chain assigns the first failing rule. Valid rows are de-duplicated and enriched with
time differences computed from epoch seconds. Job 2 groups by restaurant and by city with count, average and
a conditional sum."

---

## 13. Data-pipeline patterns we used

### 13.1 Landing zone
Between Kafka and Spark sits a plain file (`*_landed.jsonl`). Benefits: Spark gets a **fixed, inspectable,
re-runnable input**; you can look at the file, re-run Spark without touching Kafka, and attach a count to each
hop. It also decouples speed: Kafka consumption and Spark computation don't need to run in lock-step.

### 13.2 Idempotency
An operation is **idempotent** if running it twice equals running it once. Consumers overwrite their landed
file (atomic rename); Spark overwrites its outputs; dedupe makes duplicate input harmless. So **re-running any
stage is safe** — essential when a TA asks you to "run it again".

### 13.3 Reconciliation and independent verification
Track counts at every hop: 62 → 62 → 62 → **44** → 44 → 44. A number that doesn't match tells you where data
was lost or added. `common/verify_reference.py` goes further: it recomputes the results from the raw file with
plain Python (different code, different libraries) and compares every field — if two independent
implementations agree, the logic is very likely right. (It once exposed a real bug: Spark's lenient JSON reader
accepting `{…}}` — that's why Job 1 now validates strictly.)

---

## 14. Glossary

| Term | Meaning |
|---|---|
| **Broker** | one Kafka server process |
| **Topic / Partition** | named stream / one ordered log of it |
| **Offset** | sequential record number within a partition |
| **End offset** | offset the next record will get = number of records stored |
| **Replica / Leader / Follower** | copy of a partition / the one serving it / the others |
| **ISR** | in-sync replicas: those caught up with the leader |
| **Replication factor** | number of replicas per partition |
| **acks** | how many replicas must confirm a write |
| **min.insync.replicas** | minimum ISR size for `acks=all` writes to succeed |
| **Controller** | the node managing cluster metadata and leader elections |
| **KRaft / Raft** | Kafka's built-in metadata consensus (replaces ZooKeeper) |
| **Quorum** | the set of controller voters; a majority must be alive |
| **Cluster ID** | UUID every node's disk is formatted with |
| **listeners / advertised.listeners** | where a broker binds / where it tells clients to connect |
| **Bootstrap servers** | entry points a client uses to discover the cluster |
| **Producer / Consumer** | writes to / reads from topics |
| **Consumer group** | consumers sharing a topic's partitions; offsets committed |
| **Landing zone** | local files where consumed data is stored before processing |
| **Driver / Executor** | Spark's planner process / its parallel workers |
| **Transformation / Action** | lazy description / trigger that runs it |
| **Shuffle** | redistributing data between partitions (wide ops) |
| **UDF** | user-defined function (here: Python code called from Spark) |
| **Idempotent** | doing it twice = doing it once |
| **ZeroTier** | overlay network joining VMs into one virtual LAN |
