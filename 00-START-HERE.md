# Assignment 2 — Distributed Food Ordering System (Team 32)

**Kafka (KRaft, 2 brokers) + Spark batch, across 4 VMs joined by ZeroTier.**

The code is written and was tested end-to-end on your real team-32 dataset (a real 2-broker KRaft
cluster, all six pipeline stages, and the broker-failure test). There are **no wrapper scripts**: every
cluster and pipeline step is a command you type, and every one is explained, so each of you can say
exactly how it was run. What's left is to set up the four VMs, run it, and *understand* it.

| Read / use | What it is |
|---|---|
| **`00-START-HERE.md`** | this page: how long it takes, who does what, in what order |
| [`01-STEPS.md`](01-STEPS.md) | **the team run**: every command, phase by phase, with what it does |
| [`02-EXPECTED-RESULTS.md`](02-EXPECTED-RESULTS.md) | the numbers you should get at each hop + a short viva Q&A |
| [`03-SOLO-PRACTICE.md`](03-SOLO-PRACTICE.md) | **each of you** runs the whole pipeline alone on one machine first |
| [`04-CONCEPTS-GUIDE.md`](04-CONCEPTS-GUIDE.md) | Kafka, KRaft, replication, Spark, pipeline patterns — explained |
| [`05-CODE-WALKTHROUGH.md`](05-CODE-WALKTHROUGH.md) | the code, block by block, + a 40-question viva drill |
| `common/` | `file_to_kafka.py` (producer), `kafka_to_file.py` (consumer), `verify_reference.py` |
| `member1_vm1/` … `member4_vm4/` | `spark_job1.py`, `spark_job2.py`, `broker1/2.properties`, and your `order.jsonl` |
| `practice/` | the two broker config files for solo practice |

---

## 1. How long will this take? (honest estimate)

**Hours, not days** — provided Kafka, Spark and ZeroTier are already installed (they are) and nothing
exotic goes wrong with the VM network. I have **not** run this on your four real VMs, so treat the
numbers below as estimates from the work involved, not a promise.

### The team run, on one call (`01-STEPS.md`)

| Phase | Smooth | With normal hiccups | Notes |
|---|---|---|---|
| 0–1 Check installs, copy folder, ZeroTier join/authorise/ping | 15 min | 30–45 min | the usual time sink: device not authorised, VM network mode, firewall |
| 2 Bring up VM2 then VM3 (config, format, start, verify) | 15 min | 30 min | one wrong IP or leftover demo data dir = a retry |
| 3 Create topics | 5 min | 5 min | |
| 4 Stage 1 (publish → land → Spark 1 → publish) | 10 min | 20 min | |
| 5 Stage 2 (land → Spark 2) | 5 min | 10 min | |
| 6 Failure test + screenshots | 15 min | 25 min | |
| 7–8 Verification, screenshots, checklist | 10 min | 20 min | |
| **Total** | **≈ 75–90 min** | **≈ 2.5–3 h** | |

**Recommended: two shorter calls** — *Call 1* (about 1½ h): Phases 0–3, stop when the topics exist.
*Call 2* (about 1–1½ h): Phases 4–8. A break between them also gives time to fix whatever Call 1 turned up.

### What takes the real time: understanding, not typing
| Activity | Time |
|---|---|
| **Each member** does `03-SOLO-PRACTICE.md` alone, **before** the call | 1–1½ h each (about 20 min on a re-run) |
| Each member reads `04-CONCEPTS-GUIDE.md` (skim §1–§10 first) | 1–1½ h |
| Together: the viva drill in `05-CODE-WALKTHROUGH.md`, quizzing each other | 45–60 min |

**Suggested schedule:** solo practice + reading (separately) → Call 1 → Call 2 → viva drill together.
If you only have one evening, do the team run and skim the "Say it in the viva" boxes — but the solo
practice is what stops the viva from being scary, because it's what makes you able to explain the part
that isn't on your VM.

---

## 2. The pipeline in one picture

```
 VM1 (Member 1)                VM2 + VM3 (Members 2, 3)               VM4 (Member 4)
 ──────────────                ─────────────────────────              ──────────────
 order.jsonl
    │ Producer 1
    ▼                          ┌──────────────────────────┐
    └───────────────────────►  │  topic  orders           │ ───────► Consumer 1
                               │  2 partitions, RF 2      │              │
                               │                          │              ▼
                               │  Broker 1 (VM2, also     │         orders_landed.jsonl
                               │     KRaft controller)    │              │ Spark Job 1
                               │  Broker 2 (VM3)          │              ▼
 Consumer 2 ◄───────────────── │  topic  order_metrics    │ ◄─────── order_metrics.jsonl
    │                          │  2 partitions, RF 2      │          Producer 2
    ▼                          └──────────────────────────┘
 order_metrics_landed.jsonl
    │ Spark Job 2
    ▼
 restaurant_summary.csv + city_summary.csv
```
Two rules the assignment is strict about: **Spark never reads Kafka directly** (always via a local file)
and **VM4 → VM1 data goes through Kafka, never a file copy**.

---

## 3. Who does what

One member per VM. Members 1 and 4 own the data path; Members 2 and 3 own the Kafka cluster — so they also
own the things that *prove* it works (topic evidence, failure test, independent verification).

| | VM | Owns | Waits for |
|---|---|---|---|
| **Member 1** | VM1 | Producer 1 · Consumer 2 · **Spark Job 2** → the two CSVs | topics (M2); `order_metrics` published (M4) |
| **Member 2** | VM2 | ZeroTier network · **controller + Broker 1** · create the topics | everyone's ZeroTier IPs |
| **Member 3** | VM3 | **Broker 2** · **broker-failure test** · independent verification | VM2 up; both stages done |
| **Member 4** | VM4 | Consumer 1 · **Spark Job 1** → `order_metrics.jsonl` · Producer 2 | `orders` published (M1) |

> Everyone must be able to explain the **whole** pipeline — the viva can ask any member about any stage.
> That's what `03-SOLO-PRACTICE.md` is for.

### Member 1 — VM1 · "front door and final analytics"
- **Setup:** Spark 3.3.4 + Python 3.10 (check), `python3 -m venv ~/a2venv` + `pip install kafka-python`.
- **Do:** Phase 4.1 *Producer 1* (publish `order.jsonl` to `orders`) → Phase 5 *Consumer 2 + Spark Job 2*.
- **Hand over:** the two CSVs and `order_metrics_landed.jsonl` to Member 3.
- **Also:** review `spark_job1.py` against the cleaning rules in the assignment page §5.
- **Done when:** `order_metrics_landed.jsonl` = 44 lines; `restaurant_summary.csv` = 6 rows, `city_summary.csv` = 4 rows.

### Member 2 — VM2 · "network and cluster owner"
- **Setup:** Java 11 + Kafka 3.9.2 (check).
- **Do:** Phase 1 *create the ZeroTier network and authorise devices* → Phase 2.1–2.2 *cluster ID, configure,
  format and start the controller + broker 1* (**first!**) → Phase 3 *create `orders` and `order_metrics`*,
  screenshot `--describe`.
- **Also:** review Member 3's `broker2.properties`; review `spark_job2.py` against the analytics definitions (§6B).
- **Done when:** two brokers visible; both topics `PartitionCount: 2  ReplicationFactor: 2  Isr: 1,2`.

### Member 3 — VM3 · "second broker, failure test, truth-checker"
- **Setup:** Java 11 + Kafka 3.9.2 (check).
- **Do:** Phase 2.3 *configure, format (same cluster ID) and start broker 2* → Phase 6 *broker-failure test*
  with screenshots → Phase 7 *`verify_reference.py` on the collected outputs*.
- **Also:** review the producer/consumer against "no Kafka→Spark directly, no HDFS".
- **Done when:** `Isr` returns to `1,2` after the test, and `verify_reference.py` says **ALL CHECKS PASSED**.

### Member 4 — VM4 · "enrichment"
- **Setup:** Spark 3.3.4 + Python 3.10 (check), venv + `kafka-python`.
- **Do:** Phase 4.2 *Consumer 1* → 4.3 *Spark Job 1* → 4.4 *Producer 2*.
- **Hand over:** `order_metrics.jsonl` to Member 3.
- **Also:** review the cluster config (`broker1.properties`) against §3/§8 of the assignment page.
- **Done when:** `orders_landed.jsonl` = 62 lines, `order_metrics.jsonl` = 44, Producer 2 reports 44 published.

---

## 4. Order of work

```
 (separately)   everyone: 03-SOLO-PRACTICE + skim 04-CONCEPTS-GUIDE
 ──── CALL 1 ──────────────────────────────────────────────────────────────
 ALL    check installs, ZeroTier join / ping                 (Phases 0–1)
 M2     cluster ID, start VM2   ──►  M3 start VM3            (Phase 2)
 M2     create topics                                        (Phase 3)
 ──── CALL 2 ──────────────────────────────────────────────────────────────
 M1     Producer 1  ──►  M4 Consumer 1 → Spark 1 → Producer 2      (Phase 4)
 M1     Consumer 2 → Spark 2                                        (Phase 5)
 M3     failure test  →  verify_reference.py                        (Phases 6–7)
 ALL    screenshots + checklist                                     (Phase 8)
 (together)     viva drill from 05-CODE-WALKTHROUGH
```
Announce each hand-off in the group chat in one line: *"cluster up" → "topics created" → "orders published"
→ "order_metrics published" → "stage 2 done" → "failover done"*.

---

## 5. Things to confirm with the TA

The assignment page is ambiguous in a few places. I picked the most defensible reading for each (all are
one-line changes). I re-checked the live site on 13 Oct: the assignment text and your dataset had **not**
changed, so these are still open.

| # | Ambiguity | What the code does |
|---|---|---|
| 1 | Job 1 schema says `order amount` (space); everything else is snake_case, and Job 2 says `avg_order_amount` | **`order_amount`** — assumed typo (`OUT_COLS` in `spark_job1.py`) |
| 2 | Roles table calls VM3 "broker **and controller**", but the text says VM2 is the controller and the failure test stops VM3 with "controller handover out of scope" | **VM2 = broker + controller, VM3 = broker only** |
| 3 | "1–15 / 16–30 / >30 min" has gaps for fractional minutes | `0` ON_TIME · `(0,15]` SLIGHT · `(15,30]` MODERATE · `>30` SEVERE |
| 4 | "Rounded to 2 decimals" | numeric rounding (half-up): `12.5`, not `12.50` |
| 5 | "Exact duplicate" | all 14 input fields identical (in your data, same-`order_id` pairs are one good row + one corrupted copy, so it never matters) |
| 6 | Rules don't mention negative `delivery_fee`, delivered-before-picked-up, etc. | **not checked** ("do not add or change business rules") — none occur in your data |
| 7 | **Submission format not stated** | ask: live demo (like Assignment 1) or file upload? |

---

## 6. Your dataset in numbers

62 records → **14 rejected** → 48 valid → **4 exact duplicates** → **44 enriched orders**.

| Rejected because | Count |
|---|---|
| status ≠ DELIVERED (REFUNDED / CANCELLED / FAILED / PENDING) | 6 |
| missing required field | 4 |
| invalid timestamp (`not-a-timestamp`, `bad`) | 2 |
| distance_km ≤ 0 | 1 |
| order_amount ≤ 0 | 1 |
| *(exact duplicates, counted once)* | 4 |

Full expected files: [`02-EXPECTED-RESULTS.md`](02-EXPECTED-RESULTS.md).
