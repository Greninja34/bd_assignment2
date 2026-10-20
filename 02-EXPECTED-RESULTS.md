# 02 — Expected results for Team 32, and viva Q&A

These are the values the pipeline produced on the real team-32 `order.jsonl` and that an
independent pure-Python re-computation (`common/verify_reference.py`) agreed with row by row.
If your VMs print something different, that's a real discrepancy — not a dataset difference.

---

## 1. Counts at every hop

| Stage | File / topic | Records |
|---|---|---|
| Raw dataset | `order.jsonl` | **62** |
| Producer 1 → | topic `orders` | **62** (all lines, untouched — including the bad ones) |
| Consumer 1 → | `orders_landed.jsonl` | **62** |
| Spark Job 1 → | `order_metrics.jsonl` | **44** |
| Producer 2 → | topic `order_metrics` | **44** |
| Consumer 2 → | `order_metrics_landed.jsonl` | **44** |
| Spark Job 2 → | `restaurant_summary.csv` / `city_summary.csv` | **6** rows / **4** rows |

### Where the 62 → 44 goes (Spark Job 1's cleaning summary)

| Rule | Count | Input lines (1-based) |
|---|---|---|
| status ≠ DELIVERED | 6 | 16 REFUNDED · 17 CANCELLED · 18 FAILED · 21 CANCELLED · 32 CANCELLED · 42 PENDING |
| missing required field | 4 | 14 (`{"order_id":"BROKEN",…}`) · 27 · 40 (`record_type: invalid_order_record`) · 28 (no `city`) |
| invalid timestamp | 2 | 24 (`delivered_timestamp: "not-a-timestamp"`) · 57 (`accepted_timestamp: "bad"`) |
| distance_km ≤ 0 | 1 | 1 (`-2`) |
| order_amount ≤ 0 | 1 | 56 (`0`) |
| **= rejected** | **14** | |
| valid | 48 | |
| exact duplicates removed | 4 | lines 20, 44, 48, 61 (repeats of 13, 39, 19, 52) |
| **= enriched orders** | **44** | |

Nice viva detail: five `order_id`s (T080001–T080005) appear twice with **one good and one
corrupted copy** (negative distance, missing city, bad timestamp, zero amount, bad timestamp).
They are *not* exact duplicates; the corrupted twin is simply dropped by the cleaning rules,
so each of those orders appears once in the output.

No line in this file is syntactically broken JSON — the "dirty" rows are valid JSON with bad
content. The job still treats real malformed JSON (truncated, trailing garbage, non-objects)
as rejected; that was tested separately.

### Delivery outcome of the 44 orders

| delay_category | orders |
|---|---|
| ON_TIME | 11 |
| SLIGHT_DELAY (1–15 min) | 11 |
| MODERATE_DELAY (16–30 min) | 18 |
| SEVERE_DELAY (>30 min) | 4 |
| → `delivery_status` = DELAYED | 33 (= 11 + 18 + 4) |

---

## 2. `restaurant_summary.csv`

```csv
restaurant_id,order_count,avg_order_amount,avg_preparation_time_min,avg_delivery_time_min,delayed_order_count,delay_rate_pct
R01,8,686.25,13.0,26.0,4,50.0
R02,8,570.75,13.25,31.0,8,100.0
R03,7,665.14,13.14,36.0,4,57.14
R04,7,640.71,13.0,41.0,7,100.0
R05,7,494.86,12.86,46.0,3,42.86
R06,7,591.86,12.71,21.0,7,100.0
```

## 3. `city_summary.csv`

```csv
city,order_count,avg_order_amount,avg_delivery_time_min,delayed_order_count,delay_rate_pct
Bengaluru,16,628.5,28.5,12,75.0
Hyderabad,7,591.86,21.0,7,100.0
Mumbai,14,652.93,38.5,11,78.57
Pune,7,494.86,46.0,3,42.86
```
(order counts: 8+8+7+7+7+7 = 16+7+14+7 = **44**; delayed: 4+8+4+7+3+7 = 12+7+11+3 = **33**.)

## 4. Four sample lines of `order_metrics.jsonl` (one per delay category)

```json
{"order_id":"T080001","customer_id":"C08001","restaurant_id":"R01","delivery_partner_id":"D01","city":"Bengaluru","order_amount":668.0,"delivery_fee":28.0,"distance_km":4.0,"preparation_time_min":16.0,"delivery_time_min":26.0,"fulfillment_time_min":44.0,"delivery_speed_kmph":9.23,"delay_min":0.0,"delivery_status":"ON_TIME","delay_category":"ON_TIME"}
{"order_id":"T080002","customer_id":"C08002","restaurant_id":"R02","delivery_partner_id":"D02","city":"Bengaluru","order_amount":765.0,"delivery_fee":39.0,"distance_km":4.7,"preparation_time_min":8.0,"delivery_time_min":31.0,"fulfillment_time_min":41.0,"delivery_speed_kmph":9.1,"delay_min":8.0,"delivery_status":"DELAYED","delay_category":"SLIGHT_DELAY"}
{"order_id":"T080003","customer_id":"C08003","restaurant_id":"R03","delivery_partner_id":"D03","city":"Mumbai","order_amount":862.0,"delivery_fee":50.0,"distance_km":5.4,"preparation_time_min":11.0,"delivery_time_min":36.0,"fulfillment_time_min":49.0,"delivery_speed_kmph":9.0,"delay_min":22.0,"delivery_status":"DELAYED","delay_category":"MODERATE_DELAY"}
{"order_id":"T080004","customer_id":"C08004","restaurant_id":"R04","delivery_partner_id":"D04","city":"Mumbai","order_amount":959.0,"delivery_fee":61.0,"distance_km":6.1,"preparation_time_min":14.0,"delivery_time_min":41.0,"fulfillment_time_min":57.0,"delivery_speed_kmph":8.93,"delay_min":40.0,"delivery_status":"DELAYED","delay_category":"SEVERE_DELAY"}
```
Hand-check T080001 (good habit for the viva): accepted 08:56 → picked up 09:12 = **16**;
picked up 09:12 → delivered 09:38 = **26**; ordered 08:54 → delivered 09:38 = **44**;
speed = 4.0 km ÷ (26/60 h) = **9.23**; delivered 09:38 = expected 09:38 → delay **0**.

Output order: rows are sorted by `order_id`, CSVs by key. Row **order** in the `*_landed.jsonl`
files is *not* fixed (Kafka interleaves two partitions), which is fine — compare as sets.

---

## 5. Viva — questions to be able to answer

**Why 2 partitions and replication factor 2?**
Partitions give parallelism (2 consumers could split a topic); replication gives a copy of each
partition on the other broker. With only 2 brokers, RF 2 is the maximum, and it's what lets one
broker die without losing data or availability.

**What is the ISR and why did it shrink to `1` during the test?**
In-Sync Replicas = replicas that are fully caught up with the leader. When VM3 died its replicas
dropped out of the ISR; the surviving replica on VM2 stayed in it and became leader of every partition.

**Why can writes continue with one broker down?**
`min.insync.replicas=1`: a write with `acks=all` needs all *current* ISR members to confirm, and
the ISR is now just broker 1. (With `min.insync.replicas=2` writes would be refused.)

**Why did the assignment say stop VM3 and not VM2?**
VM2 is also the only KRaft controller. Losing it means no leader election / metadata changes
until a controller returns; surviving that needs 3 controllers, which is out of scope here.

**Why not let Spark read Kafka directly?**
Assignment rule — and it also gives a reproducible, inspectable landed file: Spark Job 1/2 are
plain *batch* jobs over a fixed input, rerunnable without touching the broker.

**Why does Producer 1 send the raw lines instead of parsing them?**
Cleaning is Spark Job 1's responsibility and the malformed records must survive the Kafka hop to be
counted/dropped there. A producer that "helpfully" fixes or skips records would hide that.

**Why is the consumer a manual-assign reader, not a consumer group?**
No committed offsets ⇒ a re-run always re-lands the entire topic from offset 0 up to the end
offset captured at start, so landed files are deterministic and complete (the consumer reports
`landed N/N`). A group consumer would return nothing on the second run.

**How are duplicates handled?**
Both Spark jobs `dropDuplicates` on the full record. Job 1 dedupes raw input; Job 2 dedupes again,
which makes it correct even if Producer 2 had been run twice.

**What is `advertised.listeners` and why must it be the ZeroTier IP?**
It's the address the broker tells clients to use *after* bootstrap. If it said `localhost`, VM1 and
VM4 would connect to VM2, get told "talk to localhost", and fail.

**Why does Spark use `local[*]` and no HDFS?**
Assignment rule (local filesystem as landing area); the data is tiny. Spark's value here is the
declarative batch transformation, not cluster scale.

**Which exact rows were dropped and why?** → section 1's table.
