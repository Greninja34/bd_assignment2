# Big Data 2026 — Assignment 2: Distributed Food Ordering System (Team 32)

Kafka (KRaft, 2 brokers) + Spark batch pipeline across 4 VMs joined by ZeroTier.

**Start with [`00-START-HERE.md`](00-START-HERE.md)** — time estimate, who does what, order of work.

## Get the code onto your VM

The docs assume the folder lives at `~/assignment2`, so clone it **to that path**:
```bash
git clone git@github.com:Greninja34/bd_assignment2.git ~/assignment2      # SSH (if your VM has a GitHub key)
git clone https://github.com/Greninja34/bd_assignment2.git ~/assignment2  # HTTPS (asks for username + a personal access token)
```
The repo is private: you need to be added as a collaborator first. No GitHub login on the VM? Use
**Code → Download ZIP** on GitHub, unzip, and rename the folder to `~/assignment2`.

## Your tasks

| Member | VM | Owns | Read first |
|---|---|---|---|
| **Member 1** | VM1 | Producer 1 · Consumer 2 · Spark Job 2 → `restaurant_summary.csv`, `city_summary.csv` | `01-STEPS.md` Phases 4.1, 5 |
| **Member 2** | VM2 | ZeroTier network · KRaft controller + Broker 1 · create both topics | `01-STEPS.md` Phases 1, 2.1–2.2, 3 |
| **Member 3** | VM3 | Broker 2 · broker-failure test · independent verification | `01-STEPS.md` Phases 2.3, 6, 7 |
| **Member 4** | VM4 | Consumer 1 · Spark Job 1 → `order_metrics.jsonl` · Producer 2 | `01-STEPS.md` Phases 4.2–4.4 |

**Everyone, before the team call:** do [`03-SOLO-PRACTICE.md`](03-SOLO-PRACTICE.md) once on your own
machine (~1–1½ h) and skim [`04-CONCEPTS-GUIDE.md`](04-CONCEPTS-GUIDE.md). The viva can ask any of us
about any stage.

## Documents

| File | What it is |
|---|---|
| [`00-START-HERE.md`](00-START-HERE.md) | overview, time estimate, roles, open questions for the TA |
| [`01-STEPS.md`](01-STEPS.md) | the team run — every command, with what it does |
| [`02-EXPECTED-RESULTS.md`](02-EXPECTED-RESULTS.md) | the numbers you should get at every hop |
| [`03-SOLO-PRACTICE.md`](03-SOLO-PRACTICE.md) | run the whole pipeline alone on one machine |
| [`04-CONCEPTS-GUIDE.md`](04-CONCEPTS-GUIDE.md) | Kafka, KRaft, replication, Spark, pipeline patterns explained |
| [`05-CODE-WALKTHROUGH.md`](05-CODE-WALKTHROUGH.md) | the code block by block + viva drill |

## Layout

```
common/          file_to_kafka.py (producer)  kafka_to_file.py (consumer)  verify_reference.py
member1_vm1/     spark_job2.py   order.jsonl (our team's dataset)
member2_vm2/     broker1.properties
member3_vm3/     broker2.properties
member4_vm4/     spark_job1.py
practice/        solo-broker1.properties  solo-broker2.properties
```
