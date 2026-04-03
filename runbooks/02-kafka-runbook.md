# Runbook 02 — Apache Kafka Module (8 Hours · Apr 28 – May 1)

> **Goal by May 1:** `claim-events` topic (3 partitions, keyed by insurer), two independent consumer groups reading in parallel, `audit-log` with 7-day retention, idempotent producer configured.

---

## Day 4 · Apr 28 · 2 Hours — Architecture & Setup

### Theory Summary
- Kafka decouples producers (claim-service) from consumers (eligibility, fraud teams)
- Topic = named channel; Partition = ordered, immutable log; Offset = position in partition
- Consumer Group = set of consumers sharing work; each group gets ALL messages

### Lab Steps

#### 1. Verify Kafka is running
```bash
# Check broker is healthy
docker compose ps kafka

# List topics (should show the 4 pre-created topics)
docker exec md-kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

#### 2. Explore broker configuration
```bash
docker exec md-kafka cat /opt/bitnami/kafka/config/server.properties | grep -E "broker.id|log.dirs|listeners|num.partitions"
```

#### 3. Map MD India architecture to Kafka
| Kafka Role | MD India Component |
|-----------|-------------------|
| Producer | claim-service (submits claim events) |
| Topic | claim-events |
| Consumer Group | eligibility-consumers, fraud-consumers |
| Broker | Kafka container |
| Offset | position in audit trail |

---

## Day 5 · Apr 29 · 2 Hours — Topics, Partitions & Producers

### Lab Steps

#### 1. Create the claim-events topic (hands-on — it already exists, delete and recreate)
```bash
# Delete the pre-created topic
docker exec md-kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --delete --topic claim-events

# Recreate with 3 partitions
docker exec md-kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic claim-events \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000

# Verify
docker exec md-kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic claim-events
```

Expected output:
```
Topic: claim-events  PartitionCount: 3  ReplicationFactor: 1
  Partition: 0  Leader: 1001  Replicas: 1001  Isr: 1001
  Partition: 1  Leader: 1001  Replicas: 1001  Isr: 1001
  Partition: 2  Leader: 1001  Replicas: 1001  Isr: 1001
```

#### 2. Publish a claim event via CLI
```bash
docker exec -it md-kafka kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic claim-events \
  --property parse.key=true \
  --property key.separator=":"
```

Type these messages (key:value format — key = insurer):
```
StarHealth:{"claim_id":"C001","member_id":"M1001","insurer":"StarHealth","amount":85000,"status":"SUBMITTED"}
HDFCErgo:{"claim_id":"C002","member_id":"M1002","insurer":"HDFCErgo","amount":45000,"status":"SUBMITTED"}
StarHealth:{"claim_id":"C003","member_id":"M1003","insurer":"StarHealth","amount":120000,"status":"SUBMITTED"}
ICICILombard:{"claim_id":"C004","member_id":"M1001","insurer":"ICICILombard","amount":30000,"status":"SUBMITTED"}
HDFCErgo:{"claim_id":"C005","member_id":"M1002","insurer":"HDFCErgo","amount":75000,"status":"SUBMITTED"}
```

Press `Ctrl+C` to exit.

#### 3. Observe partition routing
```bash
# Consume with partition info to see which insurer went to which partition
docker exec md-kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic claim-events \
  --from-beginning \
  --property print.key=true \
  --property print.partition=true \
  --property print.offset=true
```

Note: All `StarHealth` claims should be in the same partition (key-based routing).

#### 4. Submit a claim via the API (uses real producer code)
```bash
curl -s -X POST http://localhost:8080/api/v1/claims \
  -H "Content-Type: application/json" \
  -d '{
    "member_id": "M1001",
    "hospital_id": "H5501-Apollo",
    "hospital_name": "Apollo",
    "insurer": "StarHealth",
    "amount": 85000,
    "city": "mumbai",
    "claim_type": "cashless",
    "diagnosis_code": "Z51.1"
  }' | python -m json.tool
```

Then verify it appeared in Kafka:
```bash
docker exec md-kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic claim-events \
  --from-beginning \
  --max-messages 1 \
  --property print.key=true
```

---

## Day 6 · Apr 30 · 2 Hours — Consumers, Groups & Retention

### Lab Steps

#### 1. Start the eligibility consumer group
```bash
# Terminal 1 — eligibility consumer
docker exec -it md-kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic claim-events \
  --group eligibility-consumers \
  --from-beginning \
  --property print.key=true
```

#### 2. Start the fraud consumer group (separate terminal)
```bash
# Terminal 2 — fraud consumer
docker exec -it md-kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic claim-events \
  --group fraud-consumers \
  --from-beginning \
  --property print.key=true
```

#### 3. Publish new claims and watch both groups receive them
```bash
# Terminal 3
curl -s -X POST http://localhost:8080/api/v1/claims \
  -H "Content-Type: application/json" \
  -d '{"member_id":"M1002","hospital_id":"H5502-Fortis","hospital_name":"Fortis","insurer":"HDFCErgo","amount":60000,"city":"mumbai","claim_type":"cashless","diagnosis_code":"I10"}' | python -m json.tool
```

Both Terminal 1 and Terminal 2 should show the new claim — **independent consumption**.

#### 4. Check consumer group offsets and lag
```bash
docker exec md-kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group eligibility-consumers
```

Output columns explained:
- `CURRENT-OFFSET` = last consumed message position
- `LOG-END-OFFSET` = last produced message position
- `LAG` = messages not yet consumed (should be 0 if caught up)

#### 5. Configure audit-log retention
```bash
# Verify current retention
docker exec md-kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic audit-log

# Set to 7 days (604800000 ms)
docker exec md-kafka kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --alter \
  --entity-type topics \
  --entity-name audit-log \
  --add-config retention.ms=604800000

# Verify change
docker exec md-kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic audit-log
```

---

## Day 7 · May 1 · 2 Hours — Delivery Semantics & Replay

### Theory Summary
| Semantic | Config | MD India Use Case |
|---------|--------|------------------|
| At Most Once | acks=0 | Acceptable for non-critical analytics |
| At Least Once | acks=1, no idempotence | Default — possible duplicate processing |
| Exactly Once | acks=all, enable.idempotence=true | Fraud detection — duplicate alerts cause false holds |

### Lab Steps

#### 1. Simulate At Least Once (duplicate processing)
```bash
# Start a consumer, process 3 messages, then kill it with Ctrl+C
docker exec -it md-kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic claim-events \
  --group duplicate-demo \
  --max-messages 3

# Do NOT let it commit the offset — kill it after 2 messages with Ctrl+C
# Restart — it will reprocess messages from the last committed offset
docker exec -it md-kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic claim-events \
  --group duplicate-demo \
  --max-messages 5
```

Observe: messages already processed may appear again.

#### 2. Idempotent producer (already configured in claim-service)
```bash
# The producer in claim-service/app/producer.py already sets:
#   acks=all
#   enable.idempotence=True
# Submit a claim and verify it appears exactly once in audit-log:

docker exec -it md-kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic audit-log \
  --from-beginning \
  --property print.key=true
```

#### 3. Replay — run new rule against historical claims
```bash
docker exec -it md-kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic claim-events \
  --from-beginning \
  --group replay-audit \
  --property print.key=true \
  --property print.offset=true
```

#### 4. Compliance audit — count claims per insurer
```bash
docker exec md-kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic claim-events \
  --from-beginning \
  --group audit-count-$(date +%s) \
  --timeout-ms 5000 \
  --property print.key=true 2>/dev/null | \
  grep -o '"insurer":"[^"]*"' | sort | uniq -c | sort -rn
```

#### 5. View all consumer groups
```bash
docker exec md-kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --list
```

---

## Kafka CLI Quick Reference

```bash
# Topics
kafka-topics.sh --bootstrap-server localhost:9092 --list
kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic <name>
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic <name> --partitions 3 --replication-factor 1
kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic <name>

# Produce
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic <name>
kafka-console-producer.sh ... --property parse.key=true --property key.separator=":"

# Consume
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic <name> --from-beginning
kafka-console-consumer.sh ... --group <group-id>
kafka-console-consumer.sh ... --max-messages 10

# Consumer groups
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group <name>
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --reset-offsets --group <name> --topic <name> --to-earliest --execute
```

> **Docker prefix:** Prepend all commands with `docker exec md-kafka ` when running from your host.

---

## Module Checklist

- [ ] `claim-events` topic created with 3 partitions
- [ ] Claims published with insurer as key — verified partition routing
- [ ] `eligibility-consumers` and `fraud-consumers` both reading independently
- [ ] Consumer lag checked with `kafka-consumer-groups.sh`
- [ ] `audit-log` retention set to 7 days
- [ ] Replay from `--from-beginning` demonstrated
- [ ] Idempotent producer understood (`acks=all` + `enable.idempotence=true`)
