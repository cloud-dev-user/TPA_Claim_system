# HealthOne TPA Claims Processing Pipeline — Project Guide

> A complete reference for developers joining this project.  
> Covers: why it exists, how it works, every component, data flows, and how to develop against it.

---

## Table of Contents

1. [The Problem](#1-the-problem)
2. [The Solution](#2-the-solution)
3. [Architecture Overview](#3-architecture-overview)
4. [Component Deep Dive](#4-component-deep-dive)
5. [End-to-End Claim Journey](#5-end-to-end-claim-journey)
6. [Data Models](#6-data-models)
7. [Redis Data Design](#7-redis-data-design)
8. [Kafka Topic Design](#8-kafka-topic-design)
9. [APISIX Gateway Design](#9-apisix-gateway-design)
10. [Project Structure](#10-project-structure)
11. [Local Development Setup](#11-local-development-setup)
12. [Running Tests](#12-running-tests)
13. [All UIs at a Glance](#13-all-uis-at-a-glance)
14. [Environment Variables Reference](#14-environment-variables-reference)
15. [Common Developer Tasks](#15-common-developer-tasks)
16. [Troubleshooting](#16-troubleshooting)
17. [Key Design Decisions](#17-key-design-decisions)

---

## 1. The Problem

HealthOne TPA is a **Third Party Administrator (TPA)** — the company that sits between hospitals, patients, and insurance companies. When a patient walks into a hospital for cashless treatment, the hospital submits a claim to HealthOne TPA. HealthOne TPA then:

1. Checks if the hospital is empanelled (authorised for cashless settlement)
2. Verifies the patient's insurance policy has sufficient remaining limit
3. Screens the claim for fraud signals
4. Approves or rejects, then notifies the insurer

**Before this system, the process had real problems:**

| Problem | Impact |
|---------|--------|
| Each claim submission hit the core insurance database directly | Database overloaded during peak hours (year-end, post-IPD surge) |
| Eligibility check and fraud check ran sequentially | Slow — patients waited 10–20 minutes for cashless approval |
| No audit trail was retained beyond 30 days | Compliance failures during IRDAI audits |
| Hospital systems could flood the claims API with retries | Brought down the service for all hospitals |
| A single buggy fraud rule could block all claims | No way to replay historical claims against a fixed rule |

---

## 2. The Solution

A **decoupled, event-driven claims pipeline** built on four technologies:

```
Hospital → APISIX Gateway → Claim Service → Kafka → Eligibility Service
                                                  ↘→ Fraud Service
                                                  ↘→ Audit Log
                                    Redis ←────────────────────┘
                                   (cache)
```

| Technology | Role in the solution |
|-----------|---------------------|
| **GitLab CI/CD** | All service code is version-controlled; every change is tested before deployment |
| **Apache Kafka** | Decouples claim submission from processing — submission is instant, checks run async in parallel |
| **Redis** | Caches member policy data so the core DB is hit only on cache miss; stores fraud scores |
| **Apache APISIX** | Single secure entry point — authentication, rate limiting, load balancing, all without touching application code |

**Result:**
- Claim submission: **< 200ms** (just validates + publishes to Kafka)
- Eligibility + fraud checks run **in parallel** instead of sequentially
- **7 days** of events retained in Kafka for compliance replay
- **Per-hospital rate limits** prevent any single hospital from flooding the API
- Fraud counters **survive Redis restarts** (AOF persistence)

---

## 3. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        HealthOne TPA Claims Pipeline                      │
│                                                                      │
│  ┌──────────┐    ┌──────────────┐    ┌──────────────────────────┐   │
│  │ Hospital │───▶│ Apache APISIX│───▶│     Claim Service        │   │
│  │  System  │    │  (API GW)    │    │  (Spring Boot · 2 inst.) │   │
│  └──────────┘    │              │    └────────────┬─────────────┘   │
│                  │ · key-auth   │                 │ publish          │
│  ┌──────────┐    │ · rate limit │                 ▼                  │
│  │ Insurer  │───▶│ · load bal.  │    ┌──────────────────────────┐   │
│  │  Portal  │    └──────────────┘    │       Apache Kafka        │   │
│  └──────────┘                        │                            │   │
│                                      │  ┌─────────────────────┐  │   │
│  ┌────────────────────────────────┐  │  │ claim-events        │  │   │
│  │           Redis                │  │  │ (3 partitions,      │  │   │
│  │                                │  │  │  keyed by insurer)  │  │   │
│  │  member:M1001:policy (Hash)    │  │  └──────────┬──────────┘  │   │
│  │  empanelled:mumbai   (Set)     │  │             │              │   │
│  │  fraud:hospital:scores (ZSet)  │◀─┤    ┌────────┴────────┐    │   │
│  │  claim:C001:status   (String)  │  │    │                 │    │   │
│  └────────────────────────────────┘  │    ▼                 ▼    │   │
│                                      │ ┌──────────┐  ┌─────────┐ │   │
│                                      │ │Eligibility│  │ Fraud   │ │   │
│                                      │ │ Service   │  │ Service │ │   │
│                                      │ │(consumer  │  │(consumer│ │   │
│                                      │ │ group)    │  │ group)  │ │   │
│                                      │ └──────────┘  └─────────┘ │   │
│                                      │        │            │       │   │
│                                      │        ▼            ▼       │   │
│                                      │  ┌──────────────────────┐  │   │
│                                      │  │      audit-log        │  │   │
│                                      │  │  (7-day retention)    │  │   │
│                                      │  └──────────────────────┘  │   │
│                                      └──────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 4. Component Deep Dive

### Claim Service
**What it does:** Receives HTTP POST requests from hospitals (via APISIX), validates the claim, stores initial status in Redis, and publishes a Kafka event.

**Key files:**
- [claim-service-java/src/main/java/com/healthonetpa/claim/model/ClaimSubmit.java](claim-service-java/src/main/java/com/healthonetpa/claim/model/ClaimSubmit.java) — `ClaimSubmit` request model
- [claim-service-java/src/main/java/com/healthonetpa/claim/service/ClaimValidator.java](claim-service-java/src/main/java/com/healthonetpa/claim/service/ClaimValidator.java) — Business rule validation
- [claim-service-java/src/main/java/com/healthonetpa/claim/kafka/ClaimEventProducer.java](claim-service-java/src/main/java/com/healthonetpa/claim/kafka/ClaimEventProducer.java) — Idempotent Kafka producer
- [claim-service-java/src/main/java/com/healthonetpa/claim/controller/ClaimController.java](claim-service-java/src/main/java/com/healthonetpa/claim/controller/ClaimController.java) — REST endpoints (`POST /api/v1/claims`, `GET /api/v1/claims/{id}`, `/health`)

**Why two instances?** To demonstrate APISIX load balancing. Both connect to the same Kafka and Redis.

**Idempotent producer:** `acks=all` + `enable.idempotence=True` — even if the producer retries due to a network glitch, Kafka guarantees the message is written exactly once.

---

### Eligibility Service
**What it does:** Kafka consumer that reads `claim-events`, checks Redis for member policy, verifies remaining limit, deducts the claim amount, and publishes to `eligibility-results` and `audit-log`.

**Key files:**
- [eligibility-service-java/src/main/java/com/healthonetpa/eligibility/service/EligibilityCheckService.java](eligibility-service-java/src/main/java/com/healthonetpa/eligibility/service/EligibilityCheckService.java) — Business logic (empanelment + limit check)
- [eligibility-service-java/src/main/java/com/healthonetpa/eligibility/kafka/EligibilityConsumer.java](eligibility-service-java/src/main/java/com/healthonetpa/eligibility/kafka/EligibilityConsumer.java) — Kafka consumer with DLQ and graceful shutdown

**Checks performed (in order):**
1. Is the hospital in `empanelled:{city}` Redis Set? (cashless only)
2. Does `member:{id}:policy` Hash exist in Redis?
3. Is `amount ≤ (limit - used)`?
4. If eligible: `HINCRBYFLOAT used {amount}` + reset TTL

---

### Fraud Service
**What it does:** Independent Kafka consumer (different consumer group from eligibility) that reads the same `claim-events` topic and applies fraud heuristics using Redis.

**Key files:**
- [fraud-service-java/src/main/java/com/healthonetpa/fraud/service/FraudRuleService.java](fraud-service-java/src/main/java/com/healthonetpa/fraud/service/FraudRuleService.java) — Fraud scoring rules
- [fraud-service-java/src/main/java/com/healthonetpa/fraud/kafka/FraudConsumer.java](fraud-service-java/src/main/java/com/healthonetpa/fraud/kafka/FraudConsumer.java) — Kafka consumer with DLQ

**Fraud rules applied:**

| Rule | Score added | Trigger |
|------|------------|---------|
| High-amount claim | +20 | Amount ≥ ₹5,00,000 |
| Round-number amount | +5 | Amount is exact multiple of ₹1,00,000 |
| Daily member excess | +30 | Member submits > 5 claims in one day |
| Hospital base score | +5 (minimum) | Every claim updates hospital risk score |

**Flagged when:** total score ≥ 50 → publishes to `fraud-alerts` topic

---

### APISIX Gateway
**What it does:** Acts as the front door — every request from hospitals must pass through APISIX before reaching the claim service.

**Configured policies:**
- `key-auth` plugin — each hospital has a unique API key
- `limit-count` plugin — Apollo: 200 req/min; others: 50 req/min; generic: 20 req/min
- Round-robin load balancing across `claim-service-1:8080` and `claim-service-2:8081`
- Active health checks — stops routing to unhealthy instances within 5 seconds

**Config files:**
- [apisix/config.yaml](apisix/config.yaml) — APISIX runtime configuration
- [apisix/setup-apisix.sh](apisix/setup-apisix.sh) — Creates all routes, upstreams, consumers via Admin API

---

### Redis
**What it stores:**

| Key Pattern | Type | Purpose | TTL |
|------------|------|---------|-----|
| `member:{id}:policy` | Hash | Member policy (limit, used, plan, insurer) | 15 min |
| `empanelled:{city}` | Set | Empanelled hospital names for O(1) lookup | None |
| `fraud:hospital:scores` | Sorted Set | Cumulative risk score per hospital | None |
| `claim:{id}:status` | String | Current processing status | 24h |
| `fraud:member:{id}:claims:{date}` | String (counter) | Daily claim count per member | 24h |

---

### Kafka Topics

| Topic | Partitions | Retention | Key | Publisher | Consumers |
|-------|-----------|-----------|-----|-----------|----------|
| `claim-events` | 3 | 7 days | insurer code | claim-service | eligibility-consumers, fraud-consumers |
| `eligibility-results` | 3 | 1 day | claim_id | eligibility-service | (future: notification service) |
| `fraud-alerts` | 3 | 7 days | claim_id | fraud-service | (future: ops dashboard) |
| `audit-log` | 1 | 7 days | claim_id | eligibility + fraud services | (future: compliance exporter) |

**Why partition by insurer?**  
Kafka guarantees ordering within a partition. Partitioning by insurer means all claims for StarHealth always go to the same partition → eligibility/fraud services process StarHealth claims in submission order. This matters for limit tracking (prevents race conditions).

---

## 5. End-to-End Claim Journey

```
Step 1 — Hospital submits claim
  Hospital POST /api/v1/claims
    → APISIX checks: valid API key? ✓
    → APISIX checks: under rate limit? ✓
    → APISIX routes to claim-service-1 (round-robin)

Step 2 — Claim Service validates
  ClaimValidator checks:
    · member_id starts with 'M', ≥ 5 chars
    · amount between ₹100 and ₹1,00,00,000
    · claim_type is 'cashless' or 'reimbursement'
    · hospital_id, insurer, city, diagnosis_code all present
  → Saves claim:C001:status = "SUBMITTED" in Redis (24h TTL)
  → Publishes to Kafka claim-events (key = "StarHealth")
  → Returns HTTP 202 to hospital with claim_id

Step 3 — Eligibility Service (async, parallel with Step 4)
  Reads claim from claim-events (eligibility-consumers group)
  SISMEMBER empanelled:mumbai Apollo → 1 (empanelled ✓)
  HGETALL member:M1001:policy → limit=1000000, used=230000
  85000 ≤ (1000000 - 230000) = 770000 ✓
  HINCRBYFLOAT member:M1001:policy used 85000 → 315000
  EXPIRE member:M1001:policy 900
  Publishes to eligibility-results: { decision: "ELIGIBLE" }
  Publishes to audit-log: { event: "eligibility_decision", ... }

Step 4 — Fraud Service (async, parallel with Step 3)
  Reads same claim from claim-events (fraud-consumers group)
  amount=85000 < 500000 → no high-amount flag
  85000 % 100000 ≠ 0 → no round-number flag
  INCR fraud:member:M1001:claims:2026-04-24 → 1 (under limit)
  ZINCRBY fraud:hospital:scores 5 "H5501-Apollo" → 10
  risk_score = 0, is_flagged = false
  Publishes to audit-log: { event: "fraud_assessment", verdict: "CLEAR" }

Step 5 — Audit trail retained
  audit-log topic holds all decisions for 7 days
  Compliance team can replay: kafka-console-consumer --from-beginning
```

---

## 6. Data Models

### ClaimSubmit (the event that travels through the pipeline)

```json
{
  "claimId":        "C3F9A1B2",
  "memberId":       "M1001",
  "hospitalId":     "H5501-Apollo",
  "hospitalName":   "Apollo",
  "insurer":        "StarHealth",
  "amount":         85000.0,
  "city":           "mumbai",
  "claimType":      "cashless",
  "diagnosisCode":  "Z51.1",
  "submittedAt":    "2026-04-24T10:30:00Z",
  "status":         "SUBMITTED",
  "admissionDate":  "2026-04-20",
  "dischargeDate":  "2026-04-24",
  "preAuthNumber":  null,
  "remarks":        null
}
```

### ClaimStatus lifecycle
```
SUBMITTED → VALIDATING → ELIGIBLE → APPROVED
                       ↘ INELIGIBLE → REJECTED
                       ↘ FRAUD_REVIEW
```

---

## 7. Redis Data Design

### Member Policy Hash
```
HSET member:M1001:policy
  name     "Anita Sharma"
  plan     "Gold Family Floater"
  limit    1000000          ← total coverage amount in ₹
  used     230000           ← amount used so far this policy year
  renewal  "2027-03-31"
  insurer  "StarHealth"
EXPIRE member:M1001:policy 900   ← 15-minute cache
```

**On cache miss:** The service re-fetches from the core insurance DB and re-caches.  
**On claim approval:** `HINCRBYFLOAT used {claim_amount}` — atomic, no race condition.

### Empanelled Hospital Set
```
SADD empanelled:mumbai Apollo Fortis Kokilaben Max Lilavati
SISMEMBER empanelled:mumbai Apollo      → 1  (empanelled)
SISMEMBER empanelled:mumbai UnknownClinic → 0 (not empanelled → auto-reject cashless)
```

### Fraud Risk Sorted Set
```
ZADD fraud:hospital:scores 5 "H5501-Apollo"
ZINCRBY fraud:hospital:scores 10 "H5501-Apollo"  ← incremented on each claim
ZREVRANGE fraud:hospital:scores 0 4 WITHSCORES   ← top 5 riskiest hospitals
```

### Daily Fraud Counter
```
INCR fraud:member:M1001:claims:2026-04-24   ← increments on each claim
EXPIRE fraud:member:M1001:claims:2026-04-24 86400  ← auto-resets next day
```

---

## 8. Kafka Topic Design

### Why key by insurer on `claim-events`?

Kafka guarantees **ordering within a partition**. By using insurer as the key:
- All StarHealth claims → Partition 0 (always)
- All HDFCErgo claims → Partition 1 (always)
- All ICICILombard claims → Partition 2 (always)

This means the eligibility service processes each insurer's claims in the order they were submitted — critical for accurate limit tracking.

### Consumer Groups — why two?

`eligibility-consumers` and `fraud-consumers` are **separate consumer groups**. Each group gets its own independent copy of every message. They progress at their own pace — if the fraud service is slow, it doesn't block eligibility. Both read the same `claim-events` topic independently.

### Exactly-Once on the producer side

```yaml
# claim-service-java/src/main/resources/application.yml
spring.kafka.producer:
  acks: all                      # wait for all in-sync replicas
  retries: 5
  properties:
    enable.idempotence: true     # broker deduplicates retried messages
    compression.type: lz4
```

Even if the network drops mid-publish and the producer retries, Kafka won't write the same message twice.

---

## 9. APISIX Gateway Design

### Request flow
```
Hospital request
  → Route match (URI + method)
    → key-auth plugin  (is apikey header valid?)
      → limit-count plugin  (is this consumer under their rate limit?)
        → Upstream (round-robin to claim-service-1 or claim-service-2)
          → Response back to hospital
```

### API Keys (for training)

| Hospital | API Key | Rate Limit |
|---------|---------|-----------|
| Apollo | `APOLLO-KEY-2026` | 200 req/min |
| Fortis | `FORTIS-KEY-2026` | 50 req/min |
| Max | `MAX-KEY-2026` | 50 req/min |
| Kokilaben | `KOKI-KEY-2026` | 50 req/min |
| Non-network | `GENERIC-KEY-2026` | 20 req/min |

### 429 Response shape
```json
{
  "error": "Rate limit exceeded",
  "retry_after": 60,
  "hospital": "hospital-generic"
}
```

---

## 10. Project Structure

```
Training_code/
│
├── docker-compose.yml              ← Full local stack definition
├── Makefile                        ← Developer shortcuts (make up, make test-all, etc.)
│
├── claim-service-java/             ← Spring Boot 3.2 · Java 17
│   ├── src/main/java/com/healthonetpa/claim/
│   │   ├── controller/             ← REST endpoints (POST/GET /api/v1/claims, /health)
│   │   ├── kafka/                  ← Idempotent Kafka producer
│   │   ├── model/                  ← ClaimSubmit, ClaimEntity, ClaimResponse, ClaimStatus
│   │   ├── repository/             ← JPA repository → PostgreSQL
│   │   └── service/                ← ClaimValidator business rules
│   ├── src/main/resources/
│   │   └── application.yml         ← Config (Kafka, Redis, DB, Actuator, Swagger)
│   ├── Dockerfile                  ← Multi-stage build (Maven → JRE Alpine)
│   └── pom.xml
│
├── eligibility-service-java/       ← Spring Boot 3.2 · Java 17
│   ├── src/main/java/com/healthonetpa/eligibility/
│   │   ├── kafka/                  ← Kafka consumer + DLQ error handler
│   │   ├── model/                  ← EligibilityResult
│   │   └── service/                ← EligibilityCheckService (empanelment + limit check)
│   ├── src/main/resources/application.yml
│   ├── Dockerfile
│   └── pom.xml
│
├── fraud-service-java/             ← Spring Boot 3.2 · Java 17
│   ├── src/main/java/com/healthonetpa/fraud/
│   │   ├── kafka/                  ← Kafka consumer + DLQ error handler
│   │   ├── model/                  ← FraudAssessment
│   │   └── service/                ← FraudRuleService (scoring rules)
│   ├── src/main/resources/application.yml
│   ├── Dockerfile
│   └── pom.xml
│
├── apisix/
│   ├── config.yaml                 ← APISIX runtime config
│   ├── dashboard.yaml              ← APISIX Dashboard config
│   └── setup-apisix.sh             ← Configures routes/consumers via Admin API
│
├── certs/
│   ├── generate-certs.sh           ← Generates self-signed TLS cert (SAN: localhost)
│   └── server.crt / server.key     ← Generated locally — NOT committed (in .gitignore)
│
├── prometheus/prometheus.yml       ← Scrape config for all 4 services
├── grafana/provisioning/           ← Auto-provisioned Prometheus datasource
│
└── runbooks/
    ├── 00-prerequisites.md         ← Setup guide (start here)
    ├── 01-gitlab-runbook.md        ← GitLab module lab guide
    ├── 02-kafka-runbook.md         ← Kafka module lab guide
    ├── 03-redis-runbook.md         ← Redis module lab guide
    ├── WINDOWS_SETUP_RUNBOOK.md    ← Step-by-step Windows setup guide
    └── TROUBLESHOOTING_GUIDE.md    ← All known issues & resolutions
```

---

## 11. Local Development Setup

### Prerequisites
- Docker Desktop ≥ 4.25 (with WSL 2 on Windows)
- Java 17 JDK (for running tests locally without Docker)
- Maven 3.9+ (for building and testing)
- Git 2.x
- curl

### Start the full stack
```bash
# First time — builds images and starts everything
docker compose up -d --build

# Check all containers are healthy
docker compose ps

# One-time APISIX configuration
bash apisix/setup-apisix.sh
```

### Verify everything works
```bash
# Kafka: topics should be listed
docker exec healthone-kafka kafka-topics.sh --bootstrap-server localhost:9092 --list

# Redis: should return PONG
docker exec healthone-redis redis-cli PING

# Claim Service: should return {"status":"ok"}
curl http://localhost:8080/health

# APISIX: should return HTTP 200
curl -s -o /dev/null -w "%{http_code}" \
  http://localhost:9180/apisix/admin/routes \
  -H "X-API-KEY: edd1c9f034335f136f87ad84b625c8f1"

# End-to-end: submit a real claim
curl -s -X POST http://localhost:9080/api/v1/claims \
  -H "apikey: APOLLO-KEY-2026" \
  -H "Content-Type: application/json" \
  -d '{
    "member_id":"M1001", "hospital_id":"H5501-Apollo",
    "hospital_name":"Apollo", "insurer":"StarHealth",
    "amount":85000, "city":"mumbai",
    "claim_type":"cashless", "diagnosis_code":"Z51.1"
  }' | python3 -m json.tool
```

---

## 12. Running Tests

Tests are **unit tests only** — no Docker or running services needed. Requires Java 17 and Maven.

```bash
# Run all tests across all services
cd claim-service-java     && mvn test
cd eligibility-service-java && mvn test
cd fraud-service-java     && mvn test
```

Or using the Makefile shortcut:
```bash
make test-all
```

**What is tested:**
- `claim-service-java` — ClaimSubmit model, ClaimValidator business rules (JUnit 5)
- `eligibility-service-java` — EligibilityCheckService logic with mocked Redis (Mockito)
- `fraud-service-java` — FraudRuleService scoring with mocked Redis (Mockito)

**Redis and Kafka are mocked** in tests using Mockito. Tests run in under 5 seconds with zero infrastructure.

---

## 13. All UIs at a Glance

| UI | URL | Credentials | Use it for |
|----|-----|-------------|-----------|
| **Kafka UI** | http://localhost:8090 | None | Browse topics, see messages, check consumer lag |
| **RedisInsight** | http://localhost:5540 | None (add DB: host=`redis`, port=`6379`) | Browse all Redis keys, inspect Hashes/Sets/Sorted Sets |
| **APISIX Dashboard** | http://localhost:9000 | admin / healthonetpaadmin | View/edit routes, upstreams, consumers, plugins |
| **Claim Service Swagger** | http://localhost:8080/docs | None | Test the API interactively |
| **Claim Service 2 Swagger** | http://localhost:8081/docs | None | Second instance (LB demo) |
| **APISIX Prometheus metrics** | http://localhost:9091/apisix/prometheus/metrics | None | Raw metrics (requests, status codes per route) |

---

## 14. Environment Variables Reference

### claim-service

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9093` | Kafka broker address |
| `KAFKA_CLAIM_TOPIC` | `claim-events` | Topic to publish claims to |
| `KAFKA_ACKS` | `all` | Producer ack level |
| `KAFKA_IDEMPOTENT` | `true` | Enable exactly-once producer |
| `REDIS_HOST` | `localhost` | Redis server host |
| `REDIS_PORT` | `6379` | Redis server port |
| `APP_PORT` | `8080` | HTTP server port |
| `SERVICE_INSTANCE` | `claim-service-1` | Instance name (appears in logs) |
| `LOG_LEVEL` | `INFO` | Logging level |

### eligibility-service / fraud-service

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9093` | Kafka broker address |
| `KAFKA_CONSUMER_GROUP` | varies | Consumer group ID |
| `KAFKA_INPUT_TOPIC` | `claim-events` | Topic to consume from |
| `KAFKA_OUTPUT_TOPIC` | varies | Topic to publish results to |
| `KAFKA_AUTO_OFFSET_RESET` | `earliest` | Where to start on new group |
| `REDIS_HOST` | `localhost` | Redis server host |
| `POLICY_TTL` | `900` | (eligibility) seconds to cache policy |
| `FRAUD_SCORE_THRESHOLD` | `50` | (fraud) score at which claim is flagged |
| `HIGH_AMOUNT_THRESHOLD` | `500000` | (fraud) ₹ above which amount rule triggers |
| `DAILY_CLAIM_LIMIT` | `5` | (fraud) max daily claims per member |

---

## 15. Common Developer Tasks

### Submit a test claim
```bash
curl -s -X POST http://localhost:9080/api/v1/claims \
  -H "apikey: APOLLO-KEY-2026" \
  -H "Content-Type: application/json" \
  -d '{"member_id":"M1001","hospital_id":"H5501-Apollo",
       "hospital_name":"Apollo","insurer":"StarHealth",
       "amount":85000,"city":"mumbai",
       "claim_type":"cashless","diagnosis_code":"Z51.1"}'
```

### Watch a claim travel through the pipeline
```bash
# Terminal 1 — claim-events (what was published)
docker exec healthone-kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic claim-events \
  --from-beginning --property print.key=true

# Terminal 2 — eligibility results
docker exec healthone-kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic eligibility-results \
  --from-beginning

# Terminal 3 — audit log (all decisions)
docker exec healthone-kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic audit-log \
  --from-beginning

# Terminal 4 — service logs
docker compose logs -f eligibility-service fraud-service
```

### Check a member's remaining policy limit
```bash
docker exec healthone-redis redis-cli HGETALL member:M1001:policy
```

### Trigger a fraud flag (high amount + daily excess)
```bash
# Submit 6 claims for the same member on the same day
for i in $(seq 1 6); do
  curl -s -X POST http://localhost:9080/api/v1/claims \
    -H "apikey: APOLLO-KEY-2026" \
    -H "Content-Type: application/json" \
    -d "{\"member_id\":\"M1001\",\"hospital_id\":\"H5501-Apollo\",
         \"hospital_name\":\"Apollo\",\"insurer\":\"StarHealth\",
         \"amount\":600000,\"city\":\"mumbai\",
         \"claim_type\":\"cashless\",\"diagnosis_code\":\"Z51.1\"}" \
    | python3 -m json.tool
done

# Check fraud-alerts topic
docker exec healthone-kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic fraud-alerts --from-beginning
```

### Reset consumer group offset (replay all claims)
```bash
docker exec healthone-kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group eligibility-consumers \
  --topic claim-events \
  --reset-offsets --to-earliest --execute
```

### View top 5 riskiest hospitals
```bash
docker exec healthone-redis redis-cli ZREVRANGE fraud:hospital:scores 0 4 WITHSCORES
```

### Check consumer lag
```bash
docker exec healthone-kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --all-groups
```

### Clean restart (wipe all data)
```bash
docker compose down -v   # removes all volumes
docker compose up -d --build
bash apisix/setup-apisix.sh
```

---

## 16. Troubleshooting

### Kafka container keeps restarting
```bash
docker compose logs zookeeper   # check zookeeper is healthy first
docker compose logs kafka       # look for the actual error
# Zookeeper must be healthy before Kafka starts
```

### APISIX returns 403
The `allow_admin` IP whitelist is blocking your request. Verify `apisix/config.yaml` has:
```yaml
allow_admin:
  - 0.0.0.0/0
```
Then: `docker compose restart apisix`

### APISIX returns 401 on claim submission
The `apikey` header is missing or wrong. Use:
```bash
-H "apikey: APOLLO-KEY-2026"
```
Note: APISIX key-auth reads the `apikey` header by default (not `X-API-Key`).

### Redis policy data missing (HGETALL returns empty)
The `redis-init` container didn't run successfully:
```bash
docker compose logs redis-init
docker compose restart redis-init
```

### Eligibility service not processing claims
```bash
docker compose logs eligibility-service
# Common cause: Kafka not ready when consumer started
docker compose restart eligibility-service
```

### Port already in use
```bash
# Find which process holds the port (e.g. 9092)
# Windows:
netstat -ano | findstr :9092
# Linux/macOS:
lsof -i :9092
```

---

## 17. Key Design Decisions

### Why is claim submission async (202 Accepted)?
The hospital gets a response in < 200ms regardless of how long eligibility/fraud checks take. The hospital polls `GET /api/v1/claims/{id}` for the final decision. This matches how real TPA systems work — hospitals don't block waiting for approval.

### Why manual Kafka commit in consumers?
Both consumers use `enable.auto.commit: false` and call `consumer.commit()` only after successfully processing a message. This gives **at-least-once delivery** — if the consumer crashes mid-processing, the message is reprocessed on restart. Idempotency in the downstream systems (Redis `HINCRBYFLOAT` is idempotent for the same amount) handles the rare duplicate.

### Why Redis Hash over a full JSON string for policy?
With a JSON string, updating the `used` field requires: GET → deserialise → modify → serialise → SET. With a Hash, it's a single atomic `HINCRBYFLOAT used {amount}`. No race condition, no round-trip deserialisation.

### Why partition Kafka by insurer instead of claim_id?
If partitioned by `claim_id` (random), claims from the same member could be processed out of order across different partitions. Two concurrent claims for member M1001 could both see `used=230000` and both pass the limit check — a double-spend bug. Partitioning by insurer keeps all claims for each insurer in order.

### Why APISIX instead of writing auth in the application?
The security boundary (auth, rate limiting) is separate from business logic. Adding a new hospital is an APISIX consumer config change — no application deployment needed. The rate limit logic doesn't need to be duplicated across all three microservices.
