# Runbook 03 — Redis Module (8 Hours · May 2–5)

> **Goal by May 5:** Member policy Hashes with 15-min TTL, empanelled hospital Sets, fraud risk Sorted Set, RDB + AOF persistence. No claim state lost on Redis restart.

---

## Day 8 · May 2 · 4 Hours — CLI, Key-Value, Lists, Sets

### Theory Summary
- Redis = in-memory data structure store; keys are strings, values have a type
- `KEYS *` scans all keys (blocks Redis) → use `SCAN` in production
- TTL (Time To Live) = auto-expiry in seconds

### Lab Steps

#### 1. Connect to Redis
```bash
docker exec -it healthone-redis redis-cli

# Verify connection
PING                    # PONG
INFO server             # version, uptime, memory
DBSIZE                  # number of keys (should be > 0 from seed data)
```

#### 2. Key-Value operations
```bash
# Set a claim status with 24h TTL
SET claim:C001:status "SUBMITTED" EX 86400
GET claim:C001:status              # SUBMITTED
TTL claim:C001:status              # remaining seconds (~86400)
TYPE claim:C001:status             # string

# Update status
SET claim:C001:status "ELIGIBLE" XX   # XX = only update if exists
GET claim:C001:status

# Check existence before setting
EXISTS claim:C001:status           # 1 (exists)
EXISTS claim:XXXX:status           # 0 (does not exist)

# Delete
DEL claim:C001:status
EXISTS claim:C001:status           # 0
```

#### 3. SCAN — production-safe key enumeration
```bash
# Bad (blocks Redis) — only for small test datasets:
KEYS claim:*

# Good (non-blocking, returns cursor + batch):
SCAN 0 MATCH claim:* COUNT 100

# Continue with cursor from previous result until cursor = 0:
SCAN <cursor> MATCH claim:* COUNT 100
```

#### 4. Lists — hospital submission queue
```bash
# Simulate a queue of pending claims for Apollo hospital
RPUSH queue:hospital:Apollo C001 C002 C003 C004 C005
LLEN  queue:hospital:Apollo          # 5
LRANGE queue:hospital:Apollo 0 -1   # view all: [C001, C002, ...]

# Process next claim (FIFO)
LPOP queue:hospital:Apollo           # C001
LRANGE queue:hospital:Apollo 0 -1   # [C002, C003, C004, C005]

# Peek without removing
LINDEX queue:hospital:Apollo 0      # C002 (next to process)
```

#### 5. Sets — empanelled hospital lookup
```bash
# View existing empanelled hospitals (seeded by docker-compose)
SMEMBERS empanelled:mumbai

# Add a new hospital
SADD empanelled:mumbai SirHN

# Check empanelment — O(1) lookup
SISMEMBER empanelled:mumbai Apollo         # 1 (empanelled)
SISMEMBER empanelled:mumbai UnknownClinic  # 0 (NOT empanelled)

# Count
SCARD empanelled:mumbai

# Set operations — which hospitals are in both Mumbai and Delhi?
SINTER empanelled:mumbai empanelled:delhi
```

#### 6. Practical: cashless claim eligibility check
```bash
# Before approving a cashless claim:
# 1. Check if hospital is empanelled
SISMEMBER empanelled:mumbai Apollo     # returns 1

# If 0: auto-reject, publish to fraud-alerts
# If 1: proceed to policy check
```

#### 7. Memory config
```bash
CONFIG GET maxmemory
CONFIG SET maxmemory 256mb
CONFIG GET maxmemory-policy
CONFIG SET maxmemory-policy allkeys-lru
```

---

## Day 9 · May 4 · 2 Hours — Hashes & Sorted Sets

### Lab Steps

#### 1. Hashes — member policy cache
```bash
# View seeded member M1001 policy
HGETALL member:M1001:policy

# Access individual fields
HGET member:M1001:policy limit      # 1000000
HGET member:M1001:policy used       # 230000

# Eligibility check logic:
# remaining = limit - used = 1000000 - 230000 = 770000
# Sufficient for a ₹50,000 claim? Yes.

# Update used amount after claim approval
HINCRBY member:M1001:policy used 50000
HGET member:M1001:policy used        # 280000

# Set/check TTL (15-minute cache)
TTL member:M1001:policy              # remaining seconds
EXPIRE member:M1001:policy 900       # reset to 15 min

# Add a new member policy
HSET member:M2001:policy \
  name "Ramesh Iyer" \
  plan "Bronze Individual" \
  limit 300000 \
  used 0 \
  renewal "2027-09-30" \
  insurer "NivaHealth"
EXPIRE member:M2001:policy 900
HGETALL member:M2001:policy
```

#### 2. Why Hashes over serialised JSON?
```bash
# JSON approach — requires full de/serialise to update one field:
SET member:M1001:json '{"limit":1000000,"used":230000}'
# To update: GET → parse → modify → SET (round-trip)

# Hash approach — atomic partial update:
HINCRBY member:M1001:policy used 50000  # atomic, no round-trip
```

#### 3. Sorted Sets — fraud risk leaderboard
```bash
# View existing hospital scores (seeded)
ZRANGE fraud:hospital:scores 0 -1 WITHSCORES

# Add suspicious activity scores
ZINCRBY fraud:hospital:scores 25 "H5502-Unknown"
ZINCRBY fraud:hospital:scores 10 "H5501-Apollo"
ZINCRBY fraud:hospital:scores 5  "H5503-Fortis"

# Top 5 riskiest hospitals (highest score first)
ZREVRANGE fraud:hospital:scores 0 4 WITHSCORES

# Rank of a specific hospital (0 = lowest risk)
ZRANK    fraud:hospital:scores "H5501-Apollo"
ZREVRANK fraud:hospital:scores "H5501-Apollo"    # highest rank first

# Count hospitals with score > 20
ZCOUNT fraud:hospital:scores 20 +inf

# Remove a hospital from tracking
ZREM fraud:hospital:scores "H5503-Fortis"
```

#### 4. Connect Redis to the pipeline
The `eligibility-service` and `fraud-service` containers are already running and using Redis:

```bash
# Submit a claim and watch the services read from Redis
curl -s -X POST http://localhost:8080/api/v1/claims \
  -H "Content-Type: application/json" \
  -d '{"member_id":"M1001","hospital_id":"H5501-Apollo","hospital_name":"Apollo","insurer":"StarHealth","amount":50000,"city":"mumbai","claim_type":"cashless","diagnosis_code":"Z51.1"}'

# Watch eligibility-service logs
docker compose logs -f eligibility-service

# Check that M1001 used amount increased
HGET member:M1001:policy used

# Check fraud hospital score updated
ZREVRANGE fraud:hospital:scores 0 4 WITHSCORES
```

---

## Day 10 · May 5 · 2 Hours — Persistence (RDB + AOF)

### Theory Summary
| Mode | How it works | Data loss risk | Use case |
|------|-------------|---------------|---------|
| RDB (snapshot) | Binary dump at intervals | Up to snapshot interval | Policy cache (can reload from DB) |
| AOF (append-only) | Log every write command | Up to 1 second (everysec) | Fraud counters (cannot lose) |
| RDB + AOF | Both simultaneously | ≤ 1 second | Production recommendation |

### Lab Steps

#### 1. Check current persistence config
```bash
docker exec -it healthone-redis redis-cli

CONFIG GET save          # RDB schedule: "900 1 300 10 60 10000"
CONFIG GET appendonly    # should be "yes"
CONFIG GET appendfsync   # should be "everysec"
```

#### 2. Trigger a manual RDB snapshot
```bash
BGSAVE                   # background save

# Check last save time
LASTSAVE                 # Unix timestamp

# Verify the .rdb file
docker exec healthone-redis ls -lh /data/dump.rdb
```

#### 3. Verify AOF is recording writes
```bash
# Make 5 changes
HSET member:M9999:policy name "Test" limit 500000 used 0
ZADD fraud:hospital:scores 99 "H9999-Test"
INCR test:counter
INCR test:counter
INCR test:counter

# Check AOF file
docker exec healthone-redis ls -lh /data/appendonly.aof
docker exec healthone-redis wc -l /data/appendonly.aof  # count write entries
```

#### 4. Simulate restart — verify data survives
```bash
# Note current values
HGETALL member:M1001:policy
ZREVRANGE fraud:hospital:scores 0 4 WITHSCORES
GET test:counter

# Restart Redis
docker compose restart redis

# Reconnect and verify data
docker exec -it healthone-redis redis-cli
HGETALL member:M1001:policy          # should be intact (AOF replay)
ZREVRANGE fraud:hospital:scores 0 4 WITHSCORES  # intact
GET test:counter                     # intact (3)
```

#### 5. Fraud counter with daily expiry
```bash
TODAY=$(date +%Y-%m-%d)
INCR fraud:member:M1001:claims:${TODAY}
EXPIRE fraud:member:M1001:claims:${TODAY} 86400
TTL fraud:member:M1001:claims:${TODAY}
GET fraud:member:M1001:claims:${TODAY}   # 1

# Restart Redis and verify counter survived
docker compose restart redis
sleep 5
docker exec healthone-redis redis-cli GET fraud:member:M1001:claims:${TODAY}   # still 1
```

#### 6. RDB-only vs AOF — discussion
```bash
# Disable AOF (rely on RDB only — for caches that can reload from source DB)
CONFIG SET appendonly no
CONFIG GET appendonly

# Re-enable AOF
CONFIG SET appendonly yes
```

Trade-off discussion:
- Fraud counters **need AOF** — a lost counter means missed duplicate alert
- Policy cache **can use RDB** — worst case: re-fetch from core DB on miss

---

## Redis Command Reference

```bash
# Key operations
SET key value [EX seconds] [NX|XX]
GET key
DEL key [key ...]
EXISTS key
EXPIRE key seconds
TTL key
TYPE key
SCAN cursor [MATCH pattern] [COUNT count]

# Strings
INCR key
INCRBY key increment

# Hashes
HSET key field value [field value ...]
HGET key field
HGETALL key
HMGET key field [field ...]
HINCRBY key field increment
HINCRBYFLOAT key field increment
HDEL key field
EXPIRE key seconds    # TTL on whole hash

# Lists
RPUSH key value [value ...]
LPOP key
LRANGE key start stop
LLEN key

# Sets
SADD key member [member ...]
SMEMBERS key
SISMEMBER key member
SCARD key
SINTER key [key ...]

# Sorted Sets
ZADD key score member [score member ...]
ZINCRBY key increment member
ZRANGE key start stop [WITHSCORES]
ZREVRANGE key start stop [WITHSCORES]
ZRANK key member
ZREVRANK key member
ZCOUNT key min max
ZREM key member [member ...]

# Persistence
BGSAVE
LASTSAVE
CONFIG GET save
CONFIG GET appendonly
CONFIG SET appendonly yes|no
```

---

## Module Checklist

- [ ] Redis CLI connected and `PING` → `PONG`
- [ ] `SCAN` used instead of `KEYS *`
- [ ] Member policy Hashes with 15-min TTL (`HSET` + `EXPIRE`)
- [ ] Empanelled hospital Sets — `SISMEMBER` returns 1/0 correctly
- [ ] Fraud risk Sorted Set updated — `ZREVRANGE ... WITHSCORES`
- [ ] `BGSAVE` run and `.rdb` file verified
- [ ] AOF enabled and `.aof` file growing with writes
- [ ] Redis restarted and data verified intact
- [ ] Daily fraud counter using `INCR` + `EXPIRE`
