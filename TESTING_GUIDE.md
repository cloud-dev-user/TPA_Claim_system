# HealthOne TPA Claims Pipeline — Testing Guide

> Everything a developer needs to test this project:  
> unit tests, manual API tests, Kafka tests, Redis tests, and end-to-end pipeline verification.

---

## Table of Contents

1. [Testing Strategy](#1-testing-strategy)
2. [Unit Tests — Quick Start](#2-unit-tests--quick-start)
3. [Claim Service Tests — Line by Line](#3-claim-service-tests--line-by-line)
4. [Eligibility Service Tests — Line by Line](#4-eligibility-service-tests--line-by-line)
5. [Fraud Service Tests — Line by Line](#5-fraud-service-tests--line-by-line)
6. [Manual API Testing (curl)](#6-manual-api-testing-curl)
7. [APISIX Gateway Testing](#7-apisix-gateway-testing)
8. [Kafka Testing](#8-kafka-testing)
9. [Redis Testing](#9-redis-testing)
10. [End-to-End Pipeline Test](#10-end-to-end-pipeline-test)
11. [CI Pipeline (GitLab)](#11-ci-pipeline-gitlab)
12. [Test Coverage Reference](#12-test-coverage-reference)

---

## 1. Testing Strategy

This project uses **three layers of testing**:

```
┌─────────────────────────────────────────────────────┐
│  Layer 3 — End-to-End                               │
│  Full stack running in Docker                        │
│  curl → APISIX → Claim Service → Kafka → Services   │
├─────────────────────────────────────────────────────┤
│  Layer 2 — Manual / Integration                     │
│  Docker stack running, test one component at a time  │
│  curl, redis-cli, kafka-console-consumer             │
├─────────────────────────────────────────────────────┤
│  Layer 1 — Unit Tests  ← Start here                 │
│  No Docker needed. Fast. Run on every code change.   │
│  pytest with mocked Redis and Kafka                  │
└─────────────────────────────────────────────────────┘
```

| Layer | Speed | Infra needed | Run on |
|-------|-------|-------------|--------|
| Unit | < 5 seconds | None | Every save / GitLab CI |
| Manual/Integration | ~30 seconds | Docker stack up | During development |
| End-to-End | ~2 minutes | Full stack + APISIX configured | Before merging |

---

## 2. Unit Tests — Quick Start

### Install dependencies (once per service)

```bash
# Claim Service
cd claim-service
pip install -r requirements.txt

# Eligibility Service
cd eligibility-service
pip install -r requirements.txt

# Fraud Service
cd fraud-service
pip install -r requirements.txt
```

### Run all tests

```bash
# From each service directory:
pytest tests/ -v

# With short traceback on failure:
pytest tests/ -v --tb=short

# Stop on first failure:
pytest tests/ -v -x

# Run a specific test file:
pytest tests/test_claim.py -v

# Run a specific test by name:
pytest tests/test_claim.py::TestClaimValidator::test_amount_below_minimum_rejected -v

# Run all services at once (from Training_code/ root):
make test-all
```

### Expected output — all passing

```
claim-service/
  tests/test_claim.py::TestClaimSubmit::test_claim_id_auto_generated        PASSED
  tests/test_claim.py::TestClaimSubmit::test_default_status_is_submitted     PASSED
  tests/test_claim.py::TestClaimSubmit::test_submitted_at_is_set             PASSED
  tests/test_claim.py::TestClaimSubmit::test_to_dict_round_trip              PASSED
  tests/test_claim.py::TestClaimSubmit::test_to_dict_contains_all_required_keys PASSED
  tests/test_claim.py::TestClaimSubmit::test_two_claims_have_different_ids   PASSED
  tests/test_claim.py::TestClaimValidator::test_valid_claim_passes           PASSED
  ... (16 tests total)

eligibility-service/
  tests/test_eligibility.py::TestEligibilityCheck::test_eligible_cashless_claim PASSED
  ... (8 tests total)

fraud-service/
  tests/test_fraud.py::TestFraudRule::test_normal_claim_is_clear             PASSED
  ... (11 tests total)
```

---

## 3. Claim Service Tests — Line by Line

**File:** [claim-service/tests/test_claim.py](claim-service/tests/test_claim.py)  
**What it tests:** `ClaimSubmit` data model and `ClaimValidator` business rules  
**Infrastructure needed:** None

### Fixtures (shared test data)

```python
@pytest.fixture
def valid_claim() -> ClaimSubmit:
    # A baseline claim that passes all validation rules.
    # Tests that expect failure mutate ONE field from this baseline.
    return ClaimSubmit(
        member_id="M1001",
        hospital_id="H5501-Apollo",
        hospital_name="Apollo Hospital Mumbai",
        insurer="StarHealth",
        amount=85000.0,
        city="mumbai",
        claim_type="cashless",
        diagnosis_code="Z51.1",
    )

@pytest.fixture
def validator() -> ClaimValidator:
    return ClaimValidator()
```

> **Pattern used:** One valid baseline fixture. Each failing test mutates exactly one field.
> This isolates the cause — if a test fails, you know exactly which rule broke.

---

### TestClaimSubmit — model behaviour

#### `test_claim_id_auto_generated`
```python
assert valid_claim.claim_id.startswith("C")
assert len(valid_claim.claim_id) == 9  # 'C' + 8 hex chars
```
**Why:** Every claim needs a unique, predictable ID format (`C` + 8 hex = e.g. `C3F9A1B2`).
The ID is used as the Redis key and Kafka message key downstream.

#### `test_default_status_is_submitted`
```python
assert valid_claim.status == ClaimStatus.SUBMITTED
```
**Why:** A freshly created claim must start as `SUBMITTED` before any processing.
Other statuses (`ELIGIBLE`, `FRAUD_REVIEW`) are set by downstream services.

#### `test_submitted_at_is_set`
```python
assert "T" in valid_claim.submitted_at  # ISO 8601: "2026-04-24T10:30:00+00:00"
```
**Why:** The timestamp is written to the audit-log topic. It must be ISO 8601 so downstream
services can parse it without guessing the format.

#### `test_to_dict_round_trip`
```python
d = valid_claim.to_dict()
restored = ClaimSubmit.from_dict(d)
assert restored.claim_id == valid_claim.claim_id
assert restored.amount == valid_claim.amount
```
**Why:** The claim is serialised to JSON before being published to Kafka.
If `to_dict()` loses data or `from_dict()` misreads it, consumers get corrupted events.

#### `test_two_claims_have_different_ids`
```python
c1 = ClaimSubmit(...)
c2 = ClaimSubmit(...)  # identical inputs
assert c1.claim_id != c2.claim_id
```
**Why:** Two hospitals could submit identical claims for the same member.
Duplicate `claim_id` values would cause Redis key collisions and Kafka offset confusion.

---

### TestClaimValidator — business rules

#### `test_valid_claim_passes`
```python
ok, errors = validator.validate(valid_claim)
assert ok is True
assert errors == []
```
**Why:** The baseline fixture must pass cleanly — confirms the validator isn't
over-zealous before we test each failure case.

#### Member ID tests

| Test | Input | Expected |
|------|-------|----------|
| `test_member_id_must_start_with_M` | `"X1001"` | Rejected — `member_id` error |
| `test_member_id_too_short` | `"M1"` | Rejected — too short |
| `test_member_id_empty` | `""` | Rejected |

**Rule:** HealthOne TPA member IDs always start with `M` and are at least 5 characters
(`M` + 4 digit number). Anything else is not a valid member in the system.

#### Amount boundary tests

| Test | Amount | Expected |
|------|--------|----------|
| `test_amount_below_minimum_rejected` | `₹50` | Rejected (min is ₹100) |
| `test_amount_above_maximum_rejected` | `₹1.5 crore` | Rejected (max is ₹1 crore) |
| `test_amount_at_minimum_accepted` | `₹100` | **Accepted** (boundary — inclusive) |
| `test_amount_at_maximum_accepted` | `₹1 crore` | **Accepted** (boundary — inclusive) |

**Why boundary tests matter:** Off-by-one errors in comparisons (`<` vs `<=`) are the most
common validator bugs. Testing both sides of each boundary catches them.

#### `test_invalid_claim_type_rejected`
```python
valid_claim.claim_type = "emergency"
ok, errors = validator.validate(valid_claim)
assert any("claim_type" in e for e in errors)
```
**Why:** Only `cashless` and `reimbursement` are valid types.
`emergency` is not a valid claim type in HealthOne TPA's system — it's a hospital admission
category, not a claim category.

#### `test_multiple_errors_returned_at_once`
```python
bad_claim = ClaimSubmit(
    member_id="X1",    # bad: wrong prefix + too short
    hospital_id="",    # bad: empty
    insurer="",        # bad: empty
    amount=50,         # bad: below minimum
    claim_type="wrong",# bad: invalid type
    diagnosis_code="AB",# bad: too short
    ...
)
ok, errors = validator.validate(bad_claim)
assert len(errors) >= 5
```
**Why:** The validator must collect ALL errors, not stop at the first one.
If a hospital submits a bad claim, they need to fix everything in one round-trip,
not discover one error at a time.

#### `test_wrong_amount_threshold_bug` ← GitLab Session 3 demo test
```python
claim = ClaimSubmit(amount=12_000_000, ...)  # ₹1.2 crore
ok, errors = validator.validate(claim)
assert ok is False, "BUG: amount above ₹1 crore was accepted"
```
**Why this test exists:** In GitLab Session 3, participants deliberately introduce a bug
(changing `MAX_AMOUNT = 10_000_000` to `MAX_AMOUNT = 100`). This test is the one that
goes **RED** on the GitLab CI pipeline when the bug is present, demonstrating that
CI catches regressions automatically.

---

## 4. Eligibility Service Tests — Line by Line

**File:** [eligibility-service/tests/test_eligibility.py](eligibility-service/tests/test_eligibility.py)  
**What it tests:** `EligibilityCheck` business logic  
**Infrastructure needed:** None (Redis is mocked)

### How Redis is mocked

```python
@pytest.fixture
def mock_redis():
    return MagicMock()   # every method returns MagicMock() by default

@pytest.fixture
def checker(mock_redis):
    return EligibilityCheck(redis_client=mock_redis)
    # The real EligibilityCheck accepts any redis-compatible object
    # MagicMock stands in for redis.Redis
```

Each test configures `mock_redis` return values to simulate specific Redis states:

```python
mock_redis.sismember.return_value = True   # hospital IS empanelled
mock_redis.hgetall.return_value = {"limit": "1000000", "used": "230000", ...}
mock_redis.hincrbyfloat.return_value = 280000.0
```

---

### Test walkthrough

#### `test_eligible_cashless_claim` — the happy path
```python
mock_redis.sismember.return_value = True          # Apollo is empanelled in Mumbai
mock_redis.hgetall.return_value = {
    "plan": "Gold Family Floater",
    "limit": "1000000",
    "used": "230000",
}
mock_redis.hincrbyfloat.return_value = 280000.0

result = checker.check(valid_claim)  # amount=50000

assert result.eligible is True
assert result.remaining_limit == 720000.0   # 1000000 - 230000 - 50000
assert result.policy_plan == "Gold Family Floater"
```
**What this proves:** Given a fully empanelled hospital and a member with sufficient
coverage, the check approves the claim and returns the correct remaining limit.

#### `test_hospital_not_empanelled` — cashless rejection
```python
mock_redis.sismember.return_value = False   # hospital NOT in empanelled set

result = checker.check(valid_claim)

assert result.eligible is False
assert "not empanelled" in result.reason
mock_redis.hgetall.assert_not_called()   # should NOT even fetch the policy
```
**Key assertion:** `hgetall.assert_not_called()` — if the hospital fails empanelment,
the check must **stop immediately** and not waste a Redis call fetching the policy.
This tests the short-circuit logic in the code.

#### `test_member_policy_not_found`
```python
mock_redis.sismember.return_value = True
mock_redis.hgetall.return_value = {}   # empty dict = key doesn't exist in Redis

result = checker.check(valid_claim)

assert result.eligible is False
assert "No active policy" in result.reason
```
**Why:** A member could have an expired policy (TTL elapsed, not yet refreshed).
The service must not approve a claim when policy data is absent — fail safe, not fail open.

#### `test_insufficient_limit`
```python
mock_redis.hgetall.return_value = {"plan": "Silver", "limit": "100000", "used": "90000"}
valid_claim["amount"] = 50000   # 50000 > (100000 - 90000 = 10000)

result = checker.check(valid_claim)

assert result.eligible is False
assert "exceeds" in result.reason
assert result.remaining_limit == 10000.0   # reports what IS available
```
**Why:** The remaining limit is included in the rejection reason so the hospital
knows how much the member can still claim.

#### `test_exact_remaining_limit_is_eligible`
```python
mock_redis.hgetall.return_value = {"limit": "100000", "used": "50000"}
valid_claim["amount"] = 50000   # exactly equal to remaining

assert result.eligible is True
```
**Why (boundary test):** If the comparison uses `>` instead of `>=`, a claim for
the exact remaining amount would be wrongly rejected. This catches that off-by-one.

#### `test_reimbursement_skips_empanelment_check`
```python
valid_claim["claim_type"] = "reimbursement"

result = checker.check(valid_claim)

mock_redis.sismember.assert_not_called()   # empanelment only applies to cashless
assert result.eligible is True
```
**Why:** Reimbursement claims can go to any hospital — the patient paid upfront and
is claiming back. The empanelment Set is only relevant for cashless (direct billing).

#### `test_redis_error_on_policy_fetch` — resilience test
```python
mock_redis.hgetall.side_effect = redis.RedisError("connection refused")

result = checker.check(valid_claim)

assert result.eligible is False
assert "Redis error" in result.reason
```
**Why:** Redis going down must not crash the service or approve claims silently.
It must fail safe (reject) and log the error. `side_effect` makes the mock
**raise an exception** instead of returning a value.

---

## 5. Fraud Service Tests — Line by Line

**File:** [fraud-service/tests/test_fraud.py](fraud-service/tests/test_fraud.py)  
**What it tests:** `FraudRule` scoring logic  
**Infrastructure needed:** None (Redis is mocked)

### Mock defaults

```python
@pytest.fixture
def mock_redis():
    r = MagicMock()
    r.incr.return_value = 1      # member's 1st claim today (well under 5 limit)
    r.zincrby.return_value = 5.0 # hospital score after increment
    return r
```

Default mock simulates a clean state: first claim of the day, no fraud history.

---

### Fraud scoring tests

#### `test_normal_claim_is_clear`
```python
result = fraud_rule.assess(normal_claim)   # amount=50000, 1st claim today

assert result.is_flagged is False
assert result.risk_score < 50   # below the 50-point flag threshold
```
**Why:** A baseline normal claim must produce a CLEAR verdict with score below the
threshold. Confirms no false positives on normal activity.

#### `test_high_amount_increases_score`
```python
normal_claim["amount"] = 600000   # ₹6 lakh > HIGH_AMOUNT_THRESHOLD (₹5 lakh)

result = fraud_rule.assess(normal_claim)

assert result.risk_score >= 20
assert any("High claim amount" in r for r in result.reasons)
```
**Rule 1 verified:** Claims ≥ ₹5,00,000 add 20 points to risk score and include
an explanatory reason string.

#### `test_round_number_increases_score`
```python
normal_claim["amount"] = 200000   # exact multiple of ₹1 lakh

assert any("Round-number" in r for r in result.reasons)
```
**Rule 2 verified:** Round numbers (₹1L, ₹2L, ₹5L, etc.) are a classic fraud signal —
fraudulent claims are often inflated to convenient round figures.

#### `test_daily_limit_excess_increases_score`
```python
mock_redis.incr.return_value = 6   # member's 6th claim today (limit is 5)

result = fraud_rule.assess(normal_claim)

assert any("claims today" in r for r in result.reasons)
```
**Rule 3 verified:** More than 5 claims by the same member in one day adds 30 points.
`mock_redis.incr.return_value = 6` simulates Redis returning 6 from `INCR`.

#### `test_high_amount_and_daily_excess_flags_claim` — crossing the threshold
```python
mock_redis.incr.return_value = 6   # +30 points
normal_claim["amount"] = 600000    # +20 points
# Total = 50 = exactly at threshold

assert result.is_flagged is True
assert result.risk_score >= 50
```
**Why:** This is the only test that combines two rules to breach the 50-point
flag threshold. Confirms that `is_flagged = (score >= 50)` logic is correct.

#### `test_hospital_score_updated_on_every_claim`
```python
fraud_rule.assess(normal_claim)

mock_redis.zincrby.assert_called_once()
call_args = mock_redis.zincrby.call_args
assert call_args[0][0] == "fraud:hospital:scores"   # correct key
assert call_args[0][2] == normal_claim["hospital_id"]  # correct hospital
```
**Why:** The hospital Sorted Set must be updated on **every** claim — even clean ones.
Over time, high-volume hospitals accumulate scores that the ops team monitors daily.
This test verifies the Redis call happened with the right arguments.

#### `test_redis_error_on_incr_does_not_crash`
```python
mock_redis.incr.side_effect = redis.RedisError("timeout")

result = fraud_rule.assess(normal_claim)

assert isinstance(result, FraudAssessment)   # must still return a result
```
**Why:** Redis being slow or down must not crash the fraud service or block claim
processing. The rule logs the error and continues with a partial score.

#### `test_get_top_risk_hospitals`
```python
mock_redis.zrevrange.return_value = [
    ("H5502-Unknown", 75.0),
    ("H5501-Apollo",  25.0),
]
top = fraud_rule.get_top_risk_hospitals(n=2)

assert top[0][0] == "H5502-Unknown"
assert top[0][1] == 75.0
```
**Why:** The ops dashboard calls `get_top_risk_hospitals()` daily. This verifies
the Sorted Set query returns results in descending score order (highest risk first).

---

## 6. Manual API Testing (curl)

> Docker stack must be running: `docker compose up -d`

### Submit a valid claim (expect HTTP 202)

```bash
curl -s -X POST http://localhost:8080/api/v1/claims \
  -H "Content-Type: application/json" \
  -d '{
    "member_id":      "M1001",
    "hospital_id":    "H5501-Apollo",
    "hospital_name":  "Apollo",
    "insurer":        "StarHealth",
    "amount":         85000,
    "city":           "mumbai",
    "claim_type":     "cashless",
    "diagnosis_code": "Z51.1"
  }' | python -m json.tool
```

**Expected response (HTTP 202):**
```json
{
  "claim_id": "C3F9A1B2",
  "status": "SUBMITTED",
  "member_id": "M1001",
  "insurer": "StarHealth",
  "amount": 85000.0,
  "submitted_at": "2026-04-24T10:30:00+00:00",
  "message": "Claim received and queued for processing"
}
```

---

### Validation failure tests (expect HTTP 422)

```bash
# Bad member_id (doesn't start with M)
curl -s -X POST http://localhost:8080/api/v1/claims \
  -H "Content-Type: application/json" \
  -d '{"member_id":"X1001","hospital_id":"H001","hospital_name":"H",
       "insurer":"Star","amount":50000,"city":"mumbai",
       "claim_type":"cashless","diagnosis_code":"Z51.1"}' | python -m json.tool
# Expected: 422 with "member_id" in errors list

# Amount too low
curl -s -X POST http://localhost:8080/api/v1/claims \
  -H "Content-Type: application/json" \
  -d '{"member_id":"M1001","hospital_id":"H001","hospital_name":"H",
       "insurer":"Star","amount":50,"city":"mumbai",
       "claim_type":"cashless","diagnosis_code":"Z51.1"}' | python -m json.tool
# Expected: 422 with "amount" in errors

# Invalid claim_type
curl -s -X POST http://localhost:8080/api/v1/claims \
  -H "Content-Type: application/json" \
  -d '{"member_id":"M1001","hospital_id":"H001","hospital_name":"H",
       "insurer":"Star","amount":50000,"city":"mumbai",
       "claim_type":"emergency","diagnosis_code":"Z51.1"}' | python -m json.tool
# Expected: 422 with "claim_type" in errors

# Multiple bad fields at once
curl -s -X POST http://localhost:8080/api/v1/claims \
  -H "Content-Type: application/json" \
  -d '{"member_id":"X1","hospital_id":"","insurer":"",
       "hospital_name":"H","amount":10,"city":"mumbai",
       "claim_type":"bad","diagnosis_code":"A"}' | python -m json.tool
# Expected: 422 with at least 5 errors
```

---

### Claim status check (expect HTTP 200)

```bash
# Use the claim_id from a previous submission
curl -s http://localhost:8080/api/v1/claims/C3F9A1B2 | python -m json.tool
# Expected: {"claim_id": "C3F9A1B2", "status": "SUBMITTED"}

# Non-existent claim (expect HTTP 404)
curl -s -o /dev/null -w "%{http_code}" \
  http://localhost:8080/api/v1/claims/CNOTFOUND
# Expected: 404
```

---

### Health and readiness probes

```bash
# Liveness (always OK if process is running)
curl http://localhost:8080/health
# Expected: {"status":"ok","instance":"claim-service-1"}

# Readiness (checks Redis connectivity)
curl http://localhost:8080/ready
# Expected: {"status":"ready","checks":{"redis":"ok"}}

# Second instance
curl http://localhost:8081/health
# Expected: {"status":"ok","instance":"claim-service-2"}
```

---

## 7. APISIX Gateway Testing

> Run `bash apisix/setup-apisix.sh` first to configure routes and consumers.

### Authentication tests

```bash
# Valid Apollo key → 202 Accepted
curl -s -o /dev/null -w "%{http_code}" \
  -X POST http://localhost:9080/api/v1/claims \
  -H "apikey: APOLLO-KEY-2026" \
  -H "Content-Type: application/json" \
  -d '{"member_id":"M1001","hospital_id":"H5501-Apollo","hospital_name":"Apollo",
       "insurer":"StarHealth","amount":85000,"city":"mumbai",
       "claim_type":"cashless","diagnosis_code":"Z51.1"}'
# Expected: 202

# No key → 401 Unauthorized
curl -s -o /dev/null -w "%{http_code}" \
  -X POST http://localhost:9080/api/v1/claims \
  -H "Content-Type: application/json" \
  -d '{"test":1}'
# Expected: 401

# Wrong key → 401 Unauthorized
curl -s -o /dev/null -w "%{http_code}" \
  -X POST http://localhost:9080/api/v1/claims \
  -H "apikey: WRONG-KEY-9999" \
  -d '{"test":1}'
# Expected: 401
```

### Rate limit test (expect 429 after limit)

```bash
# Generic hospital has 20 req/min limit — send 22 requests
for i in $(seq 1 22); do
  CODE=$(curl -s -o /dev/null -w "%{http_code}" \
    -X POST http://localhost:9080/api/v1/claims \
    -H "apikey: GENERIC-KEY-2026" \
    -H "Content-Type: application/json" \
    -d '{"member_id":"M1001","hospital_id":"H001","hospital_name":"H",
         "insurer":"Star","amount":50000,"city":"mumbai",
         "claim_type":"cashless","diagnosis_code":"Z51.1"}')
  echo "Request $i: HTTP $CODE"
done
# Expected: first 20 return 202, requests 21+ return 429
```

### 429 response body check

```bash
curl -s -X POST http://localhost:9080/api/v1/claims \
  -H "apikey: GENERIC-KEY-2026" \
  -H "Content-Type: application/json" \
  -d '{}' | python -m json.tool
# After rate limit exceeded, expected:
# {"error": "Rate limit exceeded", "retry_after": 60, "hospital": "hospital-generic"}
```

### Load balancing verification

```bash
# Submit 10 claims — check which instance responds in logs
for i in $(seq 1 10); do
  curl -s http://localhost:9080/api/v1/claims/CTEST$i \
    -H "apikey: APOLLO-KEY-2026" > /dev/null
done

# Check logs — should see alternating claim-service-1 and claim-service-2
docker compose logs claim-service-1 | tail -5
docker compose logs claim-service-2 | tail -5
```

---

## 8. Kafka Testing

### Verify topics exist

```bash
docker exec healthone-kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
# Expected output:
# audit-log
# claim-events
# eligibility-results
# fraud-alerts
```

### Verify topic configuration

```bash
# claim-events should have 3 partitions and 7-day retention
docker exec healthone-kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic claim-events
# Look for: PartitionCount: 3  RetentionMs: 604800000
```

### Publish a test message and read it back

```bash
# Terminal 1 — start consumer first
docker exec -it healthone-kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic claim-events \
  --from-beginning \
  --property print.key=true \
  --property print.partition=true

# Terminal 2 — publish a keyed message
echo 'StarHealth:{"claim_id":"CTEST001","insurer":"StarHealth","amount":50000}' | \
  docker exec -i healthone-kafka kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic claim-events \
    --property parse.key=true \
    --property key.separator=":"

# Terminal 1 should show the message immediately, with partition number
```

### Verify partition routing (insurer key test)

```bash
# Submit 6 claims — 3 StarHealth, 2 HDFCErgo, 1 ICICILombard
# Then consume with partition info
docker exec healthone-kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic claim-events \
  --from-beginning \
  --property print.key=true \
  --property print.partition=true \
  --timeout-ms 5000 2>/dev/null

# Verify: all StarHealth claims are in the SAME partition
# Verify: all HDFCErgo claims are in the SAME partition
```

### Consumer group lag check

```bash
# After submitting some claims:
docker exec healthone-kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group eligibility-consumers

# Columns to check:
# LAG = 0 means consumer is caught up
# LAG > 0 means consumer is behind (check service logs)
```

### Replay test (compliance audit simulation)

```bash
# Start a new consumer group — reads ALL historical messages from the start
docker exec -it healthone-kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic claim-events \
  --from-beginning \
  --group replay-$(date +%s) \
  --property print.key=true \
  --timeout-ms 10000 2>/dev/null

# Count how many claims per insurer in history:
docker exec healthone-kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic claim-events \
  --from-beginning \
  --group replay-count \
  --timeout-ms 5000 2>/dev/null | \
  python -c "
import sys, json
counts = {}
for line in sys.stdin:
    try:
        d = json.loads(line.strip())
        k = d.get('insurer', 'unknown')
        counts[k] = counts.get(k, 0) + 1
    except: pass
for k,v in sorted(counts.items()): print(f'{k}: {v} claims')
"
```

---

## 9. Redis Testing

### Verify seed data loaded correctly

```bash
docker exec healthone-redis redis-cli HGETALL member:M1001:policy
# Expected:
# name       Anita Sharma
# plan       Gold Family Floater
# limit      1000000
# used       230000
# renewal    2027-03-31
# insurer    StarHealth

docker exec healthone-redis redis-cli SMEMBERS empanelled:mumbai
# Expected: Apollo Fortis Kokilaben Max Lilavati Breach_Candy

docker exec healthone-redis redis-cli ZREVRANGE fraud:hospital:scores 0 -1 WITHSCORES
# Expected: H5501-Apollo 5  H5502-Fortis 2  H5503-Kokilaben 0
```

### Empanelment check tests

```bash
# Should return 1 (empanelled)
docker exec healthone-redis redis-cli SISMEMBER empanelled:mumbai Apollo
# Should return 0 (not empanelled)
docker exec healthone-redis redis-cli SISMEMBER empanelled:mumbai UnknownClinic
# Non-existent city — empty set = not empanelled
docker exec healthone-redis redis-cli SISMEMBER empanelled:patna Apollo
```

### Policy cache TTL test

```bash
# Check remaining TTL on a policy (should be close to 900 seconds after seeding)
docker exec healthone-redis redis-cli TTL member:M1001:policy

# Manually expire it to simulate cache miss
docker exec healthone-redis redis-cli EXPIRE member:M1001:policy 1
sleep 2

# Submit a claim — eligibility service will see a cache miss
# Check eligibility-service logs for "Policy not found" or DB fallback
docker compose logs eligibility-service | tail -10
```

### Policy limit deduction test

```bash
# Record current used amount
docker exec healthone-redis redis-cli HGET member:M1001:policy used
# e.g. 230000

# Submit a claim for ₹50,000
curl -s -X POST http://localhost:8080/api/v1/claims \
  -H "Content-Type: application/json" \
  -d '{"member_id":"M1001","hospital_id":"H5501-Apollo","hospital_name":"Apollo",
       "insurer":"StarHealth","amount":50000,"city":"mumbai",
       "claim_type":"cashless","diagnosis_code":"Z51.1"}'

# Wait a moment for eligibility service to process
sleep 3

# Check used amount increased by 50000
docker exec healthone-redis redis-cli HGET member:M1001:policy used
# Expected: 280000 (230000 + 50000)
```

### Fraud score accumulation test

```bash
# Record current score for Apollo
docker exec healthone-redis redis-cli ZSCORE fraud:hospital:scores H5501-Apollo

# Submit several claims for Apollo
for i in $(seq 1 3); do
  curl -s -X POST http://localhost:8080/api/v1/claims \
    -H "Content-Type: application/json" \
    -d '{"member_id":"M1001","hospital_id":"H5501-Apollo","hospital_name":"Apollo",
         "insurer":"StarHealth","amount":50000,"city":"mumbai",
         "claim_type":"cashless","diagnosis_code":"Z51.1"}' > /dev/null
done
sleep 3

# Score should have increased
docker exec healthone-redis redis-cli ZSCORE fraud:hospital:scores H5501-Apollo
```

### Persistence test (data survives restart)

```bash
# Note current values
USED=$(docker exec healthone-redis redis-cli HGET member:M1001:policy used)
echo "used before restart: $USED"

# Restart Redis
docker compose restart redis
sleep 5

# Check values survived (AOF replay)
docker exec healthone-redis redis-cli HGET member:M1001:policy used
# Must match $USED
```

---

## 10. End-to-End Pipeline Test

This test submits a claim and verifies it travelled through **every** component.

### Setup: open 4 terminals

```bash
# Terminal 1 — watch eligibility decisions
docker exec -it healthone-kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic eligibility-results \
  --from-beginning --property print.key=true

# Terminal 2 — watch fraud assessments
docker exec -it healthone-kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic fraud-alerts \
  --from-beginning --property print.key=true

# Terminal 3 — watch audit log
docker exec -it healthone-kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic audit-log \
  --from-beginning --property print.key=true

# Terminal 4 — submit the claim
curl -s -X POST http://localhost:9080/api/v1/claims \
  -H "apikey: APOLLO-KEY-2026" \
  -H "Content-Type: application/json" \
  -d '{
    "member_id":      "M1001",
    "hospital_id":    "H5501-Apollo",
    "hospital_name":  "Apollo",
    "insurer":        "StarHealth",
    "amount":         85000,
    "city":           "mumbai",
    "claim_type":     "cashless",
    "diagnosis_code": "Z51.1"
  }' | python -m json.tool
```

### What you should observe

| Step | Where | What to see |
|------|-------|------------|
| 1 | Terminal 4 | HTTP 202 with a `claim_id` e.g. `C3F9A1B2` |
| 2 | Terminal 1 | `eligibility-results`: `{"decision":"ELIGIBLE","claim_id":"C3F9A1B2",...}` |
| 3 | Terminal 2 | Nothing (normal claim, not flagged) |
| 4 | Terminal 3 | `audit-log`: two entries — `eligibility_decision` + `fraud_assessment` |
| 5 | Redis | `HGET member:M1001:policy used` increased by 85000 |

### Trigger a fraud alert

```bash
# High amount + 6th claim today = score ≥ 50
for i in $(seq 1 6); do
  curl -s -X POST http://localhost:9080/api/v1/claims \
    -H "apikey: APOLLO-KEY-2026" \
    -H "Content-Type: application/json" \
    -d '{"member_id":"M1002","hospital_id":"H5501-Apollo","hospital_name":"Apollo",
         "insurer":"StarHealth","amount":600000,"city":"mumbai",
         "claim_type":"cashless","diagnosis_code":"Z51.1"}' > /dev/null
done

# Terminal 2 should now show a fraud alert message
```

### Verify both consumer groups are independent

```bash
# Reset ONLY eligibility-consumers offset, not fraud-consumers
docker exec healthone-kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group eligibility-consumers \
  --topic claim-events \
  --reset-offsets --to-earliest --execute

# Restart eligibility-service to pick up the reset
docker compose restart eligibility-service

# fraud-consumers should NOT be affected — check its lag separately
docker exec healthone-kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group fraud-consumers
```

---

## 11. CI Pipeline (GitLab)

Every push to any branch on `claim-service` triggers the pipeline defined in
[claim-service/.gitlab-ci.yml](claim-service/.gitlab-ci.yml).

### Pipeline stages

```
Stage 1: lint          Stage 2: test
┌──────────┐           ┌──────────┐
│  flake8  │  ──────▶  │  pytest  │
│          │  (only if │          │
│ max 100  │   lint    │ JUnit    │
│ chars/ln │   passes) │ report   │
└──────────┘           └──────────┘
```

### Triggering the pipeline

```bash
# Make a change and push to your feature branch
git add app/claim.py
git commit -m "feat: update ClaimValidator max amount"
git push origin feature/claim-api

# In GitLab UI: CI/CD → Pipelines → watch both stages
```

### Making the pipeline fail (GitLab Session 3 demo)

```bash
# Introduce the deliberate bug — change MAX_AMOUNT
# In claim.py line ~60: MAX_AMOUNT: float = 100  # wrong!
git add app/claim.py
git commit -m "fix: update amount threshold"
git push origin feature/claim-api

# Pipeline will go RED on pytest stage:
# FAILED tests/test_claim.py::TestClaimValidator::test_wrong_amount_threshold_bug
# FAILED tests/test_claim.py::TestClaimValidator::test_amount_at_maximum_accepted
```

```bash
# Fix the bug
# Restore: MAX_AMOUNT: float = 10_000_000
git revert HEAD
git push origin feature/claim-api
# Pipeline goes GREEN
```

### Reading the pipeline results

In GitLab UI: CI/CD → Pipelines → click the pipeline → click `pytest` job:
- Green ✓ = all 16 tests passed
- Red ✗ = see the test output for which assertion failed
- JUnit report is attached as an artifact — download it for the test report

---

## 12. Test Coverage Reference

### All unit tests at a glance

| Test | Service | What it covers |
|------|---------|---------------|
| `test_claim_id_auto_generated` | claim | ClaimSubmit auto-generates `C` + 8 hex ID |
| `test_default_status_is_submitted` | claim | New claim status = SUBMITTED |
| `test_submitted_at_is_set` | claim | Timestamp in ISO 8601 format |
| `test_to_dict_round_trip` | claim | Serialise → deserialise preserves all fields |
| `test_to_dict_contains_all_required_keys` | claim | Dict has all required keys for Kafka |
| `test_two_claims_have_different_ids` | claim | UUID uniqueness for same inputs |
| `test_valid_claim_passes` | claim | Baseline valid claim passes all rules |
| `test_member_id_must_start_with_M` | claim | Wrong prefix rejected |
| `test_member_id_too_short` | claim | Min 5 chars enforced |
| `test_member_id_empty` | claim | Empty string rejected |
| `test_amount_below_minimum_rejected` | claim | < ₹100 rejected |
| `test_amount_above_maximum_rejected` | claim | > ₹1 crore rejected |
| `test_amount_at_minimum_accepted` | claim | ₹100 boundary — inclusive |
| `test_amount_at_maximum_accepted` | claim | ₹1 crore boundary — inclusive |
| `test_invalid_claim_type_rejected` | claim | Only cashless/reimbursement valid |
| `test_reimbursement_accepted` | claim | reimbursement is valid |
| `test_empty_hospital_id_rejected` | claim | Required field enforced |
| `test_short_diagnosis_code_rejected` | claim | Min 3 chars for ICD-10 |
| `test_multiple_errors_returned_at_once` | claim | Collects ALL errors, not just first |
| `test_wrong_amount_threshold_bug` | claim | GitLab CI demo — catches the bug |
| `test_eligible_cashless_claim` | eligibility | Happy path — empanelled + sufficient limit |
| `test_hospital_not_empanelled` | eligibility | Cashless rejected + no policy fetch |
| `test_member_policy_not_found` | eligibility | Cache miss → reject (fail safe) |
| `test_insufficient_limit` | eligibility | Amount > remaining → reject with remaining |
| `test_exact_remaining_limit_is_eligible` | eligibility | Boundary — claim = remaining → approve |
| `test_reimbursement_skips_empanelment_check` | eligibility | Reimbursement bypasses Set lookup |
| `test_redis_error_on_policy_fetch` | eligibility | Redis down → fail safe, not crash |
| `test_result_to_dict_contains_decision_key` | eligibility | Output dict has `decision` key |
| `test_normal_claim_is_clear` | fraud | Baseline → CLEAR, score < 50 |
| `test_high_amount_increases_score` | fraud | ≥ ₹5L → +20 points, reason in list |
| `test_round_number_increases_score` | fraud | Round ₹ → +5 points |
| `test_daily_limit_excess_increases_score` | fraud | > 5 claims/day → +30 points |
| `test_high_amount_and_daily_excess_flags_claim` | fraud | Two rules combined → FRAUD_REVIEW |
| `test_hospital_score_updated_on_every_claim` | fraud | ZINCRBY called with right key + hospital |
| `test_assessment_to_dict_contains_verdict` | fraud | Output has `verdict` field |
| `test_redis_error_on_incr_does_not_crash` | fraud | Redis error → continue, don't crash |
| `test_get_top_risk_hospitals` | fraud | ZREVRANGE returns descending order |
| `test_clear_verdict_in_dict` | fraud | CLEAR verdict in dict |
| `test_flagged_verdict_in_dict` | fraud | FRAUD_REVIEW verdict in dict |

**Total: 39 unit tests across 3 services**

### What is NOT unit tested (tested manually or by E2E)
- Kafka producer actually publishing to a broker
- FastAPI HTTP layer (route matching, request parsing)
- Consumer loop reading from Kafka
- APISIX auth and rate limiting
- Redis persistence (RDB/AOF)
- Cross-service data flow
