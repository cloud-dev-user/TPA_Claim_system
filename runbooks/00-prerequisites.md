# Runbook 00 — Prerequisites & Local Setup

> **Time required:** 30–45 minutes  
> **Do this BEFORE Day 1 (April 24)**

---

## What You Are Setting Up

A fully local environment that mirrors the HealthOne TPA production stack:

| Component | Purpose | Port |
|-----------|---------|------|
| Apache Kafka + Zookeeper | Event streaming backbone | 9092 / 9093 |
| Redis | Caching & fraud scoring | 6379 |
| Apache APISIX | API Gateway | 9080 (gateway), 9180 (admin) |
| APISIX Dashboard | Visual management UI | 9000 |
| claim-service (×2) | Claims REST API | 8080, 8081 |
| eligibility-service | Eligibility consumer | — |
| fraud-service | Fraud detection consumer | — |

---

## 1. Install Prerequisites

### Docker Desktop
Download and install Docker Desktop for your OS:
- **Windows**: Docker Desktop ≥ 4.25 (requires WSL 2)
- **macOS**: Docker Desktop ≥ 4.25
- **Linux**: Docker Engine ≥ 24.x + Docker Compose plugin v2

Verify after install:
```bash
docker --version          # Docker version 24.x or higher
docker compose version    # Docker Compose version v2.x or higher
```

> **Windows users**: Ensure WSL 2 is enabled.  
> Run in PowerShell as Admin: `wsl --install`

### Git
```bash
git --version   # git version 2.x or higher
```
Download from: https://git-scm.com/downloads

### Python 3.12 (for running tests locally)
```bash
python --version   # Python 3.12.x
```
Download from: https://www.python.org/downloads/

### curl
Used for APISIX testing. Pre-installed on macOS/Linux.  
Windows: comes with Git Bash or install via `winget install curl.curl`

---

## 2. Clone / Copy the Training Code

> Your trainer will provide access to the GitLab group `healthone-tpa-claims`.  
> For Day 1, use the local copy at your workstation.

```bash
# Navigate to your working folder
cd ~/training   # or wherever you prefer

# The project structure should look like:
Training_code/
├── docker-compose.yml
├── Makefile
├── claim-service/
├── eligibility-service/
├── fraud-service/
├── apisix/
└── runbooks/
```

---

## 3. System Resource Check

Docker needs sufficient resources. Set in Docker Desktop → Settings → Resources:

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| CPU | 4 cores | 6 cores |
| Memory | 6 GB | 8 GB |
| Disk | 20 GB | 30 GB |

---

## 4. Start the Full Stack

```bash
cd Training_code

# Build images and start all containers
docker compose up -d --build

# Watch startup progress (Ctrl+C to stop watching)
docker compose logs -f
```

Expected startup sequence:
1. `zookeeper` → healthy (10–15 sec)
2. `kafka` → healthy (30–45 sec)
3. `kafka-init` → creates topics, exits with code 0
4. `redis` → healthy (5 sec)
5. `redis-init` → seeds data, exits with code 0
6. `etcd` → healthy (10 sec)
7. `apisix` → healthy (20–30 sec)
8. `claim-service-1`, `claim-service-2` → healthy
9. `eligibility-service`, `fraud-service` → running

---

## 5. Verify Everything Is Running

```bash
docker compose ps
```

All containers should show `healthy` or `running`. Then run:

```bash
# Kafka: list topics
docker exec healthone-kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
# Expected output:
# audit-log
# claim-events
# eligibility-results
# fraud-alerts

# Redis: ping
docker exec healthone-redis redis-cli PING
# Expected: PONG

# Redis: check seed data
docker exec healthone-redis redis-cli HGETALL member:M1001:policy

# Claim service health
curl http://localhost:8080/health
# Expected: {"status":"ok","instance":"claim-service-1"}

# APISIX admin API
curl http://localhost:9180/apisix/admin/routes \
  -H 'X-API-KEY: edd1c9f034335f136f87ad84b625c8f1'
```

---

## 6. Configure APISIX Routes

```bash
bash apisix/setup-apisix.sh
```

This creates:
- Upstream pointing to both claim-service instances
- Route: `POST /api/v1/claims`
- Consumers: hospital-apollo (200/min), hospital-fortis/max/kokilaben (50/min), hospital-generic (20/min)
- Prometheus metrics enabled

---

## 7. End-to-End Smoke Test

```bash
curl -s -X POST http://localhost:9080/api/v1/claims \
  -H "apikey: APOLLO-KEY-2026" \
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

Expected response (HTTP 202):
```json
{
  "claim_id": "CXXXXXXXX",
  "status": "SUBMITTED",
  "member_id": "M1001",
  "insurer": "StarHealth",
  "amount": 85000.0,
  "submitted_at": "2026-04-24T...",
  "message": "Claim received and queued for processing"
}
```

---

## 8. Useful URLs

| Service | URL |
|---------|-----|
| Claim Service API docs | http://localhost:8080/docs |
| APISIX Dashboard | http://localhost:9000 (admin / healthonetpaadmin) |
| APISIX Prometheus metrics | http://localhost:9091/apisix/prometheus/metrics |

---

## Troubleshooting

**Kafka keeps restarting**  
→ Check Zookeeper is healthy first: `docker compose logs zookeeper`  
→ Ensure Docker has ≥ 6 GB RAM allocated

**APISIX exits immediately**  
→ etcd must be healthy first: `docker compose logs etcd`  
→ Check `apisix/config.yaml` formatting (YAML is whitespace-sensitive)

**Port already in use**  
→ Find and kill the conflicting process:
```bash
# Linux/macOS
lsof -i :9080
# Windows
netstat -ano | findstr :9080
```

**Redis seed data missing**  
→ Re-run the init container: `docker compose restart redis-init`

**Clean restart (nuclear option)**  
```bash
docker compose down -v   # deletes all volumes
docker compose up -d --build
```
