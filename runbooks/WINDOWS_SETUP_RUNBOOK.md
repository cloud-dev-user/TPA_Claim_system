# HealthOne TPA Claims Processing Lab
## Windows Environment Setup Runbook
**Version:** v1.0 | **Date:** April 2026 | **Platform:** Windows 10 / 11 (64-bit)

---

## Table of Contents
1. [Overview & Architecture](#1-overview--architecture)
2. [System Requirements](#2-system-requirements)
3. [Install Prerequisites](#3-install-prerequisites)
4. [Clone the Repository](#4-clone-the-repository)
5. [Generate TLS Certificate](#5-generate-tls-certificate)
6. [Start the Stack](#6-start-the-stack)
7. [Configure APISIX Gateway](#7-configure-apisix-gateway)
8. [Post-Installation Verification](#8-post-installation-verification)
9. [Access URLs & Credentials](#9-access-urls--credentials)
10. [API Keys & Test Data Reference](#10-api-keys--test-data-reference)
11. [Port Reference](#11-port-reference)
12. [Stop / Restart / Reset](#12-stop--restart--reset)

---

## 1. Overview & Architecture

This runbook sets up a complete Java microservices lab on a Windows laptop using Docker Desktop.  
**No prior configuration is assumed** — every tool is installed from scratch.

### Stack Components

| Service | Technology | Role |
|---|---|---|
| claim-service (×2) | Java 17 + Spring Boot 3.2 | REST API — accepts claim submissions |
| eligibility-service | Java 17 + Spring Boot 3.2 | Kafka consumer — checks member policy |
| fraud-service | Java 17 + Spring Boot 3.2 | Kafka consumer — applies fraud rules |
| Kafka + Zookeeper | Bitnami Kafka 3.6 | Async message broker |
| Redis 7.2 | Redis Alpine | Cache + state store |
| PostgreSQL 16 | Postgres Alpine | Persistent claim storage |
| APISIX 3.8 | Apache APISIX | API Gateway — HTTPS, auth, rate limiting |
| Prometheus + Grafana | prom/prometheus + Grafana 10 | Metrics & dashboards |
| Kafka UI | kafbat/kafka-ui | Topic/message browser |
| RedisInsight | redis/redisinsight | Redis data browser |

### Request Flow

```
Hospital Client
   │
   ▼  HTTPS port 9443  [API key auth + rate limiting]
APISIX Gateway
   │
   ▼  HTTP (internal Docker network)
claim-service-1  OR  claim-service-2   ← round-robin load balancing
   │
   ├─ Validate request
   ├─ Save claim to PostgreSQL
   ├─ Cache status in Redis  (key: claim:{claimId}:status, TTL 24h)
   └─ Publish to Kafka topic: claim-events
              │
              ├──▶  eligibility-service
              │        └─ Check Redis member policy
              │        └─ Publish to: eligibility-results + audit-log
              │
              └──▶  fraud-service
                       └─ Apply fraud rules (Redis counters + sorted sets)
                       └─ Publish to: fraud-alerts + audit-log
```

---

## 2. System Requirements

| Component | Minimum | Recommended |
|---|---|---|
| OS | Windows 10 64-bit (Build 19041+) | Windows 11 64-bit |
| RAM | 8 GB | 16 GB |
| Disk Space | 20 GB free | 40 GB free |
| CPU | 4 cores | 8 cores |
| Internet | Required (Docker image pulls) | Broadband |
| Privileges | Administrator account | Administrator account |

> ⚠️ **WSL2 required:** Docker Desktop uses WSL2 (Windows Subsystem for Linux 2). It is enabled by default on Windows 11. On Windows 10 the Docker installer will prompt you to enable it.

---

## 3. Install Prerequisites

> ⚠️ Install **all tools** below before cloning the repository or running Docker.

---

### 3.1 Git for Windows

Git provides the **Git Bash** terminal used for all commands in this guide.

1. Open browser → go to: **https://git-scm.com/download/win**
2. Download the 64-bit installer (auto-detected)
3. Run the installer with these settings:
   - **Default editor:** keep default or choose Notepad++
   - **PATH environment:** select `Git from the command line and also 3rd-party software`
   - **Line endings:** `Checkout Windows-style, commit Unix-style line endings`
   - **Terminal emulator:** `Use MinTTY (the default terminal of MSYS2)`
   - Click **Install** → **Finish**
4. Open **Git Bash**: Start Menu → search `Git Bash`
5. Verify:
   ```bash
   git --version
   ```
   Expected: `git version 2.x.x.windows.x`

---

### 3.2 Docker Desktop

1. Open browser → go to: **https://www.docker.com/products/docker-desktop/**
2. Click **Download for Windows** → run the installer
3. On configuration screen: ensure **"Use WSL 2 instead of Hyper-V"** is checked
4. Click **OK** → installation starts (may require a restart)
5. After restart: Docker Desktop launches automatically (system tray icon)
6. Wait until the Docker Desktop status shows **"Engine running"** (green icon in tray)
7. Open Git Bash and verify:
   ```bash
   docker --version
   docker compose version
   ```
   Expected:
   ```
   Docker version 24.x.x, build xxxxxxx
   Docker Compose version v2.x.x
   ```

> ⚠️ If Docker Desktop shows **"WSL2 installation incomplete"**: open PowerShell as Administrator and run `wsl --update`, then restart Docker Desktop.

---

### 3.3 Python 3

Required by the APISIX setup script to safely generate JSON payloads.

1. Open browser → go to: **https://www.python.org/downloads/windows/**
2. Click **"Download Python 3.x.x"** (latest stable)
3. Run installer — **IMPORTANT:** check **"Add Python to PATH"** at the bottom before clicking Install
4. Click **Install Now**
5. Open a **new** Git Bash window and verify:
   ```bash
   python3 --version
   ```
   Expected: `Python 3.x.x`

> ⚠️ You must open a **new** Git Bash window after installing Python for the PATH to take effect.

---

### 3.4 Verify OpenSSL (bundled with Git)

OpenSSL is included with Git for Windows. Just verify it is accessible:

```bash
openssl version
```
Expected: `OpenSSL 3.x.x`

> ℹ️ If not found: reinstall Git for Windows and ensure the "Git Bash Here" shell integration is selected.

---

### 3.5 Verify curl (bundled with Git Bash)

```bash
curl --version
```
Expected: `curl 8.x.x ...`

---

> ✅ **Java and Maven are NOT required** — services are compiled inside Docker containers using a multi-stage build.

---

## 4. Clone the Repository

> All commands from this point forward are run in **Git Bash**.

**Step 1 — Navigate to your working directory:**
```bash
cd ~/Documents
```

**Step 2 — Clone the repository:**
```bash
git clone <REPOSITORY_URL> MD_India_Training
```
> Replace `<REPOSITORY_URL>` with the URL provided by your instructor.

**Step 3 — Enter the project directory:**
```bash
cd MD_India_Training/Training_code
```

**Step 4 — Verify the structure:**
```bash
ls
```
Expected output includes:
```
apisix/               claim-service-java/     certs/
docker-compose.yml    eligibility-service-java/
fraud-service-java/   grafana/                prometheus/
runbooks/             PROJECT_GUIDE.md
```

> ⚠️ **All subsequent commands must be run from inside `Training_code/`.**  
> If you open a new terminal, always run: `cd ~/Documents/MD_India_Training/Training_code`

---

## 5. Generate TLS Certificate

The lab uses HTTPS. A self-signed certificate must be generated before starting Docker.

**Step 1 — Generate the certificate:**
```bash
bash certs/generate-certs.sh
```
Expected output:
```
Certificate generated:
    Subject: CN=localhost, O=HealthOne TPA, ...
    DNS:localhost, DNS:healthonetpa.local, IP Address:127.0.0.1
    Not After: (date ~2 years from today)
Files: .../certs/server.crt  .../certs/server.key
```

**Step 2 — Trust the certificate in Windows** _(removes browser security warning)_

1. Open **Windows Explorer** → navigate to the `certs/` folder inside the project
2. Double-click **`server.crt`**
3. Click **"Install Certificate"**
4. Select **"Local Machine"** → click **Next**
5. Select **"Place all certificates in the following store"**
6. Click **Browse** → select **"Trusted Root Certification Authorities"** → click **OK**
7. Click **Next** → **Finish** → click **Yes** on the security prompt
8. **Restart your browser** (Chrome / Edge / Firefox)

> ℹ️ Without this step your browser shows a certificate warning. `curl -k` bypasses the warning automatically — you only need to trust for the browser.

---

## 6. Start the Stack

> ⚠️ Ensure Docker Desktop is running (green icon in system tray) before proceeding.

**Step 1 — Build and start all containers:**
```bash
docker compose up --build
```

- **First run:** 5–15 minutes (Maven downloads dependencies and compiles Java inside Docker)
- **Subsequent runs:** 1–2 minutes (Docker layer cache reused)
- Leave this terminal open — all logs stream here

---

### Expected Startup Sequence

| Order | Container | What it does | Completes when |
|---|---|---|---|
| 1 | zookeeper | Starts Zookeeper coordination | Health check passes |
| 2 | kafka | Starts Kafka broker | Health check passes |
| 3 | kafka-init | Creates 5 Kafka topics | Exits with code 0 ✓ |
| 4 | redis | Starts Redis with password auth | Health check passes |
| 5 | redis-init | Seeds member policies + hospital data | Exits with code 0 ✓ |
| 6 | postgres | Starts PostgreSQL | Health check passes |
| 7 | etcd | Starts etcd (APISIX config store) | Health check passes |
| 8 | apisix | Starts API Gateway | Health check passes |
| 9 | claim-service-1/2 | Builds + starts REST API | Health check passes |
| 10 | eligibility-service | Starts Kafka consumer | Connected to broker |
| 11 | fraud-service | Starts Kafka consumer | Connected to broker |
| 12 | prometheus / grafana | Starts observability stack | Running |
| 13 | kafka-ui / redisinsight | Starts dev tools | Running |

> ℹ️ `kafka-init` and `redis-init` will show **`Exited (0)`** in `docker compose ps` — this is **correct**. They are one-shot init containers that run once and exit.

---

**Step 2 — Verify containers in a new Git Bash window:**
```bash
docker compose ps
```
All Java services should show **healthy**. Init containers show **Exited (0)**.

**Step 3 — Wait for claim-service to be ready:**
```bash
curl http://localhost:8080/health
```
Expected: `{"status":"UP","service":"claim-service"}`

---

## 7. Configure APISIX Gateway

APISIX routes, API key consumers, TLS certificate, and HTTPS tool routes are configured by a single setup script. Run it once after the stack is up.

```bash
bash apisix/setup-apisix.sh
```

Expected output:
```
[OK]  APISIX Admin API is ready (HTTP 200)
[OK]  TLS certificate registered (valid for localhost, healthonetpa.local)
[OK]  Upstream created
[OK]  Route created: POST /api/v1/claims
[OK]  Route created: GET /api/v1/claims/*
[OK]  Consumer created: hospital_apollo
[OK]  Consumer created: hospital_fortis
[OK]  Consumer created: hospital_max
[OK]  Consumer created: hospital_kokilaben
[OK]  Consumer created: hospital_generic
[OK]  Tool route: https://localhost:9443/grafana/*
[OK]  Tool route: https://localhost:9443/prometheus/*
[OK]  Tool route: https://localhost:9443/kafka-ui*
[OK]  Tool route: https://localhost:9443/swagger-ui*
[OK]  Tool route: https://localhost:9443/redisinsight/
[OK]  Prometheus metrics enabled on all routes
```

> ℹ️ This script is **idempotent** — safe to run multiple times (uses PUT, not POST).  
> ⚠️ If any step shows `[ERR]`, wait 30 seconds and re-run — APISIX may still be initialising.

---

## 8. Post-Installation Verification

Run all tests below to confirm the environment is fully operational.

---

### T01 — Claim service health (direct)
```bash
curl http://localhost:8080/health
```
✅ Expected: `{"status":"UP","service":"claim-service"}`

---

### T02 — APISIX HTTPS reachability
```bash
curl -k https://localhost:9443/
```
✅ Expected: `{"error_msg":"404 Route Not Found"}`
> ℹ️ 404 from APISIX root is **correct** — it confirms HTTPS/TLS is working. No route is configured for `/`.

---

### T03 — Authentication check (no API key → should fail)
```bash
curl -k -X POST https://localhost:9443/api/v1/claims \
  -H "Content-Type: application/json" \
  -d '{"test":1}'
```
✅ Expected: `{"message":"Missing API key"}` or `{"message":"Unauthorized"}`

---

### T04 — Submit a valid claim (expect HTTP 202)
```bash
curl -k -X POST https://localhost:9443/api/v1/claims \
  -H "apikey: APOLLO-KEY-2026" \
  -H "Content-Type: application/json" \
  -d '{
    "memberId":     "M1001",
    "hospitalId":   "H5501-Apollo",
    "hospitalName": "Apollo",
    "insurer":      "StarHealth",
    "amount":       85000,
    "city":         "mumbai",
    "claimType":    "cashless",
    "diagnosisCode":"Z51.1"
  }'
```
✅ Expected (HTTP 202):
```json
{
  "claimId": "CXXXXXXXX",
  "status": "SUBMITTED",
  "memberId": "M1001",
  "insurer": "StarHealth",
  "amount": 85000.0,
  "submittedAt": "2026-04-xx...",
  "message": "Claim submitted successfully"
}
```
> ℹ️ Save the `claimId` value for T05.

---

### T05 — Check claim status
```bash
curl -k https://localhost:9443/api/v1/claims/CXXXXXXXX \
  -H "apikey: APOLLO-KEY-2026"
```
✅ Expected: `{"claimId":"CXXXXXXXX","status":"SUBMITTED"}`
> Replace `CXXXXXXXX` with the `claimId` from T04.

---

### T06 — Kafka topics exist
```bash
docker exec healthone-kafka /opt/bitnami/kafka/bin/kafka-topics.sh \
  --list --bootstrap-server localhost:9092
```
✅ Expected output includes:
```
audit-log
claim-events
claim-events.DLT
eligibility-results
fraud-alerts
```

---

### T07 — Redis member data seeded
```bash
docker exec healthone-redis redis-cli -a healthone-tpa-redis-2026 HGETALL member:M1001:policy
```
✅ Expected:
```
name
Anita Sharma
plan
Gold Family Floater
limit
1000000
used
230000
```

---

### T08 — Claim persisted in PostgreSQL
```bash
docker exec healthone-postgres psql -U healthonetpa -d healthonetpa \
  -c "SELECT claim_id, status, amount FROM claims LIMIT 5;"
```
✅ Expected: Table with a row showing the claimId from T04.

---

### T09 — Swagger UI accessible
```bash
curl -sk -o /dev/null -w "%{http_code}" \
  https://localhost:9443/swagger-ui/index.html
```
✅ Expected: `200`

Then open in browser: **https://localhost:9443/swagger-ui/index.html**

---

### T10 — Grafana accessible
```bash
curl -sk -o /dev/null -w "%{http_code}" \
  https://localhost:9443/grafana/login
```
✅ Expected: `200`

Then open in browser: **https://localhost:9443/grafana/** → login: `admin / healthonetpa2026`

---

### T11 — Kafka UI accessible
Open in browser: **https://localhost:9443/kafka-ui/**
✅ Expected: Kafka UI showing cluster `healthone-tpa-local` with 5 topics.

---

## 9. Access URLs & Credentials

| Tool | HTTPS URL (via APISIX) | Credentials |
|---|---|---|
| **API Gateway** | https://localhost:9443/api/v1/claims | `apikey` header (see Section 10) |
| **Swagger UI** | https://localhost:9443/swagger-ui/index.html | None |
| **Grafana** | https://localhost:9443/grafana/ | admin / healthonetpa2026 |
| **Kafka UI** | https://localhost:9443/kafka-ui/ | None |
| **Prometheus** | https://localhost:9443/prometheus/ | None |
| **RedisInsight** | https://localhost:9443/redisinsight/ | None |
| **APISIX Admin UI** | http://localhost:9000 | admin / healthonetpaadmin |

---

## 10. API Keys & Test Data Reference

### API Keys (send in `apikey` request header)

| Hospital | API Key | Rate Limit |
|---|---|---|
| Apollo Hospitals | `APOLLO-KEY-2026` | 200 req/min |
| Fortis Healthcare | `FORTIS-KEY-2026` | 50 req/min |
| Max Healthcare | `MAX-KEY-2026` | 50 req/min |
| Kokilaben Hospital | `KOKI-KEY-2026` | 50 req/min |
| Generic / Other | `GENERIC-KEY-2026` | 20 req/min |

### Pre-Seeded Member Policies (Redis)

| Member ID | Name | Plan | Policy Limit | Used |
|---|---|---|---|---|
| M1001 | Anita Sharma | Gold Family Floater | ₹10,00,000 | ₹2,30,000 |
| M1002 | Rajesh Kumar | Silver Individual | ₹5,00,000 | ₹50,000 |
| M1003 | Priya Patel | Platinum Family | ₹20,00,000 | ₹0 |

### Empanelled Hospitals by City

| City | Hospitals (cashless claims) |
|---|---|
| mumbai | Apollo, Fortis, Kokilaben, Max, Lilavati, Breach_Candy |
| delhi | Apollo, Fortis, Max, AIIMS_Private, Medanta |
| bangalore | Apollo, Fortis, Manipal, Narayana, Sakra |
| chennai | Apollo, Fortis, MIOT, Gleneagles, Sri_Ramachandra |

### Fraud Detection Thresholds

| Rule | Condition | Score Added |
|---|---|---|
| High amount | Amount ≥ ₹5,00,000 | +20 |
| Round number | Amount is multiple of ₹1,00,000 | +5 |
| Daily volume | Same member >5 claims today | +30 |
| **Flag threshold** | **Total score ≥ 50** | **→ FRAUD_REVIEW** |

---

## 11. Port Reference

| Port | Container | Description | Recommendation |
|---|---|---|---|
| **9443** | APISIX | HTTPS API Gateway | ✅ Use this |
| 9080 | APISIX | HTTP Gateway | Use 9443 instead |
| 9180 | APISIX | Admin API | Script/internal use |
| 8080 | claim-service-1 | REST API (direct) | Bypass gateway |
| 8081 | claim-service-2 | REST API instance 2 | Bypass gateway |
| 9092 | Kafka | Internal broker | Container-internal |
| 9093 | Kafka | External broker | Local dev only |
| 6379 | Redis | Cache (password protected) | Internal |
| 5432 | PostgreSQL | Database | Internal |
| 3000 | Grafana | HTTP direct | Use 9443/grafana/ |
| 9090 | Prometheus | HTTP direct | Use 9443/prometheus/ |
| 8090 | Kafka UI | HTTP direct | Use 9443/kafka-ui/ |
| 5540 | RedisInsight | HTTP direct | Use 9443/redisinsight/ |
| 9000 | APISIX Dashboard | Admin web UI | HTTP only |
| 2181 | Zookeeper | Coordination | Internal |
| 2379 | etcd | APISIX config store | Internal |

---

## 12. Stop / Restart / Reset

### Stop all containers (keep data)
```bash
docker compose stop
```
Containers stop, volumes preserved. Restart with `docker compose start`.

### Stop and remove containers (keep data)
```bash
docker compose down
```
Containers and networks removed. Data in volumes is preserved.

### Full reset — remove everything including data
```bash
docker compose down -v
```
> ⚠️ **WARNING:** Deletes all PostgreSQL claims, Redis data, and Kafka messages. Use only to start completely fresh.

### Restart after stop
```bash
docker compose up
```
Images already built — starts in ~1–2 minutes.  
After restart, re-run APISIX setup: `bash apisix/setup-apisix.sh`

### Rebuild Java services only (after code change)
```bash
docker compose up --build claim-service-1 claim-service-2
```

### View logs for a specific service
```bash
docker logs healthone-claim-service-1 --tail 50 -f
```
Replace `healthone-claim-service-1` with any container name from `docker compose ps`.

### Check status of all containers
```bash
docker compose ps
```

---

*HealthOne TPA Training — Internal Use Only | April 2026*
