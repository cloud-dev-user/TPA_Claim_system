# HealthOne TPA Training Stack — Troubleshooting Guide

> All issues documented here were encountered and resolved while building the HealthOne TPA Claims Processing Pipeline from Python FastAPI → Java Spring Boot 3.2 on Windows.

---

## Table of Contents

1. [Kafka UI Image Pull Failure](#issue-01)
2. [APISIX "Missing Related Consumer" Error](#issue-02)
3. [UNKNOWN_TOPIC_OR_PARTITION Continuous Warnings](#issue-03)
4. [kafka-init / redis-init Commands Not Executing](#issue-04)
5. [Redis Healthcheck Failing After Password Added](#issue-05)
6. [Claim Submission Fails — Snappy Native Library](#issue-06)
7. [APISIX Setup Script Silently Stopping](#issue-07)
8. [SSL / HTTPS Not Working](#issue-08)
9. [Grafana Redirect Loop via HTTPS](#issue-09)
10. [Prometheus 302 Redirect via HTTPS](#issue-10)
11. [Swagger UI Whitelabel 404 Error](#issue-11)

---

## Issue 01 — Kafka UI Image Pull Failure {#issue-01}

### Symptoms

```
Error response from daemon: pull access denied for provectus/kafka-ui
```

Docker Compose fails to start `kafka-ui` service.

### Root Cause

The `provectus/kafka-ui` Docker Hub image is no longer maintained. The project migrated to a new GitHub organization (`kafbat`) and the Docker Hub repository was abandoned/privatized.

### Resolution

**File:** `docker-compose.yml`

```yaml
# BEFORE (broken)
kafka-ui:
  image: provectus/kafka-ui:latest

# AFTER (working)
kafka-ui:
  image: ghcr.io/kafbat/kafka-ui:latest
```

The `ghcr.io/kafbat/kafka-ui:latest` image is the official successor and is publicly available on GitHub Container Registry.

---

## Issue 02 — APISIX "Missing Related Consumer" {#issue-02}

### Symptoms

```json
{"message": "Missing related consumer"}
```

Returned on every API call even after running `setup-apisix.sh` successfully (no errors in script output).

### Root Cause

**Cause A — Script not yet run:** APISIX routes require consumers to exist. If `setup-apisix.sh` was not run after `docker compose up`, no consumers exist.

**Cause B — Hyphen in consumer username:** APISIX validates consumer usernames with regex `^[a-zA-Z0-9_]+$`. Consumer names containing hyphens (e.g., `hospital-apollo`) are silently rejected with a 400 error, leaving the consumer uncreated.

**Cause C — Malformed JSON payload:** When building JSON with bash heredocs or complex string concatenation, nested quotes in fields like `rejected_msg` caused malformed JSON. The `curl -sf` flag caused failures to be swallowed silently.

### Resolution

**Step 1:** Rename all consumers — replace hyphens with underscores.

```bash
# BEFORE (rejected by APISIX)
username="hospital-apollo"

# AFTER (accepted)
username="hospital_apollo"
```

**Step 2:** Build JSON using Python to avoid bash quoting issues.

```bash
# BEFORE (fragile bash string)
CONSUMER_JSON='{"username":"hospital_apollo","plugins":{"key-auth":{"key":"hosp-apollo-key-2026"}}}'

# AFTER (reliable python3 generation)
CONSUMER_JSON=$(python3 -c "
import json
print(json.dumps({
    'username': 'hospital_apollo',
    'plugins': {
        'key-auth': {'key': 'hosp-apollo-key-2026'}
    }
}))
")
```

**Step 3:** Replace `curl -sf` with error-checking curl.

```bash
# BEFORE (silent failures)
curl -sf -X PUT ...

# AFTER (detects APISIX errors)
RESPONSE=$(curl -s -X PUT ...)
if echo "$RESPONSE" | grep -q '"error_msg"'; then
    echo "ERROR: $RESPONSE"
fi
```

**Verification:**

```bash
# Re-run the setup script
docker exec healthone-apisix sh -c 'cd /usr/local/apisix && /bin/bash /dev/stdin' < apisix/setup-apisix.sh

# Confirm consumers exist
curl -s http://localhost:9180/apisix/admin/consumers \
  -H "X-API-KEY: edd1c9f034335f136f87ad84b625c8f1" | python3 -m json.tool
```

---

## Issue 03 — UNKNOWN_TOPIC_OR_PARTITION Continuous Warnings {#issue-03}

### Symptoms

Java services flood logs with repeated errors:

```
WARN  org.apache.kafka.clients.NetworkClient - [Consumer ...] Error while fetching metadata with correlation id X:
{claim-events=UNKNOWN_TOPIC_OR_PARTITION}
```

Services start but cannot connect to topics.

### Root Cause

**Startup ordering:** Java services were only waiting for the Kafka broker to be healthy (`condition: service_healthy`). The broker being healthy means it can accept connections — it does **not** mean the topics have been created. `kafka-init` (the one-shot topic creator) runs after the broker is healthy, but the Java services were starting concurrently with `kafka-init`.

### Resolution

Add `kafka-init: condition: service_completed_successfully` to all services that consume or produce to Kafka topics.

**File:** `docker-compose.yml`

```yaml
# Add to depends_on for claim-service-1, claim-service-2, eligibility-service, fraud-service
depends_on:
  kafka:
    condition: service_healthy
  kafka-init:
    condition: service_completed_successfully   # ← ADD THIS
  redis:
    condition: service_healthy
```

Also remove `restart: on-failure` from `kafka-init` — it should run once and exit cleanly. If it restarts, its exit code will confuse `service_completed_successfully`.

---

## Issue 04 — kafka-init / redis-init Commands Not Executing {#issue-04}

### Symptoms

`kafka-init` container exits with code 0 but **no topics are created**. Docker logs show no output from the `kafka-topics.sh` commands. Same issue for `redis-init` — Redis has no seed data despite the container appearing to succeed.

### Root Cause

YAML `command` block with surrounding quotes wraps the entire script as a **single string argument** passed to the shell. Bash receives a literal multi-line string as `$1` rather than executing the commands inside it.

```yaml
# BROKEN — the entire block is one quoted string
command:
  - /bin/bash
  - -c
  - "
    echo 'Creating topics...'
    /opt/bitnami/kafka/bin/kafka-topics.sh ...
    "
```

Additionally, the short-form binary path `kafka-topics.sh` without the full path fails inside Docker because `/opt/bitnami/kafka/bin` is not in the container's `$PATH`.

### Resolution

Use list-form `command` with the script as an unquoted YAML block scalar (`|`), and use the full absolute binary path.

```yaml
# WORKING
command:
  - /bin/bash
  - -c
  - |
    echo 'Creating Kafka topics...'
    /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
      --create --if-not-exists --topic claim-events \
      --partitions 3 --replication-factor 1
    echo 'Topics created.'
    /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
```

Same fix applied to `redis-init`:

```yaml
command:
  - /bin/sh
  - -c
  - |
    echo 'Seeding Redis...'
    redis-cli -h redis -a healthone-tpa-redis-2026 HSET member:M1001:policy ...
    echo 'Done.'
```

---

## Issue 05 — Redis Healthcheck Fails After Adding Password {#issue-05}

### Symptoms

```
healthone-redis  | NOAUTH Authentication required.
```

Redis container stuck in `unhealthy` state. All services that depend on `redis: service_healthy` never start.

### Root Cause

After adding `--requirepass healthone-tpa-redis-2026` to the Redis `command`, the healthcheck still used unauthenticated `redis-cli ping`. Redis rejects the ping with `NOAUTH`.

### Resolution

**File:** `docker-compose.yml` — add `-a <password>` flag to the healthcheck command.

```yaml
redis:
  command: >
    redis-server
    --requirepass healthone-tpa-redis-2026
    ...
  healthcheck:
    # BEFORE (broken after adding password)
    test: ["CMD", "redis-cli", "ping"]

    # AFTER (authenticated)
    test: ["CMD", "redis-cli", "-a", "healthone-tpa-redis-2026", "ping"]
    interval: 5s
    timeout: 3s
    retries: 5
```

---

## Issue 06 — Claim Submission Fails — Snappy Native Library {#issue-06}

### Symptoms

```json
{"error": "Failed to publish claim event"}
```

Spring Boot logs show:

```
ERROR c.m.claim.kafka.ClaimEventProducer - Failed to send Kafka message
java.lang.UnsatisfiedLinkError: SnappyLoader.loadNativeLibrary ... libsnappy
```

### Root Cause

Kafka producer was configured with `compression.type: snappy`. Snappy compression requires a native `.so` shared library (`libsnappy`). The production Docker image uses `eclipse-temurin:17-jre-alpine` — Alpine Linux's minimal filesystem does not include `libsnappy`.

LZ4 compression is implemented in pure Java and has no native dependency.

### Resolution

**File:** `claim-service-java/src/main/resources/application.yml`

```yaml
spring:
  kafka:
    producer:
      properties:
        # BEFORE (requires native library — broken on Alpine)
        compression.type: snappy

        # AFTER (pure Java — works everywhere)
        compression.type: lz4
```

No change needed to `pom.xml`. The `kafka-clients` library bundles LZ4 support. LZ4 provides comparable compression ratios to Snappy with similar performance.

---

## Issue 07 — APISIX Setup Script Silently Stopping {#issue-07}

### Symptoms

`setup-apisix.sh` output stops partway through without any error message. Consumer creation appears to run, but subsequent route creation is never attempted. The `set -euo pipefail` shebang is present.

### Root Cause

`set -euo pipefail` causes bash to immediately exit on any non-zero exit code. `curl -sf` returns exit code 22 on HTTP 4xx responses. When APISIX rejects a consumer (due to invalid username with hyphens — see Issue 02), `curl -sf` exits with code 22, and `set -e` kills the entire script silently.

### Resolution

Remove `curl -sf` for APISIX calls. Instead capture the response body and grep for the `error_msg` field in the JSON response.

```bash
# BEFORE (silent script death)
curl -sf -X PUT "http://localhost:9180/apisix/admin/consumers" \
  -H "X-API-KEY: $ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d "$CONSUMER_JSON"

# AFTER (explicit error detection)
RESPONSE=$(curl -s -X PUT "http://localhost:9180/apisix/admin/consumers" \
  -H "X-API-KEY: $ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d "$CONSUMER_JSON")

if echo "$RESPONSE" | grep -q '"error_msg"'; then
    echo "  ERROR creating consumer: $RESPONSE"
else
    echo "  OK"
fi
```

---

## Issue 08 — SSL / HTTPS Not Working {#issue-08}

### Symptoms

```bash
curl -k https://localhost:9443/api/v1/claims
# Returns: {"error_msg":"404 Route Not Found"}

curl https://localhost:9443/api/v1/claims
# Returns: SSL handshake error / certificate verify failed
```

APISIX is running but HTTPS routes return 404.

### Root Cause

**Cause A — Certificate not uploaded to APISIX:** APISIX requires the TLS certificate to be registered via its Admin API. Simply mounting `certs/` into the container is not sufficient — APISIX must be told about the certificate via a `PUT /apisix/admin/ssls` call.

**Cause B — setup-apisix.sh not run (or silently failed):** Routes are defined via the Admin API, not config files. If the script failed or was not run, no routes exist.

### Resolution

**Step 1:** Generate the self-signed certificate with SAN extensions (required by modern browsers/curl).

```bash
# Run from project root
openssl req -x509 -newkey rsa:4096 -sha256 -days 825 \
  -nodes -keyout certs/server.key -out certs/server.crt \
  -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,DNS:healthonetpa.local,IP:127.0.0.1"
```

**Step 2:** Mount certs into APISIX container.

```yaml
apisix:
  volumes:
    - ./apisix/config.yaml:/usr/local/apisix/conf/config.yaml:ro
    - ./certs:/etc/apisix/ssl:ro    # ← ADD THIS
```

**Step 3:** Upload certificate to APISIX via `setup-apisix.sh`.

```bash
setup_ssl() {
    CERT=$(cat /path/to/certs/server.crt)
    KEY=$(cat /path/to/certs/server.key)
    
    SSL_JSON=$(python3 -c "
import json, sys
cert = open('certs/server.crt').read()
key = open('certs/server.key').read()
print(json.dumps({'cert': cert, 'key': key, 'snis': ['localhost', 'healthonetpa.local', '127.0.0.1']}))
")
    curl -s -X PUT "http://localhost:9180/apisix/admin/ssls/1" \
        -H "X-API-KEY: $ADMIN_KEY" \
        -H "Content-Type: application/json" \
        -d "$SSL_JSON"
}
```

**Step 4:** Re-run setup script after starting the stack.

```bash
bash apisix/setup-apisix.sh
```

**Verification:**

```bash
curl -k -X POST https://localhost:9443/api/v1/claims \
  -H "apikey: hosp-apollo-key-2026" \
  -H "Content-Type: application/json" \
  -d '{"memberId":"M1001","hospitalId":"H5501-Apollo","amount":50000,"diagnosis":"Fever"}'
# Expected: 202 Accepted with claimId
```

---

## Issue 09 — Grafana Redirect Loop via HTTPS {#issue-09}

### Symptoms

Opening `https://localhost:9443/grafana/` in browser causes an infinite redirect loop or shows `ERR_TOO_MANY_REDIRECTS`.

### Root Cause

Conflict between APISIX route configuration and Grafana's sub-path setting:

- APISIX was configured to strip prefix `/grafana/` using `proxy-rewrite` plugin, sending `/` to Grafana
- Grafana was configured with `GF_SERVER_SERVE_FROM_SUB_PATH=true` and `GF_SERVER_ROOT_URL=https://localhost:9443/grafana/`
- With `SERVE_FROM_SUB_PATH=true`, Grafana internally serves all assets from `/grafana/...` and redirects bare `/` back to `/grafana/`
- APISIX strips the prefix again → infinite loop

### Resolution

**Option: Passthrough routing** — do not strip the prefix. APISIX forwards the full path including `/grafana/` and Grafana handles it natively.

```bash
# In setup-apisix.sh — use passthrough (no proxy-rewrite plugin)
curl -s -X PUT "http://localhost:9180/apisix/admin/routes/tool-grafana" \
  -H "X-API-KEY: $ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "uri": "/grafana/*",
    "upstream": {
      "type": "roundrobin",
      "nodes": {"grafana:3000": 1}
    }
  }'
```

Grafana Docker Compose env vars remain:

```yaml
GF_SERVER_ROOT_URL: "https://localhost:9443/grafana/"
GF_SERVER_SERVE_FROM_SUB_PATH: "true"
```

**Routing Strategy Reference:**

| Tool | Internal Serves From | APISIX Strategy |
|------|---------------------|-----------------|
| Grafana | `/grafana/` | Passthrough |
| Prometheus | `/` (root) | Passthrough |
| Kafka UI | `/kafka-ui` | Passthrough |
| Swagger UI | `/swagger-ui.html` | Passthrough |
| RedisInsight | `/` (root) | Strip prefix |

---

## Issue 10 — Prometheus 302 Redirect via HTTPS {#issue-10}

### Symptoms

`https://localhost:9443/prometheus/` returns `302 Found` or redirects to `http://localhost:9090/prometheus/graph` — breaking out of HTTPS.

### Root Cause

Initial configuration used `--web.route-prefix=/prometheus` which makes Prometheus serve all endpoints at `/prometheus/...` internally. Combined with an APISIX route that also stripped the `/prometheus/` prefix, the effective path became `/prometheus/prometheus/...` — causing double-prefix redirects.

Alternatively: Prometheus with `--web.route-prefix=/prometheus` sends absolute redirects to `http://localhost:9090/prometheus/` which escapes the APISIX proxy entirely.

### Resolution

**File:** `docker-compose.yml` — use `--web.route-prefix=/` (serve from root) and set only the external URL.

```yaml
prometheus:
  command:
    - '--config.file=/etc/prometheus/prometheus.yml'
    - '--storage.tsdb.retention.time=7d'
    - '--web.external-url=https://localhost:9443/prometheus/'   # ← for link generation
    - '--web.route-prefix=/'                                    # ← serve from root internally
```

**APISIX route:** Use passthrough (no strip) — the full path `/prometheus/...` is forwarded to Prometheus which handles it natively via the external-url setting.

```bash
# Passthrough route for Prometheus
curl -s -X PUT "http://localhost:9180/apisix/admin/routes/tool-prometheus" \
  -d '{
    "uri": "/prometheus/*",
    "upstream": {"type": "roundrobin", "nodes": {"prometheus:9090": 1}}
  }'
```

---

## Issue 11 — Swagger UI Whitelabel 404 Error {#issue-11}

### Symptoms

```
Whitelabel Error Page
This application has no explicit mapping for /error, so you are seeing this as a fallback.
...
type=Not Found, status=404
```

Accessing `https://localhost:9443/swagger-ui/` returns Spring Boot's default 404 error page.

### Root Cause

APISIX route was configured to strip prefix `/swagger-ui/` and forward `/` to the claim service. Spring Boot's Swagger UI is mounted at `/swagger-ui.html` and `/swagger-ui/index.html` — there is no handler for the root path `/`.

### Resolution

**Option: Passthrough routing** — forward the full `/swagger-ui.html` path to Spring Boot.

```bash
# Correct passthrough routes for Swagger
curl -s -X PUT "http://localhost:9180/apisix/admin/routes/tool-swagger" \
  -d '{
    "uri": "/swagger-ui*",
    "upstream": {"type": "roundrobin", "nodes": {"claim-service-1:8080": 1}}
  }'

curl -s -X PUT "http://localhost:9180/apisix/admin/routes/tool-apidocs" \
  -d '{
    "uri": "/api-docs*",
    "upstream": {"type": "roundrobin", "nodes": {"claim-service-1:8080": 1}}
  }'
```

**Correct access URL:** `https://localhost:9443/swagger-ui/index.html`

The `springdoc` config in `application.yml`:

```yaml
springdoc:
  api-docs:
    path: /api-docs
  swagger-ui:
    path: /swagger-ui.html
```

---

## Quick Reference — Error → Issue Mapping

| Error Message | Issue # |
|---|---|
| `pull access denied for provectus/kafka-ui` | [Issue 01](#issue-01) |
| `{"message": "Missing related consumer"}` | [Issue 02](#issue-02) |
| `UNKNOWN_TOPIC_OR_PARTITION` in Kafka consumer logs | [Issue 03](#issue-03) |
| `kafka-init` exits 0 but no topics created | [Issue 04](#issue-04) |
| `NOAUTH Authentication required` in Redis | [Issue 05](#issue-05) |
| `{"error": "Failed to publish claim event"}` | [Issue 06](#issue-06) |
| `setup-apisix.sh` stops without error | [Issue 07](#issue-07) |
| `{"error_msg":"404 Route Not Found"}` on HTTPS | [Issue 08](#issue-08) |
| Browser: `ERR_TOO_MANY_REDIRECTS` on Grafana | [Issue 09](#issue-09) |
| `302 Found` redirect from Prometheus | [Issue 10](#issue-10) |
| `Whitelabel Error Page` / `status=404` on Swagger | [Issue 11](#issue-11) |

---

## Diagnostic Commands

```bash
# Check all container health statuses
docker compose ps

# Follow logs for a specific service
docker compose logs -f claim-service-1

# Check Kafka topics exist
docker exec healthone-kafka /opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list

# Check Redis seed data
docker exec healthone-redis redis-cli -a healthone-tpa-redis-2026 KEYS "*"

# Check APISIX consumers
curl -s http://localhost:9180/apisix/admin/consumers \
  -H "X-API-KEY: edd1c9f034335f136f87ad84b625c8f1" | python3 -m json.tool

# Check APISIX routes
curl -s http://localhost:9180/apisix/admin/routes \
  -H "X-API-KEY: edd1c9f034335f136f87ad84b625c8f1" | python3 -m json.tool

# Check APISIX SSL certificates
curl -s http://localhost:9180/apisix/admin/ssls \
  -H "X-API-KEY: edd1c9f034335f136f87ad84b625c8f1" | python3 -m json.tool

# Test HTTPS with self-signed cert
curl -kv https://localhost:9443/api/v1/claims \
  -H "apikey: hosp-apollo-key-2026"

# Check Prometheus targets
curl -s http://localhost:9090/api/v1/targets | python3 -m json.tool
```

---

*Generated for HealthOne TPA Claims Processing Pipeline — April 2026*
