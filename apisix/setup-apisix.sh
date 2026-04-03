#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
#  MD India APISIX Setup Script
#  Configures: Upstream, Routes, Consumers, Rate Limits, Prometheus, TLS
#
#  Run AFTER: docker-compose up (wait for APISIX to be healthy)
#  Usage:     bash apisix/setup-apisix.sh
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail

APISIX_ADMIN="http://localhost:9180"
ADMIN_KEY="edd1c9f034335f136f87ad84b625c8f1"
H="Content-Type: application/json"
K="X-API-KEY: ${ADMIN_KEY}"

# ── Colour helpers ─────────────────────────────────────────────────────────────
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
ok()   { echo -e "${GREEN}[OK]${NC}  $1"; }
info() { echo -e "${YELLOW}[>>]${NC}  $1"; }
fail() { echo -e "${RED}[ERR]${NC} $1"; exit 1; }

wait_for_apisix() {
  info "Waiting for APISIX Admin API (up to 120 seconds)..."
  for i in $(seq 1 60); do
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
      "${APISIX_ADMIN}/apisix/admin/routes" \
      -H "${K}" 2>/dev/null)
    if [ "${HTTP_CODE}" = "200" ] || [ "${HTTP_CODE}" = "404" ]; then
      ok "APISIX Admin API is ready (HTTP ${HTTP_CODE})"
      return
    fi
    printf "."
    sleep 2
  done
  echo ""
  fail "APISIX did not become ready after 120 seconds. Check: docker compose logs apisix"
}

# ──────────────────────────────────────────────────────────────────────────────
# 0. TLS — Upload certificate to APISIX
# ──────────────────────────────────────────────────────────────────────────────
setup_ssl() {
  info "Uploading TLS certificate to APISIX (SNI: localhost, mdindia.local)..."

  CERT_FILE="./certs/server.crt"
  KEY_FILE="./certs/server.key"

  if [ ! -f "${CERT_FILE}" ] || [ ! -f "${KEY_FILE}" ]; then
    fail "TLS cert not found. Run: bash certs/generate-certs.sh first."
  fi

  # Use python3 to safely JSON-encode multi-line PEM content
  CERT_JSON=$(python3 -c "import json,sys; print(json.dumps(open('${CERT_FILE}').read()))")
  KEY_JSON=$(python3 -c "import json,sys; print(json.dumps(open('${KEY_FILE}').read()))")

  curl -sf -X PUT "${APISIX_ADMIN}/apisix/admin/ssls/1" \
    -H "${H}" -H "${K}" \
    -d "{
      \"cert\": ${CERT_JSON},
      \"key\":  ${KEY_JSON},
      \"snis\": [\"localhost\", \"mdindia.local\", \"127.0.0.1\"]
    }" > /dev/null

  ok "TLS certificate registered (valid for localhost, mdindia.local)"
}

# ──────────────────────────────────────────────────────────────────────────────
# 1. Upstream — claim-service (round-robin across 2 instances)
# ──────────────────────────────────────────────────────────────────────────────
setup_upstream() {
  info "Creating upstream: claim-service (round-robin, 2 nodes)"
  curl -sf -X PUT "${APISIX_ADMIN}/apisix/admin/upstreams/claim-service-upstream" \
    -H "${H}" -H "${K}" \
    -d '{
      "id": "claim-service-upstream",
      "name": "claim-service",
      "desc": "MD India claim submission service — 2 instances for load balancing",
      "type": "roundrobin",
      "nodes": {
        "claim-service-1:8080": 1,
        "claim-service-2:8081": 1
      },
      "checks": {
        "active": {
          "http_path": "/health",
          "interval": 5,
          "timeout": 2,
          "unhealthy": { "interval": 2, "http_failures": 3 },
          "healthy":   { "interval": 5, "successes": 2 }
        }
      },
      "scheme": "http"
    }' > /dev/null
  ok "Upstream created"
}

# ──────────────────────────────────────────────────────────────────────────────
# 2. Route — POST /api/v1/claims
# ──────────────────────────────────────────────────────────────────────────────
setup_route() {
  info "Creating route: POST /api/v1/claims"
  curl -sf -X PUT "${APISIX_ADMIN}/apisix/admin/routes/claims-route" \
    -H "${H}" -H "${K}" \
    -d '{
      "id": "claims-route",
      "name": "claims-submission",
      "desc": "Hospital claim submission — API key auth + rate limiting",
      "uri": "/api/v1/claims",
      "methods": ["POST"],
      "upstream_id": "claim-service-upstream",
      "plugins": {
        "key-auth": {},
        "request-id": {
          "header_name": "X-Request-ID",
          "include_in_response": true
        },
        "response-rewrite": {
          "headers": {
            "set": {
              "Strict-Transport-Security": "max-age=31536000; includeSubDomains",
              "X-Content-Type-Options": "nosniff",
              "X-Frame-Options": "DENY",
              "Cache-Control": "no-store"
            }
          }
        },
        "cors": {
          "allow_origins": "*",
          "allow_methods": "POST, GET, OPTIONS",
          "allow_headers": "apikey, Content-Type, X-Request-ID",
          "max_age": 3600
        },
        "prometheus": {}
      }
    }' > /dev/null
  ok "Route created: POST /api/v1/claims"

  info "Creating route: GET /api/v1/claims/* (status check)"
  curl -sf -X PUT "${APISIX_ADMIN}/apisix/admin/routes/claims-status-route" \
    -H "${H}" -H "${K}" \
    -d '{
      "id": "claims-status-route",
      "name": "claims-status",
      "uri": "/api/v1/claims/*",
      "methods": ["GET"],
      "upstream_id": "claim-service-upstream",
      "plugins": {
        "key-auth": {},
        "response-rewrite": {
          "headers": {
            "set": {
              "Strict-Transport-Security": "max-age=31536000; includeSubDomains",
              "X-Content-Type-Options": "nosniff",
              "X-Frame-Options": "DENY",
              "Cache-Control": "no-store"
            }
          }
        },
        "prometheus": {}
      }
    }' > /dev/null
  ok "Route created: GET /api/v1/claims/*"
}

# ──────────────────────────────────────────────────────────────────────────────
# 3. Consumers — one per hospital with per-consumer rate limits
# ──────────────────────────────────────────────────────────────────────────────
create_consumer() {
  local username=$1
  local api_key=$2
  local rate_limit=$3
  local desc=$4

  info "Creating consumer: ${username} (rate: ${rate_limit}/min)"

  # Use python3 to safely build JSON — avoids bash escaping issues
  PAYLOAD=$(python3 -c "
import json
print(json.dumps({
  'username': '${username}',
  'desc':     '${desc}',
  'plugins': {
    'key-auth': {'key': '${api_key}'},
    'limit-count': {
      'count':         ${rate_limit},
      'time_window':   60,
      'rejected_code': 429,
      'rejected_msg':  'Rate limit exceeded. Retry after 60 seconds.',
      'key':           'consumer_name',
      'policy':        'local'
    }
  }
}))
")

  RESPONSE=$(curl -s -X PUT "${APISIX_ADMIN}/apisix/admin/consumers/${username}" \
    -H "${H}" -H "${K}" \
    -d "${PAYLOAD}")
  if echo "${RESPONSE}" | grep -q "error_msg"; then
    fail "Consumer ${username} failed: ${RESPONSE}"
  fi
  ok "Consumer created: ${username}"
}

setup_consumers() {
  # APISIX usernames: only [a-zA-Z0-9_] allowed (no hyphens)
  create_consumer "hospital_apollo"    "APOLLO-KEY-2026"  200 "Apollo Hospitals network partner"
  create_consumer "hospital_fortis"    "FORTIS-KEY-2026"  50  "Fortis Healthcare network"
  create_consumer "hospital_max"       "MAX-KEY-2026"     50  "Max Healthcare network"
  create_consumer "hospital_kokilaben" "KOKI-KEY-2026"    50  "Kokilaben network"
  create_consumer "hospital_generic"   "GENERIC-KEY-2026" 20  "Non-empanelled hospital standard limit"
}

# ──────────────────────────────────────────────────────────────────────────────
# 4. Internal tool routes — proxied via APISIX HTTPS (no auth required)
#    All dashboard/tool UIs are accessible only through https://localhost:9443
# ──────────────────────────────────────────────────────────────────────────────
setup_tool_routes() {
  # All tools serve from their own sub-path internally.
  # APISIX forwards the full path unchanged (no proxy-rewrite stripping).
  #
  #  Tool          Internal sub-path          Configured via
  #  ----------    -----------------------    ----------------------------
  #  Grafana       /grafana/                  GF_SERVER_SERVE_FROM_SUB_PATH
  #  Prometheus    /prometheus/               --web.external-url
  #  Kafka UI      /kafka-ui/                 SERVER_SERVLET_CONTEXT_PATH
  #  RedisInsight  /                          (serves from root, strip needed)
  #  Swagger UI    /swagger-ui/               SpringDoc default
  #  api-docs      /api-docs                  SpringDoc default

  # Helper: passthrough route (no prefix stripping)
  passthrough_route() {
    local id=$1 uri=$2 host=$3 port=$4 desc=$5
    PAYLOAD=$(python3 -c "
import json
print(json.dumps({
  'id': '${id}', 'name': '${id}', 'desc': '${desc}',
  'uri': '${uri}',
  'upstream': {'type': 'roundrobin', 'nodes': {'${host}:${port}': 1}, 'scheme': 'http'},
  'plugins': {'response-rewrite': {'headers': {'set': {
    'Strict-Transport-Security': 'max-age=31536000; includeSubDomains',
    'X-Frame-Options': 'SAMEORIGIN',
    'X-Content-Type-Options': 'nosniff'
  }}}}
}))
")
    curl -sf -X PUT "${APISIX_ADMIN}/apisix/admin/routes/${id}" \
      -H "${H}" -H "${K}" -d "${PAYLOAD}" > /dev/null
    ok "Tool route: https://localhost:9443${uri} → ${host}:${port}"
  }

  # Helper: strip-prefix route (for tools that serve from /)
  strip_route() {
    local id=$1 prefix=$2 host=$3 port=$4 desc=$5
    PAYLOAD=$(python3 -c "
import json
print(json.dumps({
  'id': '${id}', 'name': '${id}', 'desc': '${desc}',
  'uri': '${prefix}*',
  'upstream': {'type': 'roundrobin', 'nodes': {'${host}:${port}': 1}, 'scheme': 'http'},
  'plugins': {
    'proxy-rewrite': {'regex_uri': ['${prefix}(.*)', '/\$1']},
    'response-rewrite': {'headers': {'set': {
      'Strict-Transport-Security': 'max-age=31536000; includeSubDomains',
      'X-Frame-Options': 'SAMEORIGIN',
      'X-Content-Type-Options': 'nosniff'
    }}}
  }
}))
")
    curl -sf -X PUT "${APISIX_ADMIN}/apisix/admin/routes/${id}" \
      -H "${H}" -H "${K}" -d "${PAYLOAD}" > /dev/null
    ok "Tool route: https://localhost:9443${prefix} → ${host}:${port} (prefix stripped)"
  }

  info "Creating HTTPS tool routes..."

  # Passthrough — tool serves from its own sub-path
  passthrough_route "route_grafana"    "/grafana/*"    "grafana"         3000 "Grafana dashboards"
  passthrough_route "route_prometheus" "/prometheus/*" "prometheus"      9090 "Prometheus metrics"
  passthrough_route "route_kafka_ui"   "/kafka-ui*"    "kafka-ui"        8080 "Kafka UI browser"
  passthrough_route "route_swagger"    "/swagger-ui*"  "claim-service-1" 8080 "Swagger UI"
  passthrough_route "route_apidocs"    "/api-docs*"    "claim-service-1" 8080 "OpenAPI spec"

  # Strip prefix — RedisInsight serves from /
  strip_route "route_redisinsight" "/redisinsight/" "redisinsight" 5540 "RedisInsight browser"
}

# ──────────────────────────────────────────────────────────────────────────────
# 5. Prometheus plugin (route-level metrics)
# ──────────────────────────────────────────────────────────────────────────────
setup_prometheus() {
  info "Enabling prometheus plugin globally"
  curl -sf -X PUT "${APISIX_ADMIN}/apisix/admin/global_rules/prometheus" \
    -H "${H}" -H "${K}" \
    -d '{
      "id": "prometheus",
      "plugins": {
        "prometheus": {
          "prefer_name": true
        }
      }
    }' > /dev/null
  ok "Prometheus metrics enabled on all routes"
}

# ──────────────────────────────────────────────────────────────────────────────
# 5. Print test commands
# ──────────────────────────────────────────────────────────────────────────────
print_test_commands() {
  echo ""
  echo "════════════════════════════════════════════════════════════════"
  echo "  APISIX Setup Complete — Test Commands"
  echo "════════════════════════════════════════════════════════════════"
  echo ""
  echo "# Submit a claim — HTTPS (Apollo — valid key)"
  echo "curl -k -X POST https://localhost:9443/api/v1/claims \\"
  echo "  -H 'apikey: APOLLO-KEY-2026' \\"
  echo "  -H 'Content-Type: application/json' \\"
  echo "  -d '{\"member_id\":\"M1001\",\"hospital_id\":\"H5501-Apollo\","
  echo "       \"hospital_name\":\"Apollo\",\"insurer\":\"StarHealth\","
  echo "       \"amount\":85000,\"city\":\"mumbai\",\"claim_type\":\"cashless\","
  echo "       \"diagnosis_code\":\"Z51.1\"}'"
  echo ""
  echo "# Submit a claim — HTTP (will redirect to HTTPS)"
  echo "curl -X POST http://localhost:9080/api/v1/claims \\"
  echo "  -H 'apikey: APOLLO-KEY-2026' \\"
  echo "  -H 'Content-Type: application/json' \\"
  echo "  -d '{\"member_id\":\"M1001\",\"hospital_id\":\"H5501-Apollo\","
  echo "       \"hospital_name\":\"Apollo\",\"insurer\":\"StarHealth\","
  echo "       \"amount\":85000,\"city\":\"mumbai\",\"claim_type\":\"cashless\","
  echo "       \"diagnosis_code\":\"Z51.1\"}'"
  echo ""
  echo "# Missing API key → 401"
  echo "curl -k -X POST https://localhost:9443/api/v1/claims \\"
  echo "  -H 'Content-Type: application/json' -d '{\"test\":1}'"
  echo ""
  echo "# Wrong key → 401"
  echo "curl -k -X POST https://localhost:9443/api/v1/claims \\"
  echo "  -H 'apikey: WRONG-KEY' -d '{\"test\":1}'"
  echo ""
  echo "# Prometheus metrics"
  echo "curl http://localhost:9091/apisix/prometheus/metrics | grep apisix_http"
  echo ""
  echo "# Rate limit test (11 rapid requests — 11th should 429)"
  echo "for i in \$(seq 1 11); do"
  echo "  curl -sk -o /dev/null -w \"%{http_code}\\n\" \\"
  echo "    -X POST https://localhost:9443/api/v1/claims \\"
  echo "    -H 'apikey: GENERIC-KEY-2026' \\"
  echo "    -H 'Content-Type: application/json' -d '{\"test\":\$i}'"
  echo "done"
  echo ""
  echo "════════════════════════════════════════════════════════════════"
  echo "  All URLs are now HTTPS via APISIX (use -k for self-signed cert)"
  echo "════════════════════════════════════════════════════════════════"
  echo "  API Gateway:   https://localhost:9443/api/v1/claims"
  echo "  Swagger UI:    https://localhost:9443/swagger-ui/"
  echo "  Grafana:       https://localhost:9443/grafana/        (admin / mdindia2026)"
  echo "  Kafka UI:      https://localhost:9443/kafka-ui/"
  echo "  Prometheus:    https://localhost:9443/prometheus/"
  echo "  RedisInsight:  https://localhost:9443/redisinsight/"
  echo "════════════════════════════════════════════════════════════════"
}

# ── Main ───────────────────────────────────────────────────────────────────────
main() {
  wait_for_apisix
  setup_ssl
  setup_upstream
  setup_route
  setup_consumers
  setup_tool_routes
  setup_prometheus
  print_test_commands
}

main "$@"
