# ──────────────────────────────────────────────────────────────────────────────
#  HealthOne TPA Claims Pipeline — Local Development Makefile
# ──────────────────────────────────────────────────────────────────────────────

.PHONY: help up down build test logs status clean setup-apisix kafka-topics redis-seed gitlab-password consul-register

COMPOSE = docker-compose
KAFKA   = docker exec healthone-kafka kafka-topics.sh --bootstrap-server localhost:9092
REDIS   = docker exec healthone-redis redis-cli

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ── Stack lifecycle ────────────────────────────────────────────────────────────

up: ## Start all services (build if needed)
	$(COMPOSE) up -d --build
	@echo ""
	@echo "Stack is starting. Run 'make status' to check readiness."
	@echo "Run 'make setup-apisix' once APISIX is healthy."

down: ## Stop and remove containers (keeps volumes)
	$(COMPOSE) down

build: ## Build all service images without starting
	$(COMPOSE) build

clean: ## Stop containers AND delete all volumes (fresh start)
	$(COMPOSE) down -v
	@echo "All volumes deleted. Run 'make up' for a clean start."

logs: ## Tail logs for all services
	$(COMPOSE) logs -f --tail=100

logs-%: ## Tail logs for a specific service: make logs-claim-service-1
	$(COMPOSE) logs -f --tail=100 $*

status: ## Show container status and health
	$(COMPOSE) ps

restart-%: ## Restart a specific service: make restart-fraud-service
	$(COMPOSE) restart $*

# ── Setup ──────────────────────────────────────────────────────────────────────

setup-apisix: ## Configure APISIX routes, consumers, and rate limits
	bash apisix/setup-apisix.sh

# ── Kafka operations ───────────────────────────────────────────────────────────

kafka-topics: ## List all Kafka topics
	$(KAFKA) --list

kafka-describe: ## Describe claim-events topic
	$(KAFKA) --describe --topic claim-events

kafka-produce: ## Open an interactive producer for claim-events
	docker exec -it healthone-kafka kafka-console-producer.sh \
	  --bootstrap-server localhost:9092 \
	  --topic claim-events \
	  --property parse.key=true \
	  --property key.separator=:

kafka-consume-claims: ## Consume claim-events from the beginning
	docker exec -it healthone-kafka kafka-console-consumer.sh \
	  --bootstrap-server localhost:9092 \
	  --topic claim-events \
	  --from-beginning \
	  --group replay-audit

kafka-consume-eligibility: ## Consume eligibility results
	docker exec -it healthone-kafka kafka-console-consumer.sh \
	  --bootstrap-server localhost:9092 \
	  --topic eligibility-results \
	  --from-beginning

kafka-consume-fraud: ## Consume fraud alerts
	docker exec -it healthone-kafka kafka-console-consumer.sh \
	  --bootstrap-server localhost:9092 \
	  --topic fraud-alerts \
	  --from-beginning

kafka-consumer-groups: ## Show all consumer group offsets and lag
	docker exec healthone-kafka kafka-consumer-groups.sh \
	  --bootstrap-server localhost:9092 \
	  --describe \
	  --all-groups

kafka-audit-retention: ## Set audit-log retention to 7 days
	$(KAFKA) --alter --topic audit-log --config retention.ms=604800000

# ── Redis operations ───────────────────────────────────────────────────────────

redis-cli: ## Open a Redis CLI session
	docker exec -it healthone-redis redis-cli

redis-seed: ## Re-seed Redis with HealthOne TPA reference data
	$(COMPOSE) restart redis-init

redis-policy: ## View member M1001 policy (example)
	$(REDIS) HGETALL member:M1001:policy

redis-hospitals: ## View empanelled hospitals in Mumbai
	$(REDIS) SMEMBERS empanelled:mumbai

redis-fraud-scores: ## View top fraud risk hospitals
	$(REDIS) ZREVRANGE fraud:hospital:scores 0 9 WITHSCORES

redis-flush: ## DANGEROUS: flush all Redis data
	@echo "WARNING: This will delete all Redis data. Press Ctrl+C to cancel."
	@sleep 3
	$(REDIS) FLUSHALL

# ── Testing ────────────────────────────────────────────────────────────────────

test-claim: ## Run claim-service unit tests (Maven)
	cd claim-service-java && mvn test -q

test-eligibility: ## Run eligibility-service unit tests (Maven)
	cd eligibility-service-java && mvn test -q

test-fraud: ## Run fraud-service unit tests (Maven)
	cd fraud-service-java && mvn test -q

test-all: test-claim test-eligibility test-fraud ## Run all unit tests across all services

# ── End-to-end test ────────────────────────────────────────────────────────────

e2e-test: ## Send a test claim through the full pipeline via APISIX
	@echo "Submitting test claim via APISIX..."
	curl -s -X POST http://localhost:9080/api/v1/claims \
	  -H "apikey: APOLLO-KEY-2026" \
	  -H "Content-Type: application/json" \
	  -d '{"memberId":"M1001","hospitalId":"H5501-Apollo","hospitalName":"Apollo","insurer":"StarHealth","amount":85000,"city":"mumbai","claimType":"cashless","diagnosisCode":"Z51.1"}' \
	  | python3 -m json.tool

# ── GitLab operations ─────────────────────────────────────────────────────────

gitlab-password: ## Get the auto-generated GitLab root password (run once after first boot)
	docker exec healthone-gitlab grep 'Password:' /etc/gitlab/initial_root_password

gitlab-logs: ## Tail GitLab logs
	$(COMPOSE) logs -f --tail=100 gitlab

# ── Consul operations ──────────────────────────────────────────────────────────

consul-register: ## Register claim-service instances in Consul (for APISIX discovery demo)
	curl -s -X PUT http://localhost:8500/v1/agent/service/register \
	  -H 'Content-Type: application/json' \
	  -d '{"ID":"claim-service-1","Name":"claim-service","Address":"claim-service-1","Port":8080,"Tags":["healthone"]}'
	curl -s -X PUT http://localhost:8500/v1/agent/service/register \
	  -H 'Content-Type: application/json' \
	  -d '{"ID":"claim-service-2","Name":"claim-service","Address":"claim-service-2","Port":8081,"Tags":["healthone"]}'
	@echo "Services registered. View at http://localhost:8500/ui"

consul-services: ## List all services registered in Consul
	curl -s http://localhost:8500/v1/catalog/services | python3 -m json.tool

# ── Build ──────────────────────────────────────────────────────────────────────

build-services: ## Build all Spring Boot JARs (without running tests)
	cd claim-service-java     && mvn package -DskipTests -q
	cd eligibility-service-java && mvn package -DskipTests -q
	cd fraud-service-java     && mvn package -DskipTests -q
