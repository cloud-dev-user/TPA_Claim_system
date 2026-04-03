# ──────────────────────────────────────────────────────────────────────────────
#  MD India Claims Pipeline — Local Development Makefile
# ──────────────────────────────────────────────────────────────────────────────

.PHONY: help up down build test logs status clean setup-apisix kafka-topics redis-seed

COMPOSE = docker-compose
KAFKA   = docker exec md-kafka kafka-topics.sh --bootstrap-server localhost:9092
REDIS   = docker exec md-redis redis-cli

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
	docker exec -it md-kafka kafka-console-producer.sh \
	  --bootstrap-server localhost:9092 \
	  --topic claim-events \
	  --property parse.key=true \
	  --property key.separator=:

kafka-consume-claims: ## Consume claim-events from the beginning
	docker exec -it md-kafka kafka-console-consumer.sh \
	  --bootstrap-server localhost:9092 \
	  --topic claim-events \
	  --from-beginning \
	  --group replay-audit

kafka-consume-eligibility: ## Consume eligibility results
	docker exec -it md-kafka kafka-console-consumer.sh \
	  --bootstrap-server localhost:9092 \
	  --topic eligibility-results \
	  --from-beginning

kafka-consume-fraud: ## Consume fraud alerts
	docker exec -it md-kafka kafka-console-consumer.sh \
	  --bootstrap-server localhost:9092 \
	  --topic fraud-alerts \
	  --from-beginning

kafka-consumer-groups: ## Show all consumer group offsets and lag
	docker exec md-kafka kafka-consumer-groups.sh \
	  --bootstrap-server localhost:9092 \
	  --describe \
	  --all-groups

kafka-audit-retention: ## Set audit-log retention to 7 days
	$(KAFKA) --alter --topic audit-log --config retention.ms=604800000

# ── Redis operations ───────────────────────────────────────────────────────────

redis-cli: ## Open a Redis CLI session
	docker exec -it md-redis redis-cli

redis-seed: ## Re-seed Redis with MD India reference data
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

test-claim: ## Run claim-service unit tests
	cd claim-service && pip install -q -r requirements.txt && \
	  pytest tests/ -v --tb=short

test-eligibility: ## Run eligibility-service unit tests
	cd eligibility-service && pip install -q -r requirements.txt && \
	  pytest tests/ -v --tb=short

test-fraud: ## Run fraud-service unit tests
	cd fraud-service && pip install -q -r requirements.txt && \
	  pytest tests/ -v --tb=short

test-all: test-claim test-eligibility test-fraud ## Run all unit tests

# ── End-to-end test ────────────────────────────────────────────────────────────

e2e-test: ## Send a test claim through the full pipeline via APISIX
	@echo "Submitting test claim via APISIX..."
	curl -s -X POST http://localhost:9080/api/v1/claims \
	  -H "apikey: APOLLO-KEY-2026" \
	  -H "Content-Type: application/json" \
	  -d '{"member_id":"M1001","hospital_id":"H5501-Apollo","hospital_name":"Apollo","insurer":"StarHealth","amount":85000,"city":"mumbai","claim_type":"cashless","diagnosis_code":"Z51.1"}' \
	  | python -m json.tool

# ── Linting ────────────────────────────────────────────────────────────────────

lint: ## Run flake8 on all services
	cd claim-service     && flake8 app/ tests/ --max-line-length=100 --extend-ignore=E203,W503
	cd eligibility-service && flake8 app/ tests/ --max-line-length=100 --extend-ignore=E203,W503
	cd fraud-service     && flake8 app/ tests/ --max-line-length=100 --extend-ignore=E203,W503
