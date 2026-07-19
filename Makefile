# Flash-Sale Ticketing Saga Platform — task runner
# Infra targets use docker-compose; build/run targets use the Maven wrapper.

.PHONY: up up-core obs trace lag kafka-ui down stop seed chaos logs ps help \
        build test install run-order run-inventory run-payment run-notification stop-services \
        poison dlq dlq-peek gateway-fail gateway-ok cb-state \
        cache-stats cache-get cache-redis

help: ## list targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

# ---- Build / run (Java side) ----

build: ## compile all modules
	./mvnw clean compile

test: ## run all tests
	./mvnw test

install: ## build + install all modules to ~/.m2 (needed before running one service alone)
	./mvnw install -DskipTests

# Each run-* boots one service. Requires `make install` first (single-module
# runs resolve contracts + the parent POM from ~/.m2, not from target/).
run-order: ## run order-service (:8081)
	./mvnw -pl services/order-service spring-boot:run

run-inventory: ## run inventory-service (:8082)
	./mvnw -pl services/inventory-service spring-boot:run

run-payment: ## run payment-service (:8083)
	./mvnw -pl services/payment-service spring-boot:run

run-notification: ## run notification-service (:8084)
	./mvnw -pl services/notification-service spring-boot:run

# Kill every service started by a run-* target. Matches the Maven process, so it
# won't touch the Docker infra (use `make stop` / `make down` for that).
# The `|| true` keeps make happy when nothing is running — pkill exits 1 on no match.
stop-services: ## stop all four Spring services started by run-*
	@pkill -f "spring-boot:run" || true
	@sleep 2
	@lsof -nP -iTCP:8081,8082,8083,8084 -sTCP:LISTEN >/dev/null 2>&1 \
		&& echo "warning: something still listening on 8081-8084 (try pkill -9 -f spring-boot:run)" \
		|| echo "all services stopped"

# ---- Infra (docker-compose) ----

# Core services needed for the saga (no observability / schema-registry).
# notification db is included so notification-service can run later.
CORE := kafka postgres-order postgres-inventory postgres-payment postgres-notification redis

up: ## start FULL infra (core + schema registry + observability) — heavier
	docker compose up -d

up-core: ## start ONLY core infra (Kafka, 3+1 Postgres, Redis) — lean, less RAM
	docker compose up -d $(CORE)

obs: ## start observability on top (Prometheus :9090, Grafana :3000, OTel collector, Jaeger :16686, kafka-exporter)
	docker compose up -d prometheus grafana otel-collector jaeger kafka-exporter
	@echo "Grafana  http://localhost:3000  (admin/admin)"
	@echo "Prom     http://localhost:9090/targets"
	@echo "Jaeger   http://localhost:16686"

# schema-registry is NOT in `obs`: nothing uses it yet (events are serialized as
# JSON strings), and it costs ~300MB for nothing. `make up` still starts it.

trace: ## open the Jaeger UI (traces of the saga)
	open http://localhost:16686

# Consumer-group lag, straight from Prometheus. Non-zero and GROWING means
# consumers can't keep up; non-zero and shrinking is just a burst draining.
lag: ## show Kafka consumer-group lag per group/topic
	@python3 infra/observability/lag.py

kafka-ui: ## start Kafka UI to browse topics/messages/lag (http://localhost:8085)
	docker compose up -d kafka-ui

down: ## tear down stack + volumes
	docker compose down -v

stop: ## stop containers but KEEP data (volumes)
	docker compose stop

seed: ## seed demo data (events, seats, users)
	@echo "TODO: run seed script against running services  # not built yet"

chaos: ## drive chaos toggles / fault injection
	@echo "TODO: invoke chaos control endpoints  # not built yet"

# ---- DLQ / poison-message demo (note 12) ----

# Inject a malformed record straight onto orders.placed. It is NOT valid JSON, so
# inventory-service's parse() throws → 3 attempts (FixedBackOff 500ms x2) → the
# DeadLetterPublishingRecoverer routes it to orders.placed.DLQ. Watch it with
# `make dlq`, the dashboard DLQ panel, or kafka-ui (:8085).
# --key so the record carries a key (mirrors how the outbox keys by orderId).
poison: ## inject a poison (malformed) event into orders.placed → lands in orders.placed.DLQ
	@printf 'deadbeef:NOT-VALID-JSON-{{{\n' | docker compose exec -T kafka \
		/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9094 \
		--topic orders.placed --property parse.key=true --property key.separator=:
	@echo "→ poison sent. inventory-service retries 3x, then routes to orders.placed.DLQ."

# List every *.DLQ topic + its depth via order-service's DLQ admin API.
dlq: ## list DLQ topics + message counts (order-service :8081 must be running)
	@curl -s http://localhost:8081/api/dlq/topics | python3 -m json.tool

# Peek the contents of one DLQ topic: make dlq-peek TOPIC=orders.placed.DLQ
dlq-peek: ## peek messages on a DLQ topic (TOPIC=orders.placed.DLQ)
	@curl -s "http://localhost:8081/api/dlq/$(TOPIC)" | python3 -m json.tool

# ---- Circuit breaker / gateway chaos (note 13) ----

# Turn the simulated payment gateway OFF (every charge fails). After a few failed
# orders the paymentGateway circuit breaker trips OPEN and payments fast-fail as
# PaymentDeclined(GATEWAY_UNAVAILABLE) → the saga compensates. Watch `make cb-state`.
gateway-fail: ## make the payment gateway fail (trips the circuit breaker)
	@curl -s -X POST "http://localhost:8083/api/chaos/gateway?fail=true" | python3 -m json.tool

# Restore the gateway. The breaker moves OPEN → HALF_OPEN (probes) → CLOSED as
# calls start succeeding again, and orders authorize normally.
gateway-ok: ## restore the payment gateway (breaker recovers to CLOSED)
	@curl -s -X POST "http://localhost:8083/api/chaos/gateway?fail=false" | python3 -m json.tool

# Show the gateway toggle + live circuit-breaker state (CLOSED / OPEN / HALF_OPEN).
cb-state: ## show gateway toggle + circuit-breaker state
	@curl -s http://localhost:8083/api/chaos/gateway | python3 -m json.tool

# ---- Seat read cache (Redis cache-aside, note 14) ----

# Cache-aside read of one seat (Redis → miss → Postgres). Run twice: 1st = miss, 2nd = hit.
cache-get: ## cache-aside read of one seat (SEAT=seat-A1)
	@curl -s "http://localhost:8082/api/seats/$(SEAT)" | python3 -m json.tool

# Hit/miss counters + hit rate for the seat cache.
cache-stats: ## show seat-cache hit/miss stats
	@curl -s http://localhost:8082/api/seats/cache/stats | python3 -m json.tool

# Peek the raw cached seat keys straight from Redis.
cache-redis: ## list cached seat keys in Redis (and dump values)
	@docker compose exec -T redis redis-cli --scan --pattern 'seat:*' | while read k; do \
		printf '%s = ' "$$k"; docker compose exec -T redis redis-cli get "$$k"; done

logs: ## tail aggregated service logs
	docker compose logs -f

ps: ## show running infra containers
	docker compose ps
