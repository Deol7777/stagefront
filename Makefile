# Flash-Sale Ticketing Saga Platform — task runner
# Infra targets use docker-compose; build/run targets use the Maven wrapper.

.PHONY: up up-core obs kafka-ui down stop seed chaos logs ps help \
        build test install run-order run-inventory run-payment run-notification

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

# ---- Infra (docker-compose) ----

# Core services needed for the saga (no observability / schema-registry).
# notification db is included so notification-service can run later.
CORE := kafka postgres-order postgres-inventory postgres-payment postgres-notification redis

up: ## start FULL infra (core + schema registry + observability) — heavier
	docker compose up -d

up-core: ## start ONLY core infra (Kafka, 3+1 Postgres, Redis) — lean, less RAM
	docker compose up -d $(CORE)

obs: ## start the observability stack on top (Prometheus, Grafana, OTel, schema registry)
	docker compose up -d prometheus grafana otel-collector schema-registry

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

logs: ## tail aggregated service logs
	docker compose logs -f

ps: ## show running infra containers
	docker compose ps
