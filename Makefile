# Flash-Sale Ticketing Saga Platform — task runner
# Placeholder targets. Commands stubbed until infra (docker-compose, k8s) exists.

.PHONY: up down seed chaos logs help

help: ## list targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-8s\033[0m %s\n", $$1, $$2}'

up: ## start infra (Kafka, Postgres x4, Redis, schema registry, observability)
	docker compose up -d

down: ## tear down stack + volumes
	docker compose down -v

seed: ## seed demo data (events, seats, users)
	@echo "TODO: run seed script against running services  # not built yet"

chaos: ## drive chaos toggles / fault injection
	@echo "TODO: invoke chaos control endpoints  # not built yet"

logs: ## tail aggregated service logs
	docker compose logs -f

ps: ## show running infra containers
	docker compose ps
