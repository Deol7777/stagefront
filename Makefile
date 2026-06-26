# Flash-Sale Ticketing Saga Platform — task runner
# Placeholder targets. Commands stubbed until infra (docker-compose, k8s) exists.

.PHONY: up down seed chaos logs help

help: ## list targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-8s\033[0m %s\n", $$1, $$2}'

up: ## start full stack (Kafka, Postgres x4, Redis, services)
	@echo "TODO: docker compose up -d  # not built yet"

down: ## tear down stack
	@echo "TODO: docker compose down -v  # not built yet"

seed: ## seed demo data (events, seats, users)
	@echo "TODO: run seed script against running services  # not built yet"

chaos: ## drive chaos toggles / fault injection
	@echo "TODO: invoke chaos control endpoints  # not built yet"

logs: ## tail aggregated service logs
	@echo "TODO: docker compose logs -f  # not built yet"
