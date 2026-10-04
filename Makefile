# Project interpreter. Override with: make gen PY=python3
PY ?= .venv/bin/python

# Compose entrypoint. Picks up docker-compose.yml from the repo root.
COMPOSE ?= docker compose

.PHONY: help docker-build docker-up-build docker-up docker-down docker-logs docker-shell docker-ps docker-clean seed-data gen wizard web

help:
	@echo "Airflow Blueprint - Make Commands"
	@echo "=================================="
	@echo ""
	@echo "Docker Commands:"
	@echo "  make docker-build    - Build Docker image"
	@echo "  make docker-up       - Start the stack in the background"
	@echo "  make docker-down     - Stop services"
	@echo "  make docker-logs     - View service logs"
	@echo "  make docker-shell    - Open a shell in airflow-scheduler"
	@echo "  make docker-ps       - Show running containers"
	@echo "  make docker-clean    - Clean up Docker resources"
	@echo "  make docker-up-build - Rebuild images, then start"
	@echo ""
	@echo "Sample Data:"
	@echo "  make seed-data       - Populate all six source tables"
	@echo ""
	@echo "Pipeline Generation:"
	@echo "  make web             - Browser chat UI on http://localhost:8000"
	@echo "  make gen             - Chat: describe a pipeline in your own words"
	@echo "  make wizard          - Guided interview, one validated question at a time"
	@echo ""

# Docker Production Commands
docker-build:
	$(COMPOSE) build

docker-up-build:
	$(COMPOSE) up -d --build

docker-up:
	$(COMPOSE) up -d
	@echo "✅ Services started (health checks take ~60s)"
	@echo "📍 Airflow UI:    http://localhost:8080"
	@echo "📍 MinIO console: http://localhost:9001"
	@echo "📍 Iceberg REST:  http://localhost:8181"

docker-down:
	$(COMPOSE) down
	@echo "✅ Services stopped"

docker-logs:
	$(COMPOSE) logs -f

docker-shell:
	$(COMPOSE) exec airflow-scheduler /bin/bash

docker-ps:
	$(COMPOSE) ps

docker-clean:
	$(COMPOSE) down -v
	docker system prune -f
	@echo "✅ Docker cleaned"


tf-init:
	cd infra/aws && source ./.env && terraform init

tf-plan:
	cd infra/aws && source ./.env && terraform plan

tf-apply:
	cd infra/aws && source ./.env && terraform apply

tf-output:
	cd infra/aws && source ./.env && terraform output

tf-destroy:
	cd infra/aws && source ./.env && terraform destroy

# Sample data for all six source tables. Recreates the schema from
# init-scripts/02-init-banking-schema.sql, so it starts from a clean slate.
seed-data:
	$(PY) banking-app/scripts/generate_banking_data.py --users 100 --transactions 5

# Natural-language chat. Needs an LLM: ANTHROPIC_API_KEY, or LLM_PROVIDER=bedrock.
gen:
	$(PY) -m dag_generator.cli

# Guided interview. Deterministic - every answer is checked against the live
# blueprint registry, source catalog and connection list, so no LLM is involved.
wizard:
	$(PY) -m dag_generator.cli --wizard

# Browser UI for the same chat. Binds to localhost only: it holds sessions in
# memory, has no authentication, and writes into airflow/dags/ when asked.
# Override the port with: make web PORT=9000
PORT ?= 8000
web:
	$(PY) -m dag_generator.cli --web --port $(PORT)


