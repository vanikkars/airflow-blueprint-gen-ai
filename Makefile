# Project interpreter. Override with: make gen PY=python3
PY ?= .venv/bin/python

.PHONY: help docker-build docker-up-build docker-up docker-down docker-logs docker-shell docker-ps docker-clean seed-data

help:
	@echo "Schema Registry - Make Commands"
	@echo "================================"
	@echo ""
	@echo "Docker Commands:"
	@echo "  make docker-build    - Build Docker image"
	@echo "  make docker-up       - Start services (production)"
	@echo "  make docker-down     - Stop services"
	@echo "  make docker-logs     - View service logs"
	@echo "  make docker-shell    - Open shell in container"
	@echo "  make docker-ps       - Show running containers"
	@echo "  make docker-clean    - Clean up Docker resources"
	@echo ""
	@echo "Development Commands:"
	@echo "  make docker-dev      - Start development services (with postgres, redis)"
	@echo "  make docker-dev-down - Stop development services"
	@echo ""
	@echo "Sample Data:"
	@echo "  make seed-data       - Populate all six source tables"
	@echo ""

# Docker Production Commands
docker-build:
	docker-compose build

docker-up-build:
	docker-compose up --build

docker-up:
	docker-compose up
	@echo "✅ Services started"
	@echo "📍 API: http://localhost:8000"
	@echo "📖 Docs: http://localhost:8000/docs"

docker-down:
	docker-compose down
	@echo "✅ Services stopped"

docker-logs:
	docker-compose logs -f registry_api

docker-shell:
	docker-compose exec registry_api /bin/bash

docker-ps:
	docker-compose ps

docker-clean:
	docker-compose down -v
	docker system prune -f
	@echo "✅ Docker cleaned"

# Sample data for all six source tables. Recreates the schema from
# init-scripts/02-init-banking-schema.sql, so it starts from a clean slate.
seed-data:
	$(PY) banking-app/scripts/generate_banking_data.py --users 100 --transactions 5