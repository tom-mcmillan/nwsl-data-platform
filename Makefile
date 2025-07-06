.PHONY: help install install-dev test lint format clean setup

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install production dependencies
	pip install -r requirements.txt

install-dev: ## Install development dependencies
	pip install -r requirements.txt -r requirements-dev.txt

test: ## Run tests
	pytest

test-cov: ## Run tests with coverage
	pytest --cov=src/nwsl_analytics --cov-report=html

lint: ## Run linting
	flake8 src/ tests/
	mypy src/

format: ## Format code
	black src/ tests/
	isort src/ tests/

clean: ## Clean up cache and build files
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	rm -rf .pytest_cache/
	rm -rf htmlcov/
	rm -rf .coverage

ingest: ## Run data ingestion
	python scripts/ingest_data.py

server: ## Start MCP server
	python scripts/start_mcp_server.py

analyze: ## Run sample analysis
	python scripts/run_analysis.py
