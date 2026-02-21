.PHONY: install test lint format run clean help \
        up up-demo down down-demo reset logs logs-traffic \
        demo demo-up demo-down demo-logs demo-traffic demo-reset

PYTHON := python3
PIP := pip
COMPOSE := docker compose

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install package in development mode with all dependencies
	$(PIP) install -e ".[dev,demo]"
	@echo "✓ Installation complete"

test: ## Run tests with coverage
	pytest tests/ -v --tb=short

lint: ## Run all linters (ruff, mypy)
	@echo "Running ruff..."
	ruff check src/chview tests
	@echo "Running mypy..."
	mypy src/chview

format: ## Format code with ruff
	ruff format src/chview tests

run: ## Run Streamlit app (requires .env with ClickHouse credentials)
	streamlit run src/chview/app.py

clean: ## Clean build artifacts
	rm -rf build/ dist/ *.egg-info/ .pytest_cache/ .mypy_cache/ htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete

# ── Docker ───────────────────────────────────────────────────────────────────

up: ## Start app only — connect via .env.docker (BYO ClickHouse)
	$(COMPOSE) up --build -d app
	@echo ""
	@echo "  CHView UI  →  http://localhost:8501"
	@echo ""
	@echo "Point .env.docker at your own ClickHouse and refresh."

up-demo: ## Start full demo: app + local ClickHouse + live traffic generator
	@echo "Starting CHView demo stack..."
	$(COMPOSE) --profile demo up --build -d
	@echo ""
	@echo "  CHView UI   →  http://localhost:8501"
	@echo "  ClickHouse  →  http://localhost:8123"
	@echo ""
	@echo "Run 'make logs' to follow logs, 'make down-demo' to stop."

down: ## Stop app container (data volume preserved)
	$(COMPOSE) down

down-demo: ## Stop all demo containers (data volume preserved)
	$(COMPOSE) --profile demo down

reset: ## Stop demo and DELETE ClickHouse data volume (fresh start)
	$(COMPOSE) --profile demo down -v

logs: ## Tail logs from all running services
	$(COMPOSE) logs -f

logs-traffic: ## Tail traffic generator logs only
	$(COMPOSE) logs -f traffic

# ── Backwards-compatible aliases ─────────────────────────────────────────────
demo:         up-demo  ## Alias for up-demo
demo-up:      up-demo  ## Alias for up-demo
demo-down:    down-demo ## Alias for down-demo
demo-reset:   reset    ## Alias for reset
demo-logs:    logs     ## Alias for logs
demo-traffic: logs-traffic ## Alias for logs-traffic

update-deps: ## Update dependencies
	$(PIP) install --upgrade -e ".[dev,demo]"
