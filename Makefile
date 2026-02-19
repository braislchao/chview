.PHONY: install test lint format run clean help

PYTHON := python3
PIP := pip

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

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

run: ## Run Streamlit app
	streamlit run src/chview/app.py

clean: ## Clean build artifacts
	rm -rf build/ dist/ *.egg-info/ .pytest_cache/ .mypy_cache/ htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete

demo: ## Run with demo data (no ClickHouse needed)
	@echo "Demo mode coming in Phase 2"

update-deps: ## Update dependencies
	$(PIP) install --upgrade -e ".[dev,demo]"
