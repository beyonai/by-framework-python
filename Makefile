# Makefile for byclaw-gateway-sdk-python

.PHONY: all install format lint test clean

# Default target
all: format lint test

# Install dependencies using uv
install:
	uv sync --all-extras

# Format code using isort, ruff and pyink (matches autoformat.sh)
format:
	@echo "Organizing imports and fixing common issues..."
	uv run isort src/ tests/
	uv run ruff check --fix src/ tests/
	@echo "Auto-formatting code..."
	uv run find -L src/ -not -path "*/.*" -type f -name "*.py" -exec pyink --config pyproject.toml {} +
	uv run find -L tests/ -not -path "*/.*" -type f -name "*.py" -exec pyink --config pyproject.toml {} +

# Linting using pylint and ruff
lint:
	uv run pylint src/ tests/
	uv run ruff check src/ tests/

# Run tests
test:
	uv run pytest

# Clean up temporary files
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	rm -rf .pytest_cache .ruff_cache .mypy_cache .coverage
	rm -rf htmlcov
	rm -rf dist build *.egg-info
	rm -rf gateway-sdk.log
