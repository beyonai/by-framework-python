# Makefile for the by-framework workspace

SHELL := /bin/bash

ROOT_PROJECT := .
LIB_PROJECTS := $(patsubst %/pyproject.toml,%,$(wildcard libs/*/pyproject.toml))
PYTHON_PROJECTS := $(ROOT_PROJECT) $(LIB_PROJECTS)
PROJECTS ?= $(PYTHON_PROJECTS)
FILES ?=
PRE_COMMIT_FILE_ARGS := $(if $(strip $(FILES)),--files $(FILES),--all-files)

.PHONY: all help list-projects install format lint format-changed lint-changed test ci clean

all: format lint test

ci: install lint test

help:
	@echo "Workspace commands:"
	@echo "  make list-projects          # Show managed Python projects"
	@echo "  make install                # Sync dependencies for all projects"
	@echo "  make format                 # Format Python code for all projects"
	@echo "  make format-changed         # Format files changed from HEAD plus untracked files"
	@echo "  make lint                   # Lint Python code for all projects"
	@echo "  make lint-changed           # Lint files changed from HEAD plus untracked files"
	@echo "  make test                   # Run tests for all projects"
	@echo "  make ci                     # Install, lint, and test all projects"
	@echo "  make clean                  # Remove caches and build artifacts"
	@echo ""
	@echo "Optional override:"
	@echo "  make test PROJECTS='libs/by-framework-history-postgres'"
	@echo "  make lint FILES='src/foo.py tests/test_foo.py'"
	@echo "  make format FILES='src/foo.py'"

list-projects:
	@for project in $(PROJECTS); do \
		echo $$project; \
	done

install:
	@for project in $(PROJECTS); do \
		echo "==> Syncing $$project"; \
		(cd $$project && uv sync --all-extras); \
	done

format:
	@PROJECTS="$(PROJECTS)" ./scripts/python_quality.sh format $(FILES)
	@uv run --extra dev pre-commit run trailing-whitespace $(PRE_COMMIT_FILE_ARGS) || uv run --extra dev pre-commit run trailing-whitespace $(PRE_COMMIT_FILE_ARGS)
	@uv run --extra dev pre-commit run end-of-file-fixer $(PRE_COMMIT_FILE_ARGS) || uv run --extra dev pre-commit run end-of-file-fixer $(PRE_COMMIT_FILE_ARGS)
	@uv run --extra dev pre-commit run mixed-line-ending $(PRE_COMMIT_FILE_ARGS) || uv run --extra dev pre-commit run mixed-line-ending $(PRE_COMMIT_FILE_ARGS)

lint:
	@PROJECTS="$(PROJECTS)" ./scripts/python_quality.sh lint $(FILES)
	@uv run --extra dev pre-commit run check-yaml $(PRE_COMMIT_FILE_ARGS)
	@uv run --extra dev pre-commit run check-toml $(PRE_COMMIT_FILE_ARGS)

format-changed:
	@changed_files="$$( \
		{ git diff --name-only --diff-filter=ACMR HEAD; git ls-files --others --exclude-standard; } \
		| awk 'NF' \
		| sort -u \
		| tr '\n' ' ' \
	)"; \
	if [ -z "$$changed_files" ]; then \
		echo "No changed files to format."; \
	else \
		$(MAKE) format PROJECTS="$(PROJECTS)" FILES="$$changed_files"; \
	fi

lint-changed:
	@changed_files="$$( \
		{ git diff --name-only --diff-filter=ACMR HEAD; git ls-files --others --exclude-standard; } \
		| awk 'NF' \
		| sort -u \
		| tr '\n' ' ' \
	)"; \
	if [ -z "$$changed_files" ]; then \
		echo "No changed files to lint."; \
	else \
		$(MAKE) lint PROJECTS="$(PROJECTS)" FILES="$$changed_files"; \
	fi

test:
	@set -e; for project in $(PROJECTS); do \
		echo "==> Testing $$project"; \
		(cd $$project && uv run --extra dev pytest); \
	done

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	rm -rf .pytest_cache .ruff_cache .mypy_cache .coverage
	rm -rf htmlcov
	rm -rf dist build *.egg-info
	rm -rf by-framework.log
	rm -rf libs/*/.pytest_cache libs/*/.ruff_cache libs/*/.mypy_cache
