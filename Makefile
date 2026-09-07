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

# All ten projects share the workspace venv, so syncing them in a loop makes
# each one PRUNE the previous one's dependencies — the loop used to end with a
# venv holding only the last project's deps (and it uninstalled prometheus-client
# on the second iteration). --all-packages resolves the whole workspace once
# instead. The loop survives only for an explicit PROJECTS override, where
# narrowing the venv is what the caller asked for.
install:
	@if [ "$(strip $(PROJECTS))" = "$(strip $(PYTHON_PROJECTS))" ]; then \
		echo "==> Syncing workspace (all packages, all extras)"; \
		uv sync --all-extras --all-packages; \
	else \
		for project in $(PROJECTS); do \
			echo "==> Syncing $$project (narrowed: PROJECTS override)"; \
			(cd $$project && uv sync --all-extras); \
		done; \
	fi

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

# --all-extras, not --extra dev: `uv run` only ever adds what it is asked for,
# it never restores an extra it was not told about. So once `install`'s old
# per-project loop had uninstalled prometheus-client, `--extra dev` left it
# uninstalled and the root project's whole `observability` extra stayed missing
# for the entire test run. tests/metrics then ran against a build with no
# Prometheus registry, where the metric catalog check short-circuits and reports
# success without inspecting anything. State what the tests need, rather than
# inheriting whatever `install` happened to leave behind.
#
# Deliberately per-project rather than --all-packages: each project must be
# testable with only its own declared dependencies, or a package that quietly
# leans on a sibling's deps would still pass here and break on install.
test:
	@set -e; for project in $(PROJECTS); do \
		if ! find "$$project/tests" -type f \( -name 'test_*.py' -o -name '*_test.py' \) -print -quit 2>/dev/null | grep -q .; then \
			echo "==> Skipping $$project (no test files)"; \
			continue; \
		fi; \
		echo "==> Testing $$project"; \
		(cd $$project && uv run --all-extras pytest); \
	done

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	rm -rf .pytest_cache .ruff_cache .mypy_cache .coverage
	rm -rf htmlcov
	rm -rf dist build *.egg-info
	rm -rf by-framework.log
	rm -rf libs/*/.pytest_cache libs/*/.ruff_cache libs/*/.mypy_cache
