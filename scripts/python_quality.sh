#!/usr/bin/env bash

set -euo pipefail

MODE="${1:-}"
shift || true

if [[ "$MODE" != "format" && "$MODE" != "lint" ]]; then
  echo "Usage: $0 <format|lint> [files...]" >&2
  exit 2
fi

ROOT_PROJECT="."
LIB_PROJECTS=()
while IFS= read -r path; do
  LIB_PROJECTS+=("${path%/pyproject.toml}")
done < <(find libs -mindepth 2 -maxdepth 2 -name pyproject.toml | sort)

if [[ -n "${PROJECTS:-}" ]]; then
  # shellcheck disable=SC2206
  PROJECTS=(${PROJECTS})
else
  PROJECTS=("$ROOT_PROJECT" "${LIB_PROJECTS[@]}")
fi

normalize_path() {
  local path="$1"
  if [[ "$path" == "./"* ]]; then
    printf '%s\n' "${path#./}"
  else
    printf '%s\n' "$path"
  fi
}

collect_targets_for_project() {
  local project="$1"
  shift || true

  local -a inputs=("$@")
  local normalized_project
  normalized_project="$(normalize_path "$project")"

  if [[ "${#inputs[@]}" -eq 0 ]]; then
    local -a defaults=()
    [[ -d "$project/src" ]] && defaults+=("src")
    [[ -d "$project/tests" ]] && defaults+=("tests")
    if [[ "${#defaults[@]}" -gt 0 ]]; then
      printf '%s\n' "${defaults[@]}"
    fi
    return
  fi

  local -a matches=()
  local raw normalized
  for raw in "${inputs[@]}"; do
    # Skip non-Python files (pylint only handles .py)
    [[ "$raw" != *.py ]] && continue
    normalized="$(normalize_path "$raw")"
    if [[ "$normalized_project" == "." ]]; then
      if [[ "$normalized" == src/* || "$normalized" == tests/* ]]; then
        matches+=("$normalized")
      fi
    elif [[ "$normalized" == "$normalized_project/"* ]]; then
      matches+=("${normalized#"$normalized_project/"}")
    fi
  done

  if [[ "${#matches[@]}" -gt 0 ]]; then
    printf '%s\n' "${matches[@]}"
  fi
}

run_for_project() {
  local project="$1"
  shift || true
  local -a targets=("$@")
  local action_label

  if [[ "${#targets[@]}" -eq 0 ]]; then
    return
  fi

  if [[ "$MODE" == "format" ]]; then
    action_label="Formatting"
  else
    action_label="Linting"
  fi

  echo "==> $action_label $project"
  if [[ "$MODE" == "format" ]]; then
    (
      cd "$project"
      uv run --extra dev isort "${targets[@]}"
      uv run --extra dev ruff check --fix "${targets[@]}"
      uv run --extra dev pyink --config pyproject.toml "${targets[@]}"
    )
  else
    (
      cd "$project"
      uv run --extra dev pylint "${targets[@]}"
      uv run --extra dev ruff check "${targets[@]}"
    )
  fi
}

declare -a INPUTS
if [[ "$#" -gt 0 ]]; then
  INPUTS=("$@")
else
  INPUTS=()
fi

for project in "${PROJECTS[@]}"; do
  targets=()
  if [[ "${#INPUTS[@]}" -gt 0 ]]; then
    while IFS= read -r target; do
      if [[ -n "$target" ]]; then
        targets+=("$target")
      fi
    done < <(collect_targets_for_project "$project" "${INPUTS[@]}")
  else
    while IFS= read -r target; do
      if [[ -n "$target" ]]; then
        targets+=("$target")
      fi
    done < <(collect_targets_for_project "$project")
  fi

  if [[ "${#targets[@]}" -gt 0 ]]; then
    run_for_project "$project" "${targets[@]}"
  else
    run_for_project "$project"
  fi
done
