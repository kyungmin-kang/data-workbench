#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR=".venv"
PYTHON_OVERRIDE="${WORKBENCH_BOOTSTRAP_PYTHON:-}"
SKIP_INSTALL="${WORKBENCH_BOOTSTRAP_SKIP_INSTALL:-0}"
DRY_RUN="${WORKBENCH_BOOTSTRAP_DRY_RUN:-0}"
MIN_MAJOR=3
MIN_MINOR=11

usage() {
  cat <<'EOF'
Usage: ./scripts/bootstrap_venv.sh [--python /path/to/python] [--venv .venv-name] [--skip-install] [--dry-run]

Creates a local virtual environment with Python 3.11+ and installs the project in editable mode
with the public contributor extras: .[e2e,persistence]

Options:
  --python PATH_OR_CMD  Use a specific Python interpreter and enforce Python 3.11+
  --venv DIR            Virtualenv directory relative to repo root (default: .venv)
  --skip-install        Create/check the virtualenv but skip pip installs
  --dry-run             Only report which interpreter would be used
  -h, --help            Show this message
EOF
}

log() {
  printf '%s\n' "$*"
}

die() {
  printf 'bootstrap_venv: %s\n' "$*" >&2
  exit 1
}

resolve_python() {
  local candidate="$1"
  if [[ -z "$candidate" ]]; then
    return 1
  fi
  if [[ "$candidate" == */* ]]; then
    [[ -x "$candidate" ]] || return 1
    printf '%s\n' "$candidate"
    return 0
  fi
  command -v "$candidate" 2>/dev/null || return 1
}

python_version() {
  local python_cmd="$1"
  "$python_cmd" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")' 2>/dev/null
}

python_meets_requirement() {
  local version="$1"
  local major="${version%%.*}"
  local minor="${version#*.}"
  [[ "$major" =~ ^[0-9]+$ ]] || return 1
  [[ "$minor" =~ ^[0-9]+$ ]] || return 1
  if (( major > MIN_MAJOR )); then
    return 0
  fi
  if (( major == MIN_MAJOR && minor >= MIN_MINOR )); then
    return 0
  fi
  return 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --python)
      shift
      [[ $# -gt 0 ]] || die "missing value for --python"
      PYTHON_OVERRIDE="$1"
      ;;
    --venv)
      shift
      [[ $# -gt 0 ]] || die "missing value for --venv"
      VENV_DIR="$1"
      ;;
    --skip-install)
      SKIP_INSTALL=1
      ;;
    --dry-run)
      DRY_RUN=1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      usage >&2
      die "unknown argument: $1"
      ;;
  esac
  shift
done

selected_python=""
selected_version=""
declare -a rejected_versions=()

if [[ -n "$PYTHON_OVERRIDE" ]]; then
  resolved_python="$(resolve_python "$PYTHON_OVERRIDE")" || die "python interpreter not found: $PYTHON_OVERRIDE"
  resolved_version="$(python_version "$resolved_python")" || die "unable to inspect Python version for: $resolved_python"
  python_meets_requirement "$resolved_version" || die "Python ${MIN_MAJOR}.${MIN_MINOR}+ is required, but $resolved_python is $resolved_version"
  selected_python="$resolved_python"
  selected_version="$resolved_version"
else
  for candidate in python3.13 python3.12 python3.11 python3 python; do
    resolved_python="$(resolve_python "$candidate")" || continue
    resolved_version="$(python_version "$resolved_python")" || continue
    if python_meets_requirement "$resolved_version"; then
      selected_python="$resolved_python"
      selected_version="$resolved_version"
      break
    fi
    rejected_versions+=("$resolved_python ($resolved_version)")
  done
fi

if [[ -z "$selected_python" ]]; then
  if (( ${#rejected_versions[@]} )); then
    die "Python ${MIN_MAJOR}.${MIN_MINOR}+ is required. Checked: ${rejected_versions[*]}"
  fi
  die "Python ${MIN_MAJOR}.${MIN_MINOR}+ is required but no usable interpreter was found. Install python3.11+ or pass --python."
fi

log "Selected Python interpreter: $selected_python ($selected_version)"

if [[ "$DRY_RUN" == "1" ]]; then
  exit 0
fi

venv_path="$ROOT_DIR/$VENV_DIR"
venv_python="$venv_path/bin/python"

if [[ -x "$venv_python" ]]; then
  existing_version="$(python_version "$venv_python")" || die "existing virtualenv at $VENV_DIR is broken; remove it and rerun the bootstrap"
  python_meets_requirement "$existing_version" || die "existing virtualenv at $VENV_DIR uses Python $existing_version. Remove it and rerun with Python ${MIN_MAJOR}.${MIN_MINOR}+."
  log "Using existing virtual environment: $VENV_DIR ($existing_version)"
else
  log "Creating virtual environment at $VENV_DIR"
  "$selected_python" -m venv "$venv_path"
fi

if [[ ! -x "$venv_python" ]]; then
  die "virtualenv python not found at $venv_python"
fi

if [[ "$SKIP_INSTALL" == "1" ]]; then
  log "Skipping dependency install."
  exit 0
fi

log "Upgrading pip"
"$venv_python" -m pip install --upgrade pip

log "Installing editable project with e2e and persistence extras"
(
  cd "$ROOT_DIR"
  "$venv_python" -m pip install -e ".[e2e,persistence]"
)

log
log "Bootstrap complete."
log "Activate with: . \"$VENV_DIR/bin/activate\""
