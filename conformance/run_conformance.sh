#!/usr/bin/env bash
# Run deadline-cloud-python's test/cli_e2e/ suite against the locally-built
# Rust `deadline` binary. This is the conformance / cross-repo parity check:
# Python's own CLI tests act as the oracle for our Rust implementation.
#
# Usage:
#   ./conformance/run_conformance.sh                 # run the full cli_e2e suite
#   ./conformance/run_conformance.sh test/cli_e2e/test_farm.py   # a subset
#
# Env overrides:
#   DEADLINE_PYTHON_REPO   path to the deadline-cloud-python checkout
#                          (default: ../deadline-cloud-python relative to this repo)
#   PROFILE                debug | release   (default: debug)
#   CONFORMANCE_VENV       python venv to use (default: $DEADLINE_PYTHON_REPO/.venv)

set -euo pipefail

RS_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY_REPO="${DEADLINE_PYTHON_REPO:-$(cd "$RS_ROOT/.." && pwd)/deadline-cloud-python}"
PROFILE="${PROFILE:-debug}"

if [ ! -d "$PY_REPO" ]; then
  echo "ERROR: deadline-cloud-python not found at: $PY_REPO" >&2
  echo "Set DEADLINE_PYTHON_REPO to its location." >&2
  exit 1
fi

echo "Rust repo:   $RS_ROOT"
echo "Python repo: $PY_REPO"
echo "Profile:     $PROFILE"

# 1. Build the Rust deadline binary.
CARGO_FLAGS=""
[ "$PROFILE" = "release" ] && CARGO_FLAGS="--release"
( cd "$RS_ROOT" && cargo build -p deadline-cli $CARGO_FLAGS )
BIN_DIR="$RS_ROOT/target/$PROFILE"
[ -x "$BIN_DIR/deadline" ] || { echo "ERROR: deadline binary not found in $BIN_DIR" >&2; exit 1; }

# 2. Python venv + deps (Python package + OpenJD libs needed by the mock backend).
VENV="${CONFORMANCE_VENV:-$PY_REPO/.venv}"
if [ ! -x "$VENV/bin/python" ]; then
  echo "Creating venv at $VENV"
  python3 -m venv "$VENV"
fi
"$VENV/bin/pip" install --quiet --upgrade pip
"$VENV/bin/pip" install --quiet -e "$PY_REPO" pytest "moto[server]" openjd-sessions openjd-model

# 3. Run cli_e2e against the Rust binary (Rust binary wins on PATH).
export PATH="$BIN_DIR:$PATH"
export PYTHONPATH="$RS_ROOT/conformance:${PYTHONPATH:-}"

echo "Using deadline binary: $(command -v deadline)"

TARGETS=("$@")
[ ${#TARGETS[@]} -eq 0 ] && TARGETS=("test/cli_e2e/")

cd "$PY_REPO"
exec "$VENV/bin/python" -m pytest \
  -o addopts="" \
  -p no:cacheprovider \
  -p rust_conformance_plugin \
  "${TARGETS[@]}"
