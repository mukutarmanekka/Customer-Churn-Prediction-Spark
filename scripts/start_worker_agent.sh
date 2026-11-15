#!/usr/bin/env bash
set -euo pipefail
PYTHON_BIN="${PYTHON_BIN:-python3.10}"
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "Python executable not found: $PYTHON_BIN" >&2
  exit 1
fi
if ! "$PYTHON_BIN" -c "import psutil" >/dev/null 2>&1; then
  echo "Install psutil first: $PYTHON_BIN -m pip install 'psutil>=5.9,<8'" >&2
  exit 1
fi
exec "$PYTHON_BIN" src/node_monitor_agent.py --host 0.0.0.0 --port 9108
