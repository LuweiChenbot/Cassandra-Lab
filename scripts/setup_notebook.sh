#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-notebook.txt
.venv/bin/python -m ipykernel install --prefix "$PWD/.venv" --name cassandra-lab --display-name "Cassandra Lab (project)"
echo "Ready: bash scripts/start_notebook.sh"

