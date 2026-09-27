#!/usr/bin/env bash
# Create .venv with the notebook dependencies and register its Jupyter kernel.
# Usage: bash scripts/setup_notebook.sh
set -euo pipefail
cd "$(dirname "$0")/.."
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-notebook.txt
.venv/bin/python -m ipykernel install --prefix "$PWD/.venv" --name cassandra-lab --display-name "Cassandra Lab (project)"
echo "Ready: bash scripts/start_notebook.sh"

