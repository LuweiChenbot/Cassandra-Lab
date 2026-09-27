#!/usr/bin/env bash
# Open the demo notebook in JupyterLab (run setup_notebook.sh first).
# Usage: bash scripts/start_notebook.sh
set -euo pipefail
cd "$(dirname "$0")/.."
if [ ! -x .venv/bin/jupyter ]; then
  echo "请先运行 bash scripts/setup_notebook.sh" >&2
  exit 1
fi
export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"
exec .venv/bin/jupyter lab notebooks/Project_Demo.ipynb --ServerApp.ip=127.0.0.1

