#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
streamlit run dashboard/app.py --server.address 0.0.0.0 --server.port "${STREAMLIT_PORT:-8501}"
