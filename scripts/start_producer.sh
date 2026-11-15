#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python3 -m producer.transaction_producer "$@"
