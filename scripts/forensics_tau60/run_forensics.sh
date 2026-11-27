#!/usr/bin/env bash
#
# run_forensics.sh
#
# End-to-end reproducible runner for the Polymarket BTC Up/Down 15m (tau < 60s)
# forensic opportunity study. Executes synthetic unit tests first, then runs
# all 4 pipeline stages over /data/polymarket/collector.
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-$REPO_ROOT/scripts/.venv/bin/python}"
export COLLECTOR_DATA_DIR="${COLLECTOR_DATA_DIR:-/data/polymarket/collector}"
export FORENSICS_WORK_DIR="${FORENSICS_WORK_DIR:-/var/tmp/polymarket-forensics-tau60}"
export PYTHONPATH="$SCRIPT_DIR"

echo "=== [0/4] Running deterministic unit tests ==="
"$PYTHON_BIN" -m unittest discover -s "$SCRIPT_DIR" -p "test_*.py" -v

echo "=== [1/4] Auditing 46-day market completeness & settlement ==="
"$PYTHON_BIN" "$SCRIPT_DIR/01_audit_and_settlement.py"

echo "=== [2/4] Running CEX -> Chainlink latency-lead study ==="
"$PYTHON_BIN" "$SCRIPT_DIR/02_cex_oracle_lead.py"

echo "=== [3/4] Running 8-core L2 Taker & Maker simulator (tau < 60s) ==="
"$PYTHON_BIN" "$SCRIPT_DIR/03_clob_tau60_simulator.py"

echo "=== [4/4] Computing opportunity surfaces & Walk-Forward OOS tables ==="
"$PYTHON_BIN" "$SCRIPT_DIR/04_aggregate_and_walkforward.py"

echo "=== Forensic pipeline finished. Outputs in: $FORENSICS_WORK_DIR ==="
