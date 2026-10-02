#!/usr/bin/env bash
# Run every test. Exit 0 only when both suites pass.
set -euo pipefail
cd "$(dirname "$0")/.."
python3 tests/selftest.py > /tmp/bp-selftest.$$.log 2>&1 && echo "selftest: $(tail -1 /tmp/bp-selftest.$$.log)" || { cat /tmp/bp-selftest.$$.log | grep -E '^FAIL|FAILED'; rm -f /tmp/bp-selftest.$$.log; exit 1; }
rm -f /tmp/bp-selftest.$$.log
python3 tests/runner_selftest.py > /tmp/bp-runner.$$.log 2>&1 && echo "runner_selftest: $(tail -1 /tmp/bp-runner.$$.log)" || { cat /tmp/bp-runner.$$.log | grep -E '^FAIL|FAILED'; rm -f /tmp/bp-runner.$$.log; exit 1; }
rm -f /tmp/bp-runner.$$.log
