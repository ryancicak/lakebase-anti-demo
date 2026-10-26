#!/bin/sh
# Round 5's runner on Azure: run_connection_spike.sh's contract, through azure_runner.py, which puts
# the harness's two AWS clients on Service Bus and Key Vault. run_connection_spike.sh is installed
# beside this, byte for byte, because it is one of the files the harness digests.
set -eu
umask 077

readonly RUNNER_ROOT=/opt/lakebase-anti-demo/round5
readonly PYTHON_BIN="${RUNNER_ROOT}/venv/bin/python3.12"
readonly RUNNER="${RUNNER_ROOT}/azure_runner.py"

if [ "$#" -eq 5 ] && [ "$1" = "--resident-agent" ]; then
  exec "${PYTHON_BIN}" -I "${RUNNER}" "$@"
fi

if [ "$#" -ne 1 ]; then
  printf '%s\n' 'RUNNER_ERROR:request_missing'
  exit 64
fi

if [ "$(id -u)" -ne 0 ]; then
  printf '%s\n' 'RUNNER_ERROR:root_required'
  exit 77
fi

exec "${PYTHON_BIN}" -I "${RUNNER}" "$1"
