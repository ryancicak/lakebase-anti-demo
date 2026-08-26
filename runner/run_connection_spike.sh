#!/bin/sh
set -eu
umask 077

readonly RUNNER_ROOT=/opt/lakebase-anti-demo/round5
readonly PYTHON_BIN="${RUNNER_ROOT}/venv/bin/python3.12"
readonly RUNNER="${RUNNER_ROOT}/connection_spike_runner.py"

if [ "$#" -ne 1 ]; then
  printf '%s\n' 'RUNNER_ERROR:request_missing'
  exit 64
fi

if [ "$(id -u)" -ne 0 ]; then
  printf '%s\n' 'RUNNER_ERROR:root_required'
  exit 77
fi

exec "${PYTHON_BIN}" -I "${RUNNER}" "$1"
