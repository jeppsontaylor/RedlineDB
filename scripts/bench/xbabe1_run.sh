#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -eq 0 ]; then
  echo "usage: xbabe1_run.sh <docker command...>" >&2
  exit 1
fi

REMOTE="${REMOTE:-xbabe1}"
REMOTE_DIR="${REMOTE_DIR:-/home/ubuntu/RedlineDB}"
IMAGE="${IMAGE:-redlinedb-bench:1.95.0}"
REMOTE_COMMAND="$(printf '%q ' "$@")"

ssh "${REMOTE}" "cd '${REMOTE_DIR}' && docker build -f crates/bench/docker/Dockerfile -t '${IMAGE}' . && docker run --rm -u \$(id -u):\$(id -g) -e CARGO_TARGET_DIR=/work/target -v '${REMOTE_DIR}':/work -w /work '${IMAGE}' bash -lc $(printf '%q' "${REMOTE_COMMAND}")"
