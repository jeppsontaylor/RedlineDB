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

# Build the image, then capture its digest so the certify manifest can record
# the exact image used. Prefer a RepoDigest (set when the image has been
# pushed/pulled); fall back to the local image ID otherwise.
ssh "${REMOTE}" "cd '${REMOTE_DIR}' && docker build -f crates/bench/docker/Dockerfile -t '${IMAGE}' ."

REDLINEDB_BENCH_IMAGE_DIGEST="$(ssh "${REMOTE}" "docker inspect --format '{{if .RepoDigests}}{{index .RepoDigests 0}}{{else}}{{.Id}}{{end}}' '${IMAGE}'")"
export REDLINEDB_BENCH_IMAGE_DIGEST

ssh "${REMOTE}" "cd '${REMOTE_DIR}' && docker run --rm -u \$(id -u):\$(id -g) -e CARGO_TARGET_DIR=/work/target -e REDLINEDB_BENCH_IMAGE_DIGEST='${REDLINEDB_BENCH_IMAGE_DIGEST}' -v '${REMOTE_DIR}':/work -w /work '${IMAGE}' bash -lc $(printf '%q' "${REMOTE_COMMAND}")"
