#!/usr/bin/env bash
set -euo pipefail

REMOTE="${REMOTE:-xbabe1}"
REMOTE_DIR="${REMOTE_DIR:-/home/ubuntu/RedlineDB}"
LOCAL_ROOT="${LOCAL_ROOT:-target/bench/xbabe1}"
STAMP="${1:-}"

if [ -z "${STAMP}" ]; then
  LATEST_REMOTE="$(ssh "${REMOTE}" "ls -1dt '${REMOTE_DIR}'/target/bench/* 2>/dev/null | head -n 1 || true")"
  if [ -z "${LATEST_REMOTE}" ]; then
    echo "no remote benchmark artifacts found" >&2
    exit 1
  fi
  STAMP="$(basename "${LATEST_REMOTE}")"
fi

mkdir -p "${LOCAL_ROOT}/${STAMP}"
rsync -a --delete "${REMOTE}:${REMOTE_DIR}/target/bench/${STAMP}/" "${LOCAL_ROOT}/${STAMP}/"
