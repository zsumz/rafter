#!/usr/bin/env bash
# Retry retrieval from the action's isolated signed source, then install once.
# The action bounds this entire operation with GNU timeout, including dpkg.
set -euo pipefail
trap 'exit 130' INT
trap 'exit 143' TERM

if [[ $# -lt 2 ]]; then
  echo "usage: install-packages.sh STATE PACKAGE=VERSION..." >&2
  exit 2
fi
state=$1
shift
for package in "$@"; do
  if [[ ! "$package" =~ ^[a-z0-9][a-z0-9+.-]*=[a-zA-Z0-9.+:~_-]+$ ]]; then
    echo "runtime package must have one exact version: $package" >&2
    exit 2
  fi
done
test -f "$state/ubuntu.sources"
test -d "$state/lists"
mkdir -m 0755 "$state/archives"

# Every attempt uses the same source, index directory, archive cache, and pins.
# APT verifies signatures and package hashes. No missing-package or trust bypass
# is enabled, including when every retrieval attempt fails.
apt_options=(
  -o "Dir::Etc::sourcelist=$state/ubuntu.sources"
  -o "Dir::Etc::sourceparts=-"
  -o "Dir::State::lists=$state/lists"
  -o "Dir::Cache::archives=$state/archives"
  -o Acquire::Retries=3
  -o Acquire::Languages=none
)
install_options=(
  install -y --no-install-recommends --no-remove --allow-downgrades --reinstall
)

retry_retrieval() {
  local phase=$1 attempt status delay
  shift
  for attempt in 1 2 3 4; do
    if sudo env DEBIAN_FRONTEND=noninteractive apt-get "${apt_options[@]}" "$@"; then
      return 0
    else
      status=$?
    fi
    # Only APT's ordinary failure code is retryable. Interruptions, timeout,
    # and command-execution failures retain their status and stop immediately.
    if [[ "$status" -ne 100 || "$attempt" -eq 4 ]]; then
      echo "runtime $phase failed on attempt $attempt (exit $status)" >&2
      return "$status"
    fi
    delay=$((10 << (attempt - 1)))
    echo "runtime $phase attempt $attempt failed; retrying in ${delay}s" >&2
    sleep "$delay"
  done
}

# Require every index, even when APT would otherwise tolerate a transient error.
retry_retrieval metadata update --error-on=any
retry_retrieval packages "${install_options[@]}" --download-only "$@"

# Retrieval retries do not run maintainer scripts or partially install packages.
# Installation has one attempt and cannot fetch anything missing from the cache.
sudo env DEBIAN_FRONTEND=noninteractive apt-get "${apt_options[@]}" \
  "${install_options[@]}" --no-download "$@"
for package in "$@"; do
  actual="$(dpkg-query -W -f='${Version}' "${package%%=*}")"
  if [[ "$actual" != "${package#*=}" ]]; then
    echo "runtime version mismatch: $package (installed $actual)" >&2
    exit 1
  fi
done
