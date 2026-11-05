#!/usr/bin/env bash
set -euo pipefail
if [[ "$GITHUB_REF" != refs/heads/master ]]; then
  echo "::error::Releases must be dispatched from master."
  exit 1
fi
mkdir -p "$RUNNER_TEMP/release-tooling"
cp scripts/release/{guard.py,signing.py,assets.py,credentials.py,publisher.asc,check-blockers.sh,setup-maven.sh} \
  Makefile "$RUNNER_TEMP/release-tooling/"
echo "RELEASE_GUARD=$RUNNER_TEMP/release-tooling/guard.py" >> "$GITHUB_ENV"
echo "RELEASE_SIGNING_HELPER=$RUNNER_TEMP/release-tooling/signing.py" >> "$GITHUB_ENV"
echo "RELEASE_MAKEFILE=$RUNNER_TEMP/release-tooling/Makefile" >> "$GITHUB_ENV"
echo "RELEASE_LOG_DIR=$RUNNER_TEMP/release-logs" >> "$GITHUB_ENV"
