#!/usr/bin/env bash
set -euo pipefail

WORKFLOW="${DRA_WORKFLOW:-snapshot}"

echo "--- :compression: Downloading ${WORKFLOW} build artifacts"

buildkite-agent artifact download "dist/build/distributions/elasticsearch-hadoop-*.zip" .
buildkite-agent artifact download "build/distributions/dependencies-*.csv" .

echo "--- :package: Staging ${WORKFLOW} artifacts"
mkdir -p artifacts

cp dist/build/distributions/elasticsearch-hadoop-*.zip artifacts/
cp build/distributions/dependencies-*.csv artifacts/

if ! ls artifacts/* 1>/dev/null 2>&1; then
  echo "ERROR: no ${WORKFLOW} artifacts staged." >&2
  exit 1
fi

echo "Staged artifacts:"
ls -1 artifacts/
