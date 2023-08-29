#!/bin/bash

set -euo pipefail

echo "steps:"

source .buildkite/branches.sh

for BRANCH in "${BRANCHES[@]}"; do
  cat <<EOF
  - trigger: elasticsearch-hadoop-tests
    label: Trigger tests pipeline for $BRANCH
    async: true
    build:
      branch: $BRANCH
EOF
done



