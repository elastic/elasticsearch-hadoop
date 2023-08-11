#!/bin/bash

set -euo pipefail

echo "steps:"

source .buildkite/branches.sh

BRANCHES=(main 8.9 7.17)
for BRANCH in "${BRANCHES[@]}"; do
  cat <<EOF
  - trigger: elasticsearch-hadoop-dra-workflow
    label: Trigger DRA snapshot workflow for $BRANCH
    async: true
    build:
      branch: $BRANCH
      env:
        DRA_WORKFLOW: snapshot
  - trigger: elasticsearch-hadoop-dra-workflow
    label: Trigger DRA staging workflow for $BRANCH
    async: true
    build:
      branch: $BRANCH
      env:
        DRA_WORKFLOW: staging
EOF
done
