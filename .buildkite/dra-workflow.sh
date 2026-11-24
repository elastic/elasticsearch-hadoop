#!/usr/bin/env bash
# Generates the DRA workflow pipeline YAML, injecting the version from
# buildSrc/esh-version.properties so it is always current for the branch
# being built.
#
# The dra-prep plugin is attached to the build step itself (its config needs
# literal values, which are known here), so the artifacts are published to GCS
# from the build agent without going through the Buildkite artifact store.
set -euo pipefail

WORKFLOW="${DRA_WORKFLOW:-snapshot}"
BRANCH="${BUILDKITE_BRANCH:-}"

# Don't publish main and *.x branches to staging
if [[ ("$BRANCH" == "main" || "$BRANCH" == *.x) && "$WORKFLOW" == "staging" ]]; then
  cat <<PIPELINE
steps:
  - label: ":no_entry: Skip DRA staging for $BRANCH"
    command: echo "DRA staging is not published for $BRANCH"
PIPELINE
  exit 0
fi

STACK_VERSION=$(grep '^eshadoop' buildSrc/esh-version.properties | sed 's/eshadoop *= *//')
if [[ -n "${VERSION_QUALIFIER:-}" ]]; then
  STACK_VERSION="${STACK_VERSION}-${VERSION_QUALIFIER}"
fi

DRA_PREP_VERSION="v0.1.6"

cat <<PIPELINE
steps:
  - label: ":gradle: DRA Build"
    key: dra-build
    command: .buildkite/dra.sh
    timeout_in_minutes: 60
    agents:
      provider: gcp
      image: family/elasticsearch-ubuntu-2404
      machineType: n2-standard-8
      diskType: pd-ssd
      diskSizeGb: 100
    env:
      USE_MAVEN_GPG: "true"
      USE_MAVEN_S3_CREDENTIALS: "true"
PIPELINE

# Only publish snapshots, or builds of a version branch (keeps manual
# feature-branch staging runs from publishing).
if [[ "$WORKFLOW" == "snapshot" || "$BRANCH" =~ ^[0-9]+\.[0-9]+$ ]]; then
  cat <<PIPELINE
    plugins:
      - elastic/dra-prep#${DRA_PREP_VERSION}:
          product_id: "elasticsearch-hadoop"
          stack_version: "${STACK_VERSION}"
          workflow: "${WORKFLOW}"

  - label: ":pipeline: DRA processing for elasticsearch-hadoop / ${STACK_VERSION} / ${WORKFLOW}"
    trigger: "unified-release-dra-processing"
    async: true
    depends_on: "dra-build"
    build:
      env:
        DRA_PRODUCT_ID: "elasticsearch-hadoop"
        DRA_STACK_VERSION: "${STACK_VERSION}"
        DRA_WORKFLOW: "${WORKFLOW}"
PIPELINE
fi
