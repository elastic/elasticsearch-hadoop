#!/usr/bin/env bash
# Generates the DRA workflow pipeline YAML, injecting the version from
# buildSrc/esh-version.properties so it is always current for the branch
# being built.
set -euo pipefail

STACK_VERSION=$(grep '^eshadoop' buildSrc/esh-version.properties | sed 's/eshadoop *= *//')
if [[ -n "${VERSION_QUALIFIER:-}" ]]; then
  STACK_VERSION="${STACK_VERSION}-${VERSION_QUALIFIER}"
fi

DRA_PREP_VERSION="v0.1.6"

# Use a regular heredoc so bash substitutes $STACK_VERSION and $DRA_PREP_VERSION.
# Buildkite runtime variables ($DRA_WORKFLOW) are escaped with \$ so bash leaves
# them as literal $ for Buildkite to resolve at build time.
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
    artifact_paths:
      - "dist/build/distributions/elasticsearch-hadoop-*.zip"
      - "build/distributions/dependencies-*.csv"

  - wait: ~

  - label: ":package: DRA Prep"
    key: dra-prep
    command: ".buildkite/stage_artifacts.sh"
    if: 'build.env("DRA_WORKFLOW") == "snapshot" || (build.branch !~ /^main$/ && build.branch !~ /^[0-9]+\.x$/)'
    agents:
      image: "docker.elastic.co/ci-agent-images/ubuntu-build-essential:latest"
    plugins:
      - elastic/dra-prep#${DRA_PREP_VERSION}:
          product_id: "elasticsearch-hadoop"
          stack_version: "${STACK_VERSION}"
          workflow: "\${DRA_WORKFLOW}"

  - label: ":pipeline: DRA processing for elasticsearch-hadoop / ${STACK_VERSION} / \${DRA_WORKFLOW}"
    trigger: "unified-release-dra-processing"
    async: true
    depends_on: "dra-prep"
    if: 'build.env("DRA_WORKFLOW") == "snapshot" || (build.branch !~ /^main$/ && build.branch !~ /^[0-9]+\.x$/)'
    build:
      env:
        DRA_PRODUCT_ID: "elasticsearch-hadoop"
        DRA_STACK_VERSION: "${STACK_VERSION}"
        DRA_WORKFLOW: "\${DRA_WORKFLOW}"
PIPELINE
