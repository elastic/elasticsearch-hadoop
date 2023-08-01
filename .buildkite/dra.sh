#!/bin/bash

set -euo pipefail

DRA_WORKFLOW=${DRA_WORKFLOW:-snapshot}

if [[ "$BUILDKITE_BRANCH" == "main" && "$DRA_WORKFLOW" == "staging" ]]; then
  exit 0
fi

rm -Rfv ~/.gradle/init.d
HADOOP_VERSION=$(grep eshadoop buildSrc/esh-version.properties | sed "s/eshadoop *= *//g")

VERSION_SUFFIX=""
BUILD_ARGS="-Dbuild.snapshot=false"
if [[ "$DRA_WORKFLOW" == "snapshot" ]]; then
  VERSION_SUFFIX="-SNAPSHOT"
  BUILD_ARGS="-Dbuild.snapshot=true"
fi

RM_BRANCH="$BUILDKITE_BRANCH"
if [[ "$BUILDKITE_BRANCH" == "main" ]]; then
  RM_BRANCH=master
fi

ES_BUILD_ID=$(curl -sS "https://artifacts-$DRA_WORKFLOW.elastic.co/elasticsearch/latest/${RM_BRANCH}.json" | jq -r '.build_id')
mkdir localRepo
wget --quiet "https://artifacts-$DRA_WORKFLOW.elastic.co/elasticsearch/${ES_BUILD_ID}/maven/org/elasticsearch/gradle/build-tools/${HADOOP_VERSION}${VERSION_SUFFIX}/build-tools-${HADOOP_VERSION}${VERSION_SUFFIX}.jar" \
  -O "localRepo/build-tools-${HADOOP_VERSION}${VERSION_SUFFIX}.jar"

./gradlew -S -PlocalRepo=true "${BUILD_ARGS}" -Dorg.gradle.warning.mode=summary -Dcsv="$WORKSPACE/build/distributions/dependencies-${HADOOP_VERSION}${VERSION_SUFFIX}.csv" :dist:generateDependenciesReport distribution

# Allow other users access to read the artifacts so they are readable in the container
find "$WORKSPACE" -type f -path "*/build/distributions/*" -exec chmod a+r {} \;

# Allow other users write access to create checksum files
find "$WORKSPACE" -type d -path "*/build/distributions" -exec chmod a+w {} \;

docker run --rm \
  --name release-manager \
  -e VAULT_ADDR \
  -e VAULT_ROLE_ID \
  -e VAULT_SECRET_ID \
  --mount type=bind,readonly=false,src="$PWD",target=/artifacts \
  docker.elastic.co/infra/release-manager:latest \
  cli collect \
  --project elasticsearch-hadoop \
  --branch "$RM_BRANCH" \
  --commit "$GIT_COMMIT" \
  --workflow "$DRA_WORKFLOW" \
  --version "$HADOOP_VERSION" \
  --artifact-set main