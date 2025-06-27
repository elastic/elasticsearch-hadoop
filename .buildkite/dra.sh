#!/bin/bash

set -euo pipefail

DRA_WORKFLOW=${DRA_WORKFLOW:-snapshot}

if [[ ("$BUILDKITE_BRANCH" == "main" || "$BUILDKITE_BRANCH" == *.x) && "$DRA_WORKFLOW" == "staging" ]]; then
  exit 0
fi

echo --- Creating distribution

rm -Rfv ~/.gradle/init.d
HADOOP_VERSION=$(grep eshadoop buildSrc/esh-version.properties | sed "s/eshadoop *= *//g")
BASE_VERSION="$HADOOP_VERSION"

VERSION_SUFFIX=""
declare -a BUILD_ARGS
BUILD_ARGS[0]="-Dbuild.snapshot=false"
if [[ "$DRA_WORKFLOW" == "snapshot" ]]; then
  VERSION_SUFFIX="-SNAPSHOT"
  BUILD_ARGS[0]="-Dbuild.snapshot=true"
fi

RM_BRANCH="$BUILDKITE_BRANCH"
if [[ "$BUILDKITE_BRANCH" == "main" ]]; then
  RM_BRANCH=master
fi

if [[ -n "${VERSION_QUALIFIER:-}" ]]; then
  BUILD_ARGS+=("-Dbuild.version_qualifier=$VERSION_QUALIFIER")
  HADOOP_VERSION="${HADOOP_VERSION}-${VERSION_QUALIFIER}"
fi

echo "DRA_WORKFLOW=$DRA_WORKFLOW"
echo "HADOOP_VERSION=$HADOOP_VERSION"
echo "RM_BRANCH=$RM_BRANCH"
echo "VERSION_SUFFIX=$VERSION_SUFFIX"
echo "BUILD_ARGS=${BUILD_ARGS[@]}"

ES_BUILD_ID=$(curl -sS "https://artifacts-$DRA_WORKFLOW.elastic.co/elasticsearch/latest/${RM_BRANCH}.json" | jq -r '.build_id')
echo "ES_BUILD_ID=$ES_BUILD_ID"

mkdir localRepo
wget --quiet "https://artifacts-$DRA_WORKFLOW.elastic.co/elasticsearch/${ES_BUILD_ID}/maven/org/elasticsearch/gradle/build-tools/${HADOOP_VERSION}${VERSION_SUFFIX}/build-tools-${HADOOP_VERSION}${VERSION_SUFFIX}.jar" \
  -O "localRepo/build-tools-${HADOOP_VERSION}${VERSION_SUFFIX}.jar"

./gradlew -S -PlocalRepo=true "${BUILD_ARGS[@]}" -Dorg.gradle.warning.mode=summary -Dcsv="$WORKSPACE/build/distributions/dependencies-${HADOOP_VERSION}${VERSION_SUFFIX}.csv" :dist:generateDependenciesReport distribution zipAggregation

# Allow other users access to read the artifacts so they are readable in the container
find "$WORKSPACE" -type f -path "*/build/distributions/*" -exec chmod a+r {} \;

# Allow other users write access to create checksum files
find "$WORKSPACE" -type d -path "*/build/distributions" -exec chmod a+w {} \;

echo --- Running release-manager

docker run --rm \
  --name release-manager \
  -e VAULT_ADDR="$DRA_VAULT_ADDR" \
  -e VAULT_ROLE_ID="$DRA_VAULT_ROLE_ID_SECRET" \
  -e VAULT_SECRET_ID="$DRA_VAULT_SECRET_ID_SECRET" \
  --mount type=bind,readonly=false,src="$PWD",target=/artifacts \
  docker.elastic.co/infra/release-manager:latest \
  cli collect \
  --project elasticsearch-hadoop \
  --branch "$RM_BRANCH" \
  --commit "$BUILDKITE_COMMIT" \
  --workflow "$DRA_WORKFLOW" \
  --qualifier "${VERSION_QUALIFIER:-}" \
  --version "$BASE_VERSION" \
  --artifact-set main
