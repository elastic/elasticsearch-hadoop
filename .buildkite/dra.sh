#!/bin/bash

set -euo pipefail

DRA_WORKFLOW=${DRA_WORKFLOW:-snapshot}

if [[ ("$BUILDKITE_BRANCH" == "main" || "$BUILDKITE_BRANCH" == *.x) && "$DRA_WORKFLOW" == "staging" ]]; then
  exit 0
fi

echo --- Creating distribution

rm -Rfv ~/.gradle/init.d
HADOOP_VERSION=$(grep eshadoop buildSrc/esh-version.properties | sed "s/eshadoop *= *//g")

VERSION_SUFFIX=""
declare -a BUILD_ARGS
BUILD_ARGS[0]="-Dbuild.snapshot=false"
if [[ "$DRA_WORKFLOW" == "snapshot" ]]; then
  VERSION_SUFFIX="-SNAPSHOT"
  BUILD_ARGS[0]="-Dbuild.snapshot=true"
fi

# DRA_BRANCH is used to resolve the correct ES artifacts for this branch.
# Override DRA_BRANCH to test against a different branch's artifacts.
DRA_BRANCH="${DRA_BRANCH:-$BUILDKITE_BRANCH}"
if [[ "$DRA_BRANCH" == "main" ]]; then
  DRA_BRANCH=master
fi

if [[ -n "${VERSION_QUALIFIER:-}" ]]; then
  BUILD_ARGS+=("-Dbuild.version_qualifier=$VERSION_QUALIFIER")
  HADOOP_VERSION="${HADOOP_VERSION}-${VERSION_QUALIFIER}"
fi

echo "DRA_WORKFLOW=$DRA_WORKFLOW"
echo "HADOOP_VERSION=$HADOOP_VERSION"
echo "DRA_BRANCH=$DRA_BRANCH"
echo "VERSION_SUFFIX=$VERSION_SUFFIX"
echo "BUILD_ARGS=${BUILD_ARGS[@]}"

ES_BUILD_ID=$(curl -sS "https://artifacts-$DRA_WORKFLOW.elastic.co/elasticsearch/latest/${DRA_BRANCH}.json" | jq -r '.build_id')
echo "ES_BUILD_ID=$ES_BUILD_ID"

mkdir localRepo
wget --quiet "https://artifacts-$DRA_WORKFLOW.elastic.co/elasticsearch/${ES_BUILD_ID}/maven/org/elasticsearch/gradle/build-tools/${HADOOP_VERSION}${VERSION_SUFFIX}/build-tools-${HADOOP_VERSION}${VERSION_SUFFIX}.jar" \
  -O "localRepo/build-tools-${HADOOP_VERSION}${VERSION_SUFFIX}.jar"

./gradlew -S -PlocalRepo=true "${BUILD_ARGS[@]}" -Dorg.gradle.warning.mode=summary -Dcsv="$WORKSPACE/build/distributions/dependencies-${HADOOP_VERSION}${VERSION_SUFFIX}.csv" :dist:generateDependenciesReport distribution zipAggregation prepareDraSnapshotMavenAggregation

find "$WORKSPACE" -type f -path "*/build/distributions/*" -exec chmod a+r {} \;

echo --- Publishing maven artifacts to S3

.buildkite/dra-maven-publish.sh
