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

if [[ -n "${VERSION_QUALIFIER:-}" ]]; then
  BUILD_ARGS+=("-Dbuild.version_qualifier=$VERSION_QUALIFIER")
  HADOOP_VERSION="${HADOOP_VERSION}-${VERSION_QUALIFIER}"
fi

echo "DRA_WORKFLOW=$DRA_WORKFLOW"
echo "HADOOP_VERSION=$HADOOP_VERSION"
echo "VERSION_SUFFIX=$VERSION_SUFFIX"
echo "BUILD_ARGS=${BUILD_ARGS[@]}"

./gradlew -S "${BUILD_ARGS[@]}" -Dorg.gradle.warning.mode=summary -Dcsv="$WORKSPACE/build/distributions/dependencies-${HADOOP_VERSION}${VERSION_SUFFIX}.csv" :dist:generateDependenciesReport distribution zipAggregation prepareDraSnapshotMavenAggregation

find "$WORKSPACE" -type f -path "*/build/distributions/*" -exec chmod a+r {} \;

echo --- Publishing maven artifacts to S3

.buildkite/dra-maven-publish.sh
