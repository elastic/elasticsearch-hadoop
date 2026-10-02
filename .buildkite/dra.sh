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

BUILD_TOOLS_VERSION="${HADOOP_VERSION}${VERSION_SUFFIX}"
case "$DRA_WORKFLOW" in
  snapshot)
    BUILD_TOOLS_MAVEN_REPO="https://snapshots.elastic.co/maven"
    ;;
  staging)
    BUILD_TOOLS_MAVEN_REPO="https://staging.elastic.co/maven"
    ;;
  *)
    echo "ERROR: unsupported DRA_WORKFLOW='$DRA_WORKFLOW'" >&2
    exit 2
    ;;
esac
BUILD_TOOLS_JAR_URL="$BUILD_TOOLS_MAVEN_REPO/org/elasticsearch/gradle/build-tools/${BUILD_TOOLS_VERSION}/build-tools-${BUILD_TOOLS_VERSION}.jar"

echo "DRA_WORKFLOW=$DRA_WORKFLOW"
echo "HADOOP_VERSION=$HADOOP_VERSION"
echo "VERSION_SUFFIX=$VERSION_SUFFIX"
echo "BUILD_ARGS=${BUILD_ARGS[@]}"
echo "BUILD_TOOLS_MAVEN_REPO=$BUILD_TOOLS_MAVEN_REPO"
echo "BUILD_TOOLS_JAR_URL=$BUILD_TOOLS_JAR_URL"

mkdir -p localRepo
curl -sS --fail "$BUILD_TOOLS_JAR_URL" \
  -o "localRepo/build-tools-${BUILD_TOOLS_VERSION}.jar" || {
  echo "ERROR: failed to download ES build-tools jar: $BUILD_TOOLS_JAR_URL" >&2
  exit 1
}

./gradlew -S -PlocalRepo=true "${BUILD_ARGS[@]}" -Dorg.gradle.warning.mode=summary -Dcsv="$WORKSPACE/build/distributions/dependencies-${HADOOP_VERSION}${VERSION_SUFFIX}.csv" :dist:generateDependenciesReport distribution zipAggregation prepareDraSnapshotMavenAggregation

find "$WORKSPACE" -type f -path "*/build/distributions/*" -exec chmod a+r {} \;

echo --- Publishing maven artifacts to S3

.buildkite/dra-maven-publish.sh
