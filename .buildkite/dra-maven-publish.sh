#!/bin/bash

# Publishes the maven aggregation zip produced by :zipDraSnapshotMavenAggregation
# straight into the consumer-facing root prefixes on snapshots.elastic.co
# (snapshot workflow) or artifacts.elastic.co (staging workflow):
#
#   s3://<bucket>/maven/<groupPath>/<artifact>/<version>/<file>
#   s3://<bucket>/javadoc/<groupPath>/<artifact>/<version>/<html-tree>
#
# For each `*-javadoc.jar` in the maven tree we also unpack the browsable HTML
# tree under `javadoc/<groupPath>/<artifact>/<version>/`.
#
# Required environment:
#   DRA_WORKFLOW           snapshot|staging (default: snapshot)
#   HADOOP_VERSION         version incl. optional -<qualifier>, e.g. 9.6.0
#   VERSION_SUFFIX         "-SNAPSHOT" for snapshots, empty for staging
#   AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY [/ AWS_SESSION_TOKEN]
#                          exported via USE_MAVEN_S3_CREDENTIALS in pre-command

set -euo pipefail

DRA_WORKFLOW="${DRA_WORKFLOW:-snapshot}"
: "${HADOOP_VERSION:?HADOOP_VERSION must be set}"
VERSION_SUFFIX="${VERSION_SUFFIX-}"

case "$DRA_WORKFLOW" in
  snapshot) BUCKET="snapshots.elastic.co" ;;
  staging)  BUCKET="artifacts.elastic.co" ;;
  *) echo "unsupported DRA_WORKFLOW='$DRA_WORKFLOW'" >&2; exit 2 ;;
esac

ZIP="${MAVEN_AGGREGATION_ZIP:-build/distributions/elasticsearch-hadoop-dra-maven-aggregation-${HADOOP_VERSION}${VERSION_SUFFIX}.zip}"
if [[ ! -f "$ZIP" ]]; then
  echo "DRA aggregation zip not found: $ZIP" >&2
  exit 1
fi

WORK_DIR="$(mktemp -d -t esh-maven-publish.XXXXXX)"
trap 'rm -rf "$WORK_DIR"' EXIT

MAVEN_DIR="$WORK_DIR/maven"
JAVADOC_DIR="$WORK_DIR/javadoc"
mkdir -p "$MAVEN_DIR" "$JAVADOC_DIR"

echo "--- Unpacking $ZIP"
unzip -q "$ZIP" -d "$MAVEN_DIR"

echo "--- Expanding javadoc jars"
find "$MAVEN_DIR" -type f -name '*-javadoc.jar' -print0 | while IFS= read -r -d '' jar; do
  rel="${jar#"$MAVEN_DIR/"}"
  dir="$(dirname "$rel")"
  target="$JAVADOC_DIR/$dir"
  mkdir -p "$target"
  unzip -q -o "$jar" -d "$target"
done

echo "--- Publishing to s3://$BUCKET/{maven,javadoc}/"
# Use `cp --recursive` rather than `sync`: sync needs s3:ListBucket to diff the
# remote against the local tree, which the `unified-release-maven` role does
# not grant (only object-level Put/Get on `maven/*` and `javadoc/*`).
aws s3 cp --recursive --no-progress --only-show-errors \
  "$MAVEN_DIR/"   "s3://$BUCKET/maven/"
aws s3 cp --recursive --no-progress --only-show-errors \
  "$JAVADOC_DIR/" "s3://$BUCKET/javadoc/"

echo "Published to:"
echo "  https://$BUCKET/maven/"
echo "  https://$BUCKET/javadoc/"
