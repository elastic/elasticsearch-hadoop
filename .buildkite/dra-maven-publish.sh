#!/bin/bash

# Publishes the exploded maven tree produced by :prepareDraSnapshotMavenAggregation
# straight into the consumer-facing root prefixes on snapshots.elastic.co
# (snapshot workflow) or artifacts.elastic.co (staging workflow):
#
#   s3://<bucket>/maven/<groupPath>/<artifact>/<version>/<file>
#   s3://<bucket>/javadoc/<groupPath>/<artifact>/<version>/<html-tree>
#
# For each `*-javadoc.jar` in the maven tree we also unpack the browsable HTML
# tree under `javadoc/<groupPath>/<artifact>/<version>/`.
#
# The version is already encoded in the exploded maven tree's directory layout
# and the S3 target is the root `maven/` prefix, so no version env var is needed.
# `MAVEN_AGGREGATION_DIR` overrides the source location for standalone use.
#
# Required environment:
#   DRA_WORKFLOW           snapshot|staging (default: snapshot)
#   AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY [/ AWS_SESSION_TOKEN]
#                          exported via USE_MAVEN_S3_CREDENTIALS in pre-command

set -euo pipefail

DRA_WORKFLOW="${DRA_WORKFLOW:-snapshot}"

case "$DRA_WORKFLOW" in
  snapshot) BUCKET="snapshots.elastic.co" ;;
  staging)  BUCKET="artifacts.elastic.co" ;;
  *) echo "unsupported DRA_WORKFLOW='$DRA_WORKFLOW'" >&2; exit 2 ;;
esac

MAVEN_DIR="${MAVEN_AGGREGATION_DIR:-build/dra-maven-aggregation}"

# Sanity guard: snapshot-versioned artifacts must not land in the staging bucket.
if [[ "$DRA_WORKFLOW" == "staging" ]] && find "$MAVEN_DIR" -name '*-SNAPSHOT*' -maxdepth 4 -print -quit 2>/dev/null | grep -q .; then
  echo "ERROR: staging workflow but SNAPSHOT artifacts found in $MAVEN_DIR — aborting." >&2
  exit 2
fi

if [[ ! -d "$MAVEN_DIR" ]]; then
  echo "DRA maven aggregation tree not found: $MAVEN_DIR" >&2
  echo "  (produced by :prepareDraSnapshotMavenAggregation)" >&2
  exit 1
fi

WORK_DIR="$(mktemp -d -t esh-maven-publish.XXXXXX)"
trap 'rm -rf "$WORK_DIR"' EXIT

JAVADOC_DIR="$WORK_DIR/javadoc"
mkdir -p "$JAVADOC_DIR"

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
