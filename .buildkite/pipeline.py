#!/usr/bin/env python

import glob
import json
import os
import re
from typing import Dict

# Note: If you'd like to add any debug info here, make sure to do it on stderr
# stdout will be fed into `buildkite-agent pipeline upload`

# Discover variants from SQL module build files (spark/sql-30, spark/sql-35, etc.)
# Each module declares its own setDefaultVariant / addFeatureVariant lines, which
# is the authoritative list of what integration-test steps CI should run.
# New modules (sql-40, sql-41, ...) are picked up automatically when added.
groupingsBySparkVersion: Dict[str, list[str]] = {}
for buildFile in sorted(glob.glob("spark/sql-*/build.gradle")):
    with open(buildFile, "r") as f:
        content = f.read()
    # `Variant "spark35scala212"` => ["35", "212"]
    for grouping in re.findall(r'Variant +"spark([0-9]+)scala([0-9]+)"', content):
        sparkVer, scalaVer = grouping
        if sparkVer not in groupingsBySparkVersion:
            groupingsBySparkVersion[sparkVer] = []
        if scalaVer not in groupingsBySparkVersion[sparkVer]:
            groupingsBySparkVersion[sparkVer].append(scalaVer)

gradlePropertiesFile = open("gradle.properties", "r")
gradleProperties = gradlePropertiesFile.read()
gradlePropertiesFile.close()
# `scala210Version = 2.10.7` => ["210", "2.10.7"]
matches = re.findall(
    r"scala([0-9]+)Version *= *([0-9]+\.[0-9]+\.[0-9]+)", gradleProperties
)

scalaVersions = {}
for match in matches:
    scalaVersions[match[0]] = match[1]


pipeline = {
    "agents": {
        "provider": "gcp",
        "image": "family/elasticsearch-ubuntu-2404",
        "machineType": "n2-standard-8",
        "diskType": "pd-ssd",
        "diskSizeGb": "100",
        "useVault": "false",
    },
    "steps": [],
}

intakeTasks = map(
    lambda sparkVersion: f"-x :elasticsearch-spark-{sparkVersion}:integrationTest",
    groupingsBySparkVersion.keys(),
)


pipeline["steps"].append(
    {
        "label": "intake",
        "timeout_in_minutes": 240,
        "command": "./gradlew check " + " ".join(intakeTasks),
    }
)

for sparkVersion in groupingsBySparkVersion.keys():
    for scalaVersion in groupingsBySparkVersion[sparkVersion]:
        scalaFullVersion = scalaVersions[scalaVersion]
        pipeline["steps"].append(
            {
                "label": f"spark-{sparkVersion} / scala-{scalaFullVersion}",
                "timeout_in_minutes": 180,
                "command": f"./gradlew :elasticsearch-spark-{sparkVersion}:integrationTest -Pscala.variant={scalaFullVersion}",
            }
        )

if os.environ.get("ENABLE_DRA_WORKFLOW") == "true":
    pipeline["steps"].append(
        {
            "wait": None,
        }
    )

    pipeline["steps"].append(
        {
            "label": "DRA Snapshot Workflow",
            "command": ".buildkite/dra.sh",
            "timeout_in_minutes": 60,
            "agents": {"useVault": "true"},
            "env": {
                "USE_DRA_CREDENTIALS": "true",
            },
        },
    )

print(json.dumps(pipeline, indent=2))
