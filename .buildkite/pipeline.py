#!/usr/bin/env python

import json
import os
import re
from typing import Dict

# Note: If you'd like to add any debug info here, make sure to do it on stderr
# stdout will be fed into `buildkite-agent pipeline upload`

coreFile = open("spark/core/build.gradle", "r")
core = coreFile.read()
coreFile.close()

# `Variant "spark20scala212"` => ["20", "212"]
groupings = re.findall(r'Variant +"spark([0-9]+)scala([0-9]+)"', core)

groupingsBySparkVersion: Dict[str, list[str]] = {}
for grouping in groupings:
    if grouping[0] not in groupingsBySparkVersion:
        groupingsBySparkVersion[grouping[0]] = []
    groupingsBySparkVersion[grouping[0]].append(grouping[1])

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
        "image": "family/elasticsearch-ubuntu-2004",
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
