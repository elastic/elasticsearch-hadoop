#!/usr/bin/env python

import glob
import json
import os
import re
from typing import Dict, List, Tuple

# Note: If you'd like to add any debug info here, make sure to do it on stderr
# stdout will be fed into `buildkite-agent pipeline upload`

# Discover variants from SQL module build files (spark/sql-30, spark/sql-35, etc.)
# Each module declares its own setDefaultVariant / addFeatureVariant lines, which
# is the authoritative list of what integration-test steps CI should run.
# New modules (sql-40, sql-41, ...) are picked up automatically when added.
#
# Structure: sparkVersion → [(scalaVer, itestTaskName), ...]
groupingsBySparkVersion: Dict[str, List[Tuple[str, str]]] = {}
for buildFile in sorted(glob.glob("spark/sql-*/build.gradle")):
    with open(buildFile, "r") as f:
        content = f.read()
    # Match `setDefaultVariant "spark35scala213"` or `addFeatureVariant "spark35scala212"`
    # SparkVariantPlugin.itestTaskName() returns "integrationTest" for the default variant
    # and "integrationTest" + capitalize(variantName) for feature variants.
    for match in re.finditer(r'(setDefaultVariant|addFeatureVariant) +"spark([0-9]+)scala([0-9]+)"', content):
        kind, sparkVer, scalaVer = match.group(1), match.group(2), match.group(3)
        variantName = f"spark{sparkVer}scala{scalaVer}"
        if kind == "setDefaultVariant":
            itestTask = "integrationTest"
        else:
            itestTask = f"integrationTest{variantName[0].upper()}{variantName[1:]}"
        if sparkVer not in groupingsBySparkVersion:
            groupingsBySparkVersion[sparkVer] = []
        if not any(scalaVer == g[0] for g in groupingsBySparkVersion[sparkVer]):
            groupingsBySparkVersion[sparkVer].append((scalaVer, itestTask))

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

# Exclude ALL itest tasks from intake (both default and feature variants),
# since each gets its own dedicated parallel step below.
intakeTasks = [
    f"-x :elasticsearch-spark-{sparkVersion}:{itestTask}"
    for sparkVersion, variants in groupingsBySparkVersion.items()
    for scalaVer, itestTask in variants
]

pipeline["steps"].append(
    {
        "label": "intake",
        "timeout_in_minutes": 240,
        "command": "./gradlew check " + " ".join(intakeTasks),
    }
)

for sparkVersion, variants in groupingsBySparkVersion.items():
    for scalaVer, itestTask in variants:
        scalaFullVersion = scalaVersions[scalaVer]
        pipeline["steps"].append(
            {
                "label": f"spark-{sparkVersion} / scala-{scalaFullVersion}",
                "timeout_in_minutes": 180,
                "command": f"./gradlew :elasticsearch-spark-{sparkVersion}:{itestTask}",
            }
        )

if os.environ.get("ENABLE_DRA_WORKFLOW") == "true":
    with open("buildSrc/esh-version.properties") as _vf:
        stack_version = next(
            line.split("=", 1)[1].strip()
            for line in _vf
            if line.startswith("eshadoop")
        )
    DRA_PREP_VERSION = "v0.1.6"

    pipeline["steps"].append({"wait": None})

    pipeline["steps"].append(
        {
            "label": ":gradle: DRA Build",
            "key": "dra-build",
            "command": ".buildkite/dra.sh",
            "timeout_in_minutes": 60,
            "agents": {"useVault": "true"},
            "env": {
                "USE_MAVEN_GPG": "true",
                "USE_MAVEN_S3_CREDENTIALS": "true",
                "DRA_WORKFLOW": "snapshot",
            },
            "artifact_paths": [
                "dist/build/distributions/elasticsearch-hadoop-*.zip",
                "build/distributions/dependencies-*.csv",
            ],
        }
    )

    pipeline["steps"].append({"wait": None})

    pipeline["steps"].append(
        {
            "label": ":package: DRA Prep",
            "key": "dra-prep",
            "command": ".buildkite/stage_artifacts.sh",
            "agents": {"image": "docker.elastic.co/ci-agent-images/ubuntu-build-essential:latest"},
            "env": {"DRA_WORKFLOW": "snapshot"},
            "plugins": [
                {
                    f"elastic/dra-prep#{DRA_PREP_VERSION}": {
                        "product_id": "elasticsearch-hadoop",
                        "stack_version": stack_version,
                        "workflow": "snapshot",
                    }
                }
            ],
        }
    )

    pipeline["steps"].append(
        {
            "label": f":pipeline: DRA processing for elasticsearch-hadoop / {stack_version} / snapshot",
            "trigger": "unified-release-dra-processing",
            "async": True,
            "depends_on": "dra-prep",
            "build": {
                "env": {
                    "DRA_PRODUCT_ID": "elasticsearch-hadoop",
                    "DRA_STACK_VERSION": stack_version,
                    "DRA_WORKFLOW": "snapshot",
                }
            },
        }
    )

print(json.dumps(pipeline, indent=2))
