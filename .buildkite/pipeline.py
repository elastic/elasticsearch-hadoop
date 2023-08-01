import json
import re
from typing import Dict

coreFile = open("spark/core/build.gradle", "r")
core = coreFile.read()
coreFile.close()
groupings = re.findall(r'Variant +"spark([0-9]+)scala([0-9]+)"', core)

groupingsBySparkVersion: Dict[str, list[str]] = {}
for grouping in groupings:
    if grouping[0] not in groupingsBySparkVersion:
        groupingsBySparkVersion[grouping[0]] = []
    groupingsBySparkVersion[grouping[0]].append(grouping[1])

gradlePropertiesFile = open("gradle.properties", "r")
gradleProperties = gradlePropertiesFile.read()
gradlePropertiesFile.close()
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

# TODO
# - wait
# - label: dra-snapshot
#   command: TODO
#   timeout_in_minutes: TODO

print(json.dumps(pipeline, indent=2))
