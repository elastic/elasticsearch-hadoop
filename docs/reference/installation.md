---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/hadoop/current/install.html
navigation_title: Installation
---
# {{esh-full}} installation

elasticsearch-hadoop binaries can be obtained either by downloading them from the [elastic.co](http://elastic.co) site as a ZIP (containing project jars, sources and documentation) or by using any [Maven](http://maven.apache.org/)-compatible tool with the following dependency:

```xml
<dependency>
  <groupId>org.elasticsearch</groupId>
  <artifactId>elasticsearch-hadoop</artifactId>
  <version>9.0.0-beta1</version>
</dependency>
```

The jar above contains all the features of elasticsearch-hadoop and does not require any other dependencies at runtime; in other words it can be used as is.

$$$yarn$$$
elasticsearch-hadoop binary is suitable for Hadoop 2.x (also known as YARN) environments. Support for Hadoop 1.x environments are deprecated in 5.5 and will no longer be tested against in 6.0.

## Minimalistic binaries [_minimalistic_binaries]

In addition to the *uber* jar, elasticsearch-hadoop provides minimalistic jars for each integration, tailored for those who use just *one* module (in all other situations the `uber` jar is recommended); the jars are smaller in size and use a dedicated pom, covering only the needed dependencies. These are available under the same `groupId`, using an `artifactId` with the pattern `elasticsearch-hadoop-{{integration}}`:

```xml
<dependency>
  <groupId>org.elasticsearch</groupId>
  <artifactId>elasticsearch-hadoop-mr</artifactId> <1>
  <version>9.0.0-beta1</version>
</dependency>
```

1. *mr* artifact


```xml
<dependency>
  <groupId>org.elasticsearch</groupId>
  <artifactId>elasticsearch-hadoop-hive</artifactId> <1>
  <version>9.0.0-beta1</version>
</dependency>
```

1. *hive* artifact


```xml
<dependency>
  <groupId>org.elasticsearch</groupId>
  <artifactId>elasticsearch-spark-30_2.12</artifactId> <1>
  <version>9.0.0-beta1</version>
</dependency>
```

1. *spark* artifact. Notice the `-30` part of the suffix which indicates the Spark version compatible with the artifact. Use `30` for Spark 3.0+, `20` for Spark 2.0+, and `13` for Spark 1.3-1.6. Notice the `_2.12` suffix which indicates the Scala version compatible with the artifact. Currently it is the same as the version used by Spark itself.


The Spark connector framework is the most sensitive to version incompatibilities. For your convenience, a version compatibility matrix has been provided below:

| Spark Version | Scala Version | ES-Hadoop Artifact ID |
| --- | --- | --- |
| 1.0 - 2.x | 2.10 | <unsupported> |
| 1.0 - 1.6 | 2.11 | <unsupported> |
| 2.x | 2.11 | <unsupported> |
| 2.x | 2.12 | <unsupported> |
| 3.0+ | 2.12 | elasticsearch-spark-30_2.12 |
| 3.2+ | 2.13 | elasticsearch-spark-30_2.13 |


## Development Builds [download-dev]

Development (or nightly or snapshots) builds are published daily at *sonatype-oss* repository (see below). Make sure to use snapshot versioning:

```xml
<dependency>
  <groupId>org.elasticsearch</groupId>
  <artifactId>elasticsearch-hadoop</artifactId>
  <version>{version}-SNAPSHOT</version> <1>
</dependency>
```

1. notice the *BUILD-SNAPSHOT* suffix indicating a development build


but also enable the dedicated snapshots repository :

```xml
<repositories>
  <repository>
    <id>sonatype-oss</id>
    <url>http://oss.sonatype.org/content/repositories/snapshots</url> <1>
    <snapshots><enabled>true</enabled></snapshots> <2>
  </repository>
</repositories>
```

1. add snapshot repository
2. enable *snapshots* capability on the repository otherwise these will not be found by Maven



## Upgrading Your Stack [upgrading]

Elasticsearch for Apache Hadoop is a client library for {{es}}, albeit one with extended functionality for supporting operations on Hadoop/Spark. When upgrading Hadoop/Spark versions, it is best to check to make sure that your new versions are supported by the connector, upgrading your elasticsearch-hadoop version as appropriate.

Elasticsearch for Apache Hadoop maintains backwards compatibility with the most recent minor version of {{es}}'s previous major release (5.X supports back to 2.4.X, 6.X supports back to 5.6.X, etc…​). When you are upgrading your version of {{es}}, it is best to upgrade elasticsearch-hadoop to the new version (or higher) first. The new elasticsearch-hadoop version should continue to work for your previous {{es}} version, allowing you to upgrade as normal.

::::{note}
Elasticsearch for Apache Hadoop does not support rolling upgrades well. During a rolling upgrade, nodes that elasticsearch-hadoop is communicating with will be regularly disappearing and coming back online. Due to the constant connection failures that elasticsearch-hadoop will experience during the time frame of a rolling upgrade there is high probability that your jobs will fail. Thus, it is recommended that you disable any elasticsearch-hadoop based write or read jobs against {{es}} during your rolling upgrade process.
::::



