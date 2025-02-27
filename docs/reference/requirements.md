---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/hadoop/current/requirements.html
navigation_title: Requirements
---
# {{esh-full}} requirements

Before running elasticsearch-hadoop, please do check out the requirements below. This is even more so important when deploying elasticsearch-hadoop across a cluster where the software on some machines might be slightly out of sync. While elasticsearch-hadoop tries its best to fall back and do various validations of its environment, doing a quick sanity check especially during upgrades can save you a lot of headaches.

::::{note}
make sure to verify **all** nodes in a cluster when checking the version of a certain artifact.
::::


::::{tip}
elasticsearch-hadoop adds no extra requirements to Hadoop (or the various libraries built on top of it, such as Hive) or {{es}} however as a rule of thumb, do use the latest stable version of the said library (checking the compatibility with Hadoop and the JDK, where applicable).
::::


## JDK [requirements-jdk]

JDK level 8 (at least u20 or higher). An up-to-date support matrix for Elasticsearch is available [here](https://www.elastic.co/subscriptions/matrix). Do note that the JVM versions are **critical** for a stable environment as an incorrect version can corrupt the data underneath as explained in this [blog post](http://www.elastic.co/blog/java-1-7u55-safe-use-elasticsearch-lucene/).

One can check the available JDK version from the command line:

```bash
$ java -version
java version "1.8.0_45"
```


## {{es}} [requirements-es]

We **highly** recommend using the latest Elasticsearch (currently 9.0.0). While elasticsearch-hadoop maintains backwards compatibility with previous versions of {{es}}, we strongly recommend using the latest, stable version of Elasticsearch. You can find a matrix of supported versions [here](https://www.elastic.co/support/matrix#matrix_compatibility).

The {{es}} version is shown in its folder name:

```bash
$ ls
elasticsearch-9.0.0
```

If {{es}} is running (locally or remotely), one can find out its version through REST:

```js
$ curl -XGET http://localhost:9200
{
  "status" : 200,
  "name" : "Dazzler",
  "version" : {
    "number" : "9.0.0",
    ...
  },
  "tagline" : "You Know, for Search"
}
```


## Hadoop [requirements-hadoop]

elasticsearch-hadoop is compatible with Hadoop 2 and Hadoop 3 (ideally the latest stable version). It is tested daily against Apache Hadoop, but any distro compatible with Apache Hadoop should work just fine.

To check the version of Hadoop, one can refer either to its folder or jars (which contain the version in their names) or from the command line:

```bash
$ bin/hadoop version
Hadoop 3.3.1
```


## Apache Hive [requirements-hive]

Apache Hive 0.10 or higher. We recommend using the latest release of Hive (currently 2.3.8).

One can find out the Hive version from its folder name or command-line:

```bash
$ bin/hive --version
Hive version 2.3.8
```


## Apache Spark [requirements-spark]

::::{admonition} Deprecated in 9.0.
:class: warning

Support for Spark 2.x in elasticsearch-hadoop is deprecated.
::::


Spark 2.0 or higher. We recommend using the latest release of Spark (currently 3.2.0). As elasticsearch-hadoop provides native integration (which is recommended) with Apache Spark, it does not matter what binary one is using. The same applies when using the Hadoop layer to integrate the two as elasticsearch-hadoop supports the majority of Hadoop distributions out there.

The Spark version can be typically discovered by looking at its folder name:

```bash
$ pwd
/libs/spark/spark-3.2.0-bin-XXXXX
```

or by running its shell:

```bash
$ bin/spark-shell
...
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.2.0
      /_/
...
```

### Apache Spark SQL [requirements-spark-sql]

If planning on using Spark SQL make sure to add the appropriate Spark SQL jar as a dependency. While it is part of the Spark distribution, it is *not* part of the Spark core jar but rather has its own jar. Thus, when constructing the classpath make sure to include `spark-sql-<scala-version>.jar` or the Spark *assembly* : `spark-assembly-3.2.0-<distro>.jar`

elasticsearch-hadoop supports Spark SQL 2.x and Spark SQL 3.x. elasticsearch-hadoop supports Spark SQL 2.x on Scala 2.11 through its main jar. Since Spark 2.x and 3.x are not compatible with each other, and Scala versions are not compatible, multiple different artifacts are provided by elasticsearch-hadoop. Choose the jar appropriate for your Spark and Scala version. See the Spark chapter for more information.



