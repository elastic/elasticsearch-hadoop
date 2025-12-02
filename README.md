# Elasticsearch Hadoop [![Build Status](https://travis-ci.org/elastic/elasticsearch-hadoop.svg?branch=master)](https://travis-ci.org/elastic/elasticsearch-hadoop)
Elasticsearch real-time search and analytics natively integrated with Hadoop.
Supports [Map/Reduce](#mapreduce), [Apache Hive](#apache-hive), and [Apache Spark](#apache-spark).

See  [project page](https://www.elastic.co/elasticsearch/hadoop/) and [documentation](http://www.elastic.co/guide/en/elasticsearch/hadoop/current/index.html) for detailed information.

## Requirements
Elasticsearch cluster accessible through [REST][]. That's it!
Significant effort has been invested to create a small, dependency-free, self-contained jar that can be downloaded andput to use without any dependencies. Simply make it available to your job classpath and you're set.
For a certain library, see the dedicated [chapter](http://www.elastic.co/guide/en/elasticsearch/hadoop/current/requirements.html).

While an effort has been made to keep ES-Hadoop backwards compatible with older versions of Elasticsearch, it is best
to use the version of ES-Hadoop that is the same as the Elasticsearch version. See the
[product compatibility support matrix](https://www.elastic.co/support/matrix#matrix_compatibility) for more information. 

## Installation

### Stable Release (`9.0.0` used in the examples below)
Support for Hadoop is available through any Maven-compatible tool:

```xml
<dependency>
  <groupId>org.elasticsearch</groupId>
  <artifactId>elasticsearch-hadoop</artifactId>
  <version>9.2.2</version>
</dependency>
```
or as a stand-alone [ZIP](http://www.elastic.co/downloads/hadoop).

Spark support depends on the versions of Spark and Scala your cluster uses. For Scala 2.12 and Spark 3.0, 3.1, 3.2, 3.3, or 3.4, use:
```xml
<dependency>
  <groupId>org.elasticsearch</groupId>
  <artifactId>elasticsearch-spark-30_2.12</artifactId>
  <version>9.0.0</version>
</dependency>
```
For Scala 2.13 and Spark 3.2, 3.3, or 3.4, use:
```xml
<dependency>
  <groupId>org.elasticsearch</groupId>
  <artifactId>elasticsearch-spark-30_2.13</artifactId>
  <version>9.0.0</version>
</dependency>
```


### Supported Hadoop Versions

ES-Hadoop is developed for and tested against Hadoop 2.x and 3.x on YARN.
More information in this [section](http://www.elastic.co/guide/en/elasticsearch/hadoop/current/install.html).

### Supported Spark Versions

Spark 3.0 through 3.4 are supported. Only Scala 2.12 is supported for Spark 3.0 and 3.1. Both Scala 2.12 and 2.13
are supported for Spark 3.2 and higher.  

## Feedback / Q&A
We're interested in your feedback! You can find us on the [Elastic forum](https://discuss.elastic.co/).


## Online Documentation

The latest reference documentation is available online on the project [home page](http://www.elastic.co/guide/en/elasticsearch/hadoop/index.html). Below the README contains _basic_ usage instructions at a glance.

## Usage

### Configuration Properties

All configuration properties start with `es` prefix. Note that the `es.internal` namespace is reserved for the library internal use and should _not_ be used by the user at any point.
The properties are read mainly from the Hadoop configuration but the user can specify (some of) them directly depending on the library used.

### Required
```
es.resource=<ES resource location, relative to the host/port specified above>
```
### Essential
```
es.query=<uri or query dsl query>              # defaults to {"query":{"match_all":{}}}
es.nodes=<ES host address>                     # defaults to localhost
es.port=<ES REST port>                         # defaults to 9200
```

The full list is available [here](http://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html)

## [Map/Reduce][]

For basic, low-level or performance-sensitive environments, ES-Hadoop provides dedicated `InputFormat` and `OutputFormat` that read and write data to Elasticsearch. To use them, add the `es-hadoop` jar to your job classpath
(either by bundling the library along - it's ~300kB and there are no-dependencies), using the [DistributedCache][] or by provisioning the cluster manually.
See the [documentation](http://www.elastic.co/guide/en/elasticsearch/hadoop/current/index.html) for more information.

Note that es-hadoop supports the Hadoop API through its `EsInputFormat` and `EsOutputFormat` classes.

### Reading
```java
Configuration conf = new Configuration();
conf.set("es.resource", "radio/artists");
conf.set("es.query", "?q=me*");             // replace this with the relevant query
Job job = new Job(conf)
job.setInputFormatClass(EsInputFormat.class);
...
job.waitForCompletion(true);
```
### Writing
```java
Configuration conf = new Configuration();
conf.set("es.resource", "radio/artists"); // index or indices used for storing data
Job job = new Job(conf)
job.setOutputFormatClass(EsOutputFormat.class);
...
job.waitForCompletion(true);
```

## [Apache Hive][]
ES-Hadoop provides a Hive storage handler for Elasticsearch, meaning one can define an [external table][] on top of ES.

Add es-hadoop-<version>.jar to `hive.aux.jars.path` or register it manually in your Hive script (recommended):
```
ADD JAR /path_to_jar/es-hadoop-<version>.jar;
```
### Reading
To read data from ES, define a table backed by the desired index:
```SQL
CREATE EXTERNAL TABLE artists (
    id      BIGINT,
    name    STRING,
    links   STRUCT<url:STRING, picture:STRING>)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES('es.resource' = 'radio/artists', 'es.query' = '?q=me*');
```
The fields defined in the table are mapped to the JSON when communicating with Elasticsearch. Notice the use of `TBLPROPERTIES` to define the location, that is the query used for reading from this table.

Once defined, the table can be used just like any other:
```SQL
SELECT * FROM artists;
```

### Writing
To write data, a similar definition is used but with a different `es.resource`:
```SQL
CREATE EXTERNAL TABLE artists (
    id      BIGINT,
    name    STRING,
    links   STRUCT<url:STRING, picture:STRING>)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES('es.resource' = 'radio/artists');
```

Any data passed to the table is then passed down to Elasticsearch; for example considering a table `s`, mapped to a TSV/CSV file, one can index it to Elasticsearch like this:
```SQL
INSERT OVERWRITE TABLE artists
    SELECT NULL, s.name, named_struct('url', s.url, 'picture', s.picture) FROM source s;
```

As one can note, currently the reading and writing are treated separately but we're working on unifying the two and automatically translating [HiveQL][] to Elasticsearch queries.

## [Apache Spark][]
ES-Hadoop provides native (Java and Scala) integration with Spark: for reading a dedicated `RDD` and for writing, methods that work on any `RDD`. Spark SQL is also supported

### Reading
To read data from ES, create a dedicated `RDD` and specify the query as an argument:

```scala
import org.elasticsearch.spark._

..
val conf = ...
val sc = new SparkContext(conf)
sc.esRDD("radio/artists", "?q=me*")
```

#### Spark SQL
```scala
import org.elasticsearch.spark.sql._

// DataFrame schema automatically inferred
val df = sqlContext.read.format("es").load("buckethead/albums")

// operations get pushed down and translated at runtime to Elasticsearch QueryDSL
val playlist = df.filter(df("category").equalTo("pikes").and(df("year").geq(2016)))
```

### Writing
Import the `org.elasticsearch.spark._` package to gain `savetoEs` methods on your `RDD`s:

```scala
import org.elasticsearch.spark._

val conf = ...
val sc = new SparkContext(conf)

val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
val airports = Map("OTP" -> "Otopeni", "SFO" -> "San Fran")

sc.makeRDD(Seq(numbers, airports)).saveToEs("spark/docs")
```

#### Spark SQL

```scala
import org.elasticsearch.spark.sql._

val df = sqlContext.read.json("examples/people.json")
df.saveToEs("spark/people")
```

### Java

In a Java environment, use the `org.elasticsearch.spark.rdd.java.api` package, in particular the `JavaEsSpark` class.

### Reading
To read data from ES, create a dedicated `RDD` and specify the query as an argument.

```java
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

SparkConf conf = ...
JavaSparkContext jsc = new JavaSparkContext(conf);

JavaPairRDD<String, Map<String, Object>> esRDD = JavaEsSpark.esRDD(jsc, "radio/artists");
```

#### Spark SQL

```java
SQLContext sql = new SQLContext(sc);
DataFrame df = sql.read().format("es").load("buckethead/albums");
DataFrame playlist = df.filter(df.col("category").equalTo("pikes").and(df.col("year").geq(2016)))
```

### Writing

Use `JavaEsSpark` to index any `RDD` to Elasticsearch:
```java
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

SparkConf conf = ...
JavaSparkContext jsc = new JavaSparkContext(conf);

Map<String, ?> numbers = ImmutableMap.of("one", 1, "two", 2);
Map<String, ?> airports = ImmutableMap.of("OTP", "Otopeni", "SFO", "San Fran");

JavaRDD<Map<String, ?>> javaRDD = jsc.parallelize(ImmutableList.of(numbers, airports));
JavaEsSpark.saveToEs(javaRDD, "spark/docs");
```

#### Spark SQL

```java
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

DataFrame df = sqlContext.read.json("examples/people.json")
JavaEsSparkSQL.saveToEs(df, "spark/docs")
```

## Building the source

Elasticsearch Hadoop uses [Gradle][] for its build system and it is not required to have it installed on your machine. By default (`gradlew`), it automatically builds the package and runs the unit tests. For integration testing, use the `integrationTests` task.
See `gradlew tasks` for more information.

To create a distributable zip, run `gradlew distZip` from the command line; once completed you will find the jar in `build/libs`.

To build the project, JVM 8 (Oracle one is recommended) or higher is required.

## License
This project is released under version 2.0 of the [Apache License][]

```
Licensed to Elasticsearch under one or more contributor
license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright
ownership. Elasticsearch licenses this file to you under
the Apache License, Version 2.0 (the "License"); you may
not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
```

[Hadoop]: http://hadoop.apache.org
[Map/Reduce]: http://hadoop.apache.org/docs/r1.2.1/mapred_tutorial.html
[Apache Hive]: http://hive.apache.org
[Apache Spark]: http://spark.apache.org
[HiveQL]: http://cwiki.apache.org/confluence/display/Hive/LanguageManual
[external table]: http://cwiki.apache.org/Hive/external-tables.html
[Apache License]: http://www.apache.org/licenses/LICENSE-2.0
[Gradle]: http://www.gradle.org/
[REST]: http://www.elastic.co/guide/en/elasticsearch/reference/current/api-conventions.html
[DistributedCache]: http://hadoop.apache.org/docs/stable/api/org/apache/hadoop/filecache/DistributedCache.html
