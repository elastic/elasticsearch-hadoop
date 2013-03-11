# ElasticSearch Hadoop
Read and write data to/from ElasticSearch within Hadoop/MapReduce libraries. Automatically converts data to/from JSON. Supports MapReduce, [Hive](#hive) and [Pig](#pig). Cascading and Hadoop streaming are planned for the very near future.

# Requirements
ElasticSearch cluster accessible through [REST][]. That's it.
Significant effort has been invested to create a small, dependency-free, self-contained jar that can be downloaded and put to use without any dependencies. Simply make it available to your job classpath and you're set.

# License
This project is released under version 2.0 of the [Apache License][]

# Instalation
We're working towards a first release and we plan to have nightly builds soon available. In the meantime please build the project yourself.

# Usage

## Defaults

These properties are read from Hadoop Configuration if the user hasn't specified them.
```
es.host=<ElasticSearch host address> # optional, defaults to localhost
es.port=<ElasticSearch REST port>    # optional, defaults to 9200
es.location=<ElasticSearch resource location, relative to the host/port specified above. Can be an index or a query> # required
```

## [Hive][]
ES-Hadoop provides a Hive storage handler for ElasticSearch, meaning one can define an [external table][] on top of ES.

Add es-hadoop-<version>.jar to `hive.aux.jars.path` or register it manually in your Hive script (recommended):
```
ADD JAR /path_to_jar/es-hadoop-<version>.jar;
```
### Reading
To read data from ES, define a table backed by the desired index:
```
CREATE EXTERNAL TABLE artists (
    id      BIGINT,
    name    STRING,
    links   STRUCT<url:STRING, picture:STRING>)
STORED BY 'org.elasticsearch.hadoop.hive.ESStorageHandler'
TBLPROPERTIES('es.location' = 'radio/artists/_search?q=me*');
```
The fields defined in the table are mapped to the JSON when communicating with ElasticSearch. Notice the use of `TBLPROPERTIES` to define the location, that is the query used for reading from this table:
```
SELECT * FROM artists;
```

### Writing
To write data, a similar definition is used but with a different `es.location`:
```
CREATE EXTERNAL TABLE artists (
    id      BIGINT,
    name    STRING,
    links   STRUCT<url:STRING, picture:STRING>)
STORED BY 'org.elasticsearch.hadoop.hive.ESStorageHandler'
TBLPROPERTIES('es.location' = 'radio/artists/');
```

Any data passed to the table is then passed down to ElasticSearch; for example considering a table `s`, mapped to a TSV/CSV file, one can index it to ElasticSearch like this:
```
INSERT OVERWRITE TABLE artists 
    SELECT NULL, s.name, named_struct('url', s.url, 'picture', s.picture) FROM source s;
```

As one can note, currently the reading and writing are treated separately but we're working on unifying the two and automatically translating [HiveQL][] to ElasticSearch queries.

## [Pig][]
ES-Hadoop provides both read and write functions for Pig so you can access ElasticSearch from Pig scripts.

Register ES-Hadoop jar into your script or add it to your Pig classpath:
```
REGISTER /path_to_jar/es-hadoop-<version>.jar;
```
Additionally one can define an alias to save some chars:
```
%define ESSTORAGE org.elasticsearch.hadoop.pig.ESStorage()
```
and use `$ESSTORAGE` for storage definition.

### Reading
To read data from ES, use `ESStorage` and specify the query through the `LOAD` function:
```
A = LOAD 'radio/artists/_search?q=me*' USING org.elasticsearch.hadoop.pig.ESStorage();
DUMP A;
```

### Writing
Use the same `Storage` to write data to ElasticSearch:
```
A = LOAD 'src/test/resources/artists.dat' USING PigStorage() AS (id:long, name, url:chararray, picture: chararray);
B = FOREACH A GENERATE name, TOTUPLE(url, picture) AS links;
STORE B INTO 'radio/artists' USING org.elasticsearch.hadoop.pig.ESStorage();
```

# Building from source

ElasticSearch Hadoop uses [Gradle][] for its build system and it is not required to have it installed on your machine.

To create a distributable jar, run `gradlew -x test build` from the command line; once completed you will find the jar in `build\libs`.



[Hadoop]: http://hadoop.apache.org
[MapReduce]: http://hadoop.apache.org/docs/r1.0.4/mapred_tutorial.html
[Pig]: http://pig.apache.org
[Hive]: http://hive.apache.org
[HiveQL]: http://cwiki.apache.org/confluence/display/Hive/LanguageManual
[external table]: http://cwiki.apache.org/Hive/external-tables.html
[Apache License]: http://www.apache.org/licenses/LICENSE-2.0
[Gradle]: http://www.gradle.org/
[REST]: http://www.elasticsearch.org/guide/reference/api/