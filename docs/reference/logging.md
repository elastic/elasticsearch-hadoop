---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/hadoop/current/logging.html
navigation_title: Logging
---
# Logging in {{esh-full}}

elasticsearch-hadoop uses [commons-logging](http://commons.apache.org/proper/commons-logging/) library, same as Hadoop, for its logging infrastructure and thus it shares the same configuration means. Out of the box, no configuration is required - by default, elasticsearch-hadoop logs relevant information about the job progress at `INFO` level. Typically, whatever integration you are using (Map/Reduce, Hive), each job will print in the console at least one message indicating the elasticsearch-hadoop version used:

```bash
16:13:01,946  INFO main util.Version - Elasticsearch Hadoop v2.0.0.BUILD-SNAPSHOT [f2c5c3e280]
```

Configuring logging for Hadoop (or Hive) is outside the scope of this documentation, however in short, at runtime, Hadoop relies on [log4j 1.2](http://logging.apache.org/log4j/1.2/) as an actual logging implementation. In practice, this means adding the package name of interest and its level logging the `log4j.properties` file in the job classpath. elasticsearch-hadoop provides the following important packages:

| Package | Purpose |
| --- | --- |
| `org.elasticsearch.hadoop.hive` | Apache Hive integration |
| `org.elasticsearch.hadoop.mr` | Map/Reduce functionality |
| `org.elasticsearch.hadoop.rest` | REST/transport infrastructure |
| `org.elasticsearch.hadoop.serialization` | Serialization package |
| `org.elasticsearch.spark` | Apache Spark package |

The default logging level (`INFO`) is suitable for day-to-day use; if troubleshooting is needed, consider switching to `DEBUG` but be selective of the packages included. For low-level details, enable level `TRACE` however do remember that it will result in a **significant** amount of logging data which *will* impact your job performance and environment.

To put everything together, if you want to enable `DEBUG` logging on the Map/Reduce package make changes to the `log4j.properties` (used by your environment):

```bash
log4j.category.org.elasticsearch.hadoop.mr=DEBUG
```

::::{tip}
See the log4j [javadoc](https://logging.apache.org/log4j/1.2/apidocs/org/apache/log4j/PropertyConfigurator.html#doConfigure%28java.lang.String,%20org.apache.log4j.spi.LoggerRepository%29) for more information.
::::


## Configure the *executing* JVM logging not the client [_configure_the_executing_jvm_logging_not_the_client]

One thing to note is that in almost all cases, one needs to configure logging in the *executing* JVM, where the Map/Reduce tasks actually run and not on the client, where the job is assembled or monitored. Depending on your library, platform and version this can done through some dedicated settings. In particular Map/Reduce-based libraries like Hive can be difficult to configure since at runtime, they create Map/Reduce tasks to actually perform the work. Thus, one needs to configure logging and pass the configuration to the Map/Reduce layer for logging to occur. In both cases, this can be achieved through the `SET` command. In particular when using Hadoop 2.6, one can use `mapreduce.job.log4j-properties-file` along with an appropriate [`container-log4j.properties`](https://github.com/apache/hadoop/blob/release-2.6.0/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/resources/container-log4j.properties) file.


