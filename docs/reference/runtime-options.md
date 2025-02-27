---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration-runtime.html
navigation_title: Runtime options
---
# {{esh-full}} runtime options

When using elasticsearch-hadoop, it is important to be aware of the following Hadoop configurations that can influence the way Map/Reduce tasks are executed and in return elasticsearch-hadoop.

::::{important}
Unfortunately, these settings need to be setup **manually** **before** the job / script configuration. Since elasticsearch-hadoop is called too late in the life-cycle, after the tasks have been already dispatched and as such, cannot influence the execution anymore.
::::



## Speculative execution [_speculative_execution]

[TBC: FANCY QUOTE]
In other words, speculative execution is an **optimization**, enabled by default, that allows Hadoop to create duplicates tasks of those which it considers hanged or slowed down. When doing data crunching or reading resources, having duplicate tasks is harmless and means at most a waste of computation resources; however when writing data to an external store, this can cause data corruption through duplicates or unnecessary updates. Since the *speculative execution* behavior can be triggered by external factors (such as network or CPU load which in turn cause false positive) even in stable environments (virtualized clusters are particularly prone to this) and has a direct impact on data, elasticsearch-hadoop disables this optimization for data safety.

Please check your library setting and disable this feature. If you encounter more data then expected, double and triple check this setting.


### Disabling Map/Reduce speculative execution [_disabling_mapreduce_speculative_execution]

Speculative execution can be disabled for the map and reduce phase - we recommend disabling in both cases - by setting to `false` the following two properties:

`mapred.map.tasks.speculative.execution` `mapred.reduce.tasks.speculative.execution`

One can either set the properties by name manually on the `Configuration`/`JobConf` client:

```java
jobConf.setSpeculativeExecution(false);
// or
configuration.setBoolean("mapred.map.tasks.speculative.execution", false);
configuration.setBoolean("mapred.reduce.tasks.speculative.execution", false);
```

or by passing them as arguments to the command line:

```bash
$ bin/hadoop jar -Dmapred.map.tasks.speculative.execution=false \
                 -Dmapred.reduce.tasks.speculative.execution=false <jar>
```


### Hive speculative execution [_hive_speculative_execution]

Apache Hive has its own setting for speculative execution through namely `hive.mapred.reduce.tasks.speculative.execution`. It is enabled by default so do change it to `false` in your scripts:

```sql
set hive.mapred.reduce.tasks.speculative.execution=false;
```

Note that while the setting has been deprecated in Hive 0.10 and one might get a warning, double check that the speculative execution is actually disabled.


### Spark speculative execution [_spark_speculative_execution]

Out of the box, Spark has speculative execution disabled. Double check this is the case through the `spark.speculation` setting (`false` to disable it, `true` to enable it).

