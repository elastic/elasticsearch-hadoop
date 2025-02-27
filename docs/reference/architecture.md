---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/hadoop/current/arch.html
navigation_title: Architecture
---
# {{esh-full}} architecture

At the core, elasticsearch-hadoop integrates two *distributed* systems: **Hadoop**, a distributed computing platform and **{{es}}**, a real-time search and analytics engine. From a high-level view both provide a computational component: Hadoop through Map/Reduce or recent libraries like Apache Spark on one hand, and {{es}} through its search and aggregation on the other.

elasticsearch-hadoop goal is to *connect* these two entities so that they can transparently benefit from each other.


## Map/Reduce and Shards [arch-shards]

A critical component for scalability is parallelism or splitting a task into multiple, smaller ones that execute at the same time, on different nodes in the cluster. The concept is present in both Hadoop through its `splits` (the number of parts in which a source or input can be divided) and {{es}} through `shards` (the number of parts in which a index is divided into).

In short, roughly speaking more input splits means more tasks that can read at the same time, different parts of the source. More shards means more *buckets* from which to read an index content (at the same time).

As such, elasticsearch-hadoop uses splits and shards as the main drivers behind the number of tasks executed within the Hadoop and {{es}} clusters as they have a direct impact on parallelism.

::::{tip}
Hadoop `splits` as well as {{es}} `shards` play an important role regarding a system behavior - we recommend familiarizing with the two concepts to get a better understanding of your system runtime semantics.
::::



## Apache Spark and Shards [arch-spark]

While Apache Spark is not built on top of Map/Reduce it shares similar concepts: it features the concept of `partition` which is the rough equivalent of {{es}} `shard` or the Map/Reduce `split`. Thus, the analogy above applies here as well - more shards and/or more partitions increase the number of parallelism and thus allows both systems to scale better.

::::{note}
Due to the similarity in concepts, through-out the docs one can think interchangebly of Hadoop `InputSplit` s and Spark `Partition` s.
::::



## Reading from {{es}} [arch-reading]

Shards play a critical role when reading information from {{es}}. Since it acts as a source, elasticsearch-hadoop will create one Hadoop `InputSplit` per {{es}} shard, or in case of Apache Spark one `Partition`, that is given a query that works against index `I`. elasticsearch-hadoop will dynamically discover the number of shards backing `I` and then for each shard will create, in case of Hadoop an input split (which will determine the maximum number of Hadoop tasks to be executed) or in case of Spark a partition which will determine the `RDD` maximum parallelism.

With the default settings, {{es}} uses **5** `primary` shards per index which will result in the same number of tasks on the Hadoop side for each query.

::::{note}
elasticsearch-hadoop does not query the same shards - it iterates through all of them (primaries and replicas) using a round-robin approach. To avoid data duplication, only one shard is used from each shard group (primary and replicas).
::::


A common concern (read optimization) for improving performance is to increase the number of shards and thus increase the number of tasks on the Hadoop side. Unless such gains are demonstrated through benchmarks, we recommend against such a measure since in most cases, an {{es}} shard can **easily** handle data streaming to a Hadoop or Spark task.


## Writing to {{es}} [arch-writing]

Writing to {{es}} is driven by the number of Hadoop input splits (or tasks) or Spark partitions available. elasticsearch-hadoop detects the number of (primary) shards where the write will occur and distributes the writes between these. The more splits/partitions available, the more mappers/reducers can write data in parallel to {{es}}.


## Data co-location [arch-colocation]

Whenever possible, elasticsearch-hadoop shares the {{es}} cluster information with Hadoop and Spark to facilitate data co-location. In practice, this means whenever data is read from {{es}}, the source nodes' IPs are passed on to Hadoop and Spark to optimize task execution. If co-location is desired/possible, hosting the {{es}} and Hadoop and Spark clusters within the same rack will provide significant network savings.

