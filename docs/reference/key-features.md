---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/hadoop/current/features.html
navigation_title: Key features
---
# {{esh-full}} key features [features]

The key features of {{esh-full}} include:

Scalable Map/Reduce model
:   elasticsearch-hadoop is built around Map/Reduce: *every* operation done in elasticsearch-hadoop results in multiple Hadoop tasks (based on the number of target shards) that interact, in parallel with {{es}}.

REST based
:   elasticsearch-hadoop uses {{es}} REST interface for communication, allowing for flexible deployments by minimizing the number of ports needed to be open within a network.

Self contained
:   the library has been designed to be small and efficient. At around 300KB and *no* extra dependencies outside Hadoop itself, distributing elasticsearch-hadoop within your cluster is simple and fast.

Universal jar
:   whether you are using vanilla Apache Hadoop or a certain distro, the *same* elasticsearch-hadoop jar works transparently across all of them.

Memory and I/O efficient
:   elasticsearch-hadoop is focused on performance. From pull-based parsing, to bulk updates and direct conversion to/of native types, elasticsearch-hadoop keeps its memory and network I/O usage finely-tuned.

Adaptive I/O
:   elasticsearch-hadoop detects transport errors and retries automatically. If the {{es}} node died, re-routes the request to the available nodes (which are discovered automatically). Additionally, if {{es}} is overloaded, elasticsearch-hadoop detects the data rejected and resents it, until it is either processed or the user-defined policy applies.

Facilitates data co-location
:   elasticsearch-hadoop fully integrates with Hadoop exposing its network access information, allowing co-located {{es}} and Hadoop clusters to be aware of each other and reduce network IO.

Map/Reduce API support
:   At its core, elasticsearch-hadoop uses the low-level Map/Reduce API to read and write data to {{es}} allowing for maximum integration flexibility and performance.

*old*(`mapred`) & *new*(`mapreduce`) Map/Reduce APIs supported
:   elasticsearch-hadoop automatically adjusts to your environment; one does not have to change between using the `mapred` or `mapreduce` APIs - both are supported, by the same classes, at the same time.

Apache Hive support
:   Run Hive queries against {{es}} for advanced analystics and *real_time* responses. elasticsearch-hadoop exposes {{es}} as a Hive table so your scripts can crunch through data faster then ever.

Apache Spark
:   Run fast transformations directly against {{es}}, either by streaming data or indexing *arbitrary* `RDD`s. Available in both Java and Scala flavors.

