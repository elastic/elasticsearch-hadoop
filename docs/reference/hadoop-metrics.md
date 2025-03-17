---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/hadoop/current/metrics.html
---

# Hadoop metrics [metrics]

The Hadoop system records a set of metric counters for each job that it runs. elasticsearch-hadoop extends on that and provides metrics about its activity for each job run by leveraging the Hadoop [Counters](http://hadoop.apache.org/docs/r3.3.1/api/org/apache/hadoop/mapred/Counters.html) infrastructure. During each run, elasticsearch-hadoop sends statistics from each task instance, as it is running, which get aggregated by the Map/Reduce infrastructure and are available through the standard Hadoop APIs.

elasticsearch-hadoop provides the following counters, available under `org.elasticsearch.hadoop.mr.Counter` enum:

| Counter name | Purpose |
| --- | --- |
| Data focused |
| BYTES_SENT | Total number of data/communication bytes sent over the network to {{es}} |
| BYTES_ACCEPTED | Data/Documents accepted by {{es}} in bytes |
| BYTES_RETRIED | Data/Documents rejected by {{es}} in bytes |
| BYTES_RECEIVED | Data/Documents received from {{es}} in bytes |
| Document focused |
| DOCS_SENT | Number of docs sent over the network to {{es}} |
| DOCS_ACCEPTED | Number of documents sent and accepted by {{es}} |
| DOCS_RETRIED | Number of documents sent but rejected by {{es}} |
| DOCS_RECEIVED | Number of documents received from {{es}} |
| Network focused |
| BULK_TOTAL | Number of bulk requests made to {{es}} |
| BULK_RETRIES | Number of bulk retries (caused by document rejections) |
| SCROLL_TOTAL | Number of scroll pulled from {{es}} |
| NODE_RETRIES | Number of node fall backs (caused by network errors) |
| NET_RETRIES | Number of network retries (caused by network errors) |
| Time focused |
| NET_TOTAL_TIME_MS | Overall time (in ms) spent over the network |
| BULK_TOTAL_TIME_MS | Time (in ms) spent over the network by the bulk requests |
| BULK_RETRIES_TOTAL_TIME_MS | Time (in ms) spent over the network retrying bulk requests |
| SCROLL_TOTAL_TIME_MS | Time (in ms) spent over the network reading the scroll requests |

One can use the counters programatically, depending on the API used, through [mapred](http://hadoop.apache.org/docs/r3.3.1/api/index.html?org/apache/hadoop/mapred/Counters.md) or [mapreduce](http://hadoop.apache.org/docs/r3.3.1/api/index.html?org/apache/hadoop/mapreduce/Counter.md). Whatever the choice, elasticsearch-hadoop performs automatic reports without any user intervention. In fact, when using elasticsearch-hadoop one will see the stats reported at the end of the job run, for example:

```bash
13:55:08,100  INFO main mapreduce.Job - Job job_local127738678_0013 completed successfully
13:55:08,101  INFO main mapreduce.Job - Counters: 35
...
Elasticsearch Hadoop Counters
    Bulk Retries=0
    Bulk Retries Total Time(ms)=0
    Bulk Total=20
    Bulk Total Time(ms)=518
    Bytes Accepted=159129
    Bytes Sent=159129
    Bytes Received=79921
    Bytes Retried=0
    Documents Accepted=993
    Documents Sent=993
    Documents Received=0
    Documents Retried=0
    Network Retries=0
    Network Total Time(ms)=937
    Node Retries=0
    Scroll Total=0
    Scroll Total Time(ms)=0
```

