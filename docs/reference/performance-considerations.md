---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/hadoop/current/performance.html
navigation_title: Performance considerations
---
# Performance considerations in {{esh-full}}

As the connector itself does not abstract {{es}} nor is it stateful per se (does not keep any state between jobs and only sends writes in bulk - see below), when looking at performance, one should keep an eye on {{es}} itself and its behavior.

Do note that in general, we advise against changing too many settings especially on the {{es}} side simply because the defaults are good enough for most cases. With every release {{es}} becomes *smarter* about its environment and automatically adjusts itself (for example HDD vs SDD).

Unless you really *know* what you are doing and have actual results showing the improvements (aka real benchmarks) do **not** make blind adjustments simply because there is a blog or post somewhere telling you to do so. Way too many times we have encountered systems that underperformed due to some misconfiguration that was supposed to do otherwise.


## Always aim for stability [_always_aim_for_stability]

Do note that the first and most important part of a system (even more so in a distributed one) is to have a stable, predictable environment as much as possible. While failure is a certainty (it is not a question of if but rather when), one should also keep in mind the stability of a system under pressure. Be generous with your hardware and start with baby steps - if a system cannot perform adequate under small load, it will certainly lag behind under pressure. Always stability should come first and performance second. One can’t have a performant system without it being stable.

Please keep this in mind and do not tweak settings hoping it would automatically increase performance. That is why for reliable results, always measure to see whether the changes that you make, have the desired impact.


## Read performance [_read_performance]

elasticsearch-hadoop performs a parallel read spread across all the target resource shards. It relies on [scrolling](https://www.elastic.co/guide/en/elasticsearch/guide/current/scroll.html) which is as efficient as it can be in returning the results of a query. If the read performance is not optimal, first try to see how *expensive* the query is by interogating {{es}} directly. The {{es}} documentation is a good place to start, such as the [Improving Performance](https://www.elastic.co/guide/en/elasticsearch/guide/current/_improving_performance.html) page; similar queries might return the same results but can have a different cost.

If a query is inefficient, try rewriting it in a more optimal way. Sometimes changing the data structure (for example by adding a new field or denormalizing it) can significantly improve the query performance as there are less calculations that need to be done.

Note that typically, the number of results do not influence the performance of the connector nor {{es}} itself. The connector does **not** read all the results at once but rather in chunks and passes them onwards to the consumer. Thus typically the size becomes an issue on the consumer side depending on whether it tries to buffer the results from the connector or can simply iterate or stream through them.

A common concern is whether increasing the number of shards improves read performance. And it does if that means scaling out, that is spread the data better across multiple nodes (which ideally sit on multiple machines). If that is not the case, the shards end up sitting on the same node and thus on the same machine; there’s simply *virtual* partitioning of data as the hardware behind it remains the same.

This can increase the consumption parallelism on the client side (more shards means more tasks that can read the data from {{es}} at once) however from {{es}} perspective there are no actual gains and thus query performance will likely remain the same.


## Write performance [_write_performance]

A crucial aspect in improving the write performance is to determine the maximum rate of data that {{es}} can ingest comfortably. This depends on many variables (data size, hardware, current load, etc..) but a good rule of thumb is for a bulk request to not take longer than 1-2s to be successfully processed.

Since elasticsearch-hadoop performs parallel writes, it is important to keep this in mind across *all* tasks, which are created by Hadoop/Spark at runtime.


### Decrease bulk size [_decrease_bulk_size]

Remember that elasticsearch-hadoop allows one to configure the number of entries and size for a batch write to {{es}} *per task*. That is, assuming there are `T` tasks, with a configuration of `B` bytes and `N` number of documents (where `d` is the average document size), the maximum number of bulk write requests at a given point in time can be `T*B` bytes or `T*N` number of docs (`T*N*d` in bytes) - which ever comes first.

Thus for a job with 5 tasks, using the defaults (1mb or 1000 docs) means up to 5mb/5000 docs bulk size (spread across shards). If this takes more than 1-2s to be processed, there’s no need to decrease it. If it’s less then that, you can try increasing it in *small* steps.


### Use a maximum limit of tasks writing to {{es}} [_use_a_maximum_limit_of_tasks_writing_to_es]

In case of elasticsearch-hadoop, the runtime (Hadoop or Spark) can and will create multiple tasks that will write at the same time to {{es}}. In some cases, this leads to a disproportionate number of tasks (sometimes one or even two orders of magnitude higher) between what the user planned to use for {{es}} and the actual number. This appears in particular when dealing with inputs that are highly splittable which can easily generate tens or hundreds of tasks. If the target {{es}} cluster has just a few nodes, likely dealing with so much data at once will be problematic.


### Understand why rejections happen [_understand_why_rejections_happen]

Under too much load, {{es}} starts rejecting documents - in such a case elasticsearch-hadoop waits for a while (default 10s) and then retries (by default up to 3 times). If {{es}} keeps rejecting documents, the job will eventually fail. In such a scenario, monitor {{es}} (through Marvel or other plugins) and keep an eye on bulk processing. Look at the percentage of documents being rejected; it is perfectly fine to have some documents rejected but anything higher then 10-15% on a regular basis is a good indication the cluster is overloaded.


### Keep the number of retries and waits at bay [_keep_the_number_of_retries_and_waits_at_bay]

As mentioned above, retries happen when documents are being rejected. This can be for a number of reasons - a sudden indexing spike, node relocation, etc…​ If your job keeps being aborted as the retries fail, this is a good indication your cluster is overloaded. Increasing it will **not** make the problem go away, rather it will just hide it under the rug; the job will be quite slow (as likely each write will be retried several times until all documents are acknowledged).

