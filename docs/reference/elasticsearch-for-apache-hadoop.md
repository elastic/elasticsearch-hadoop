---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/hadoop/current/reference.html
---

# Elasticsearch for Apache Hadoop [reference]

Elasticsearch for Apache Hadoop is an [open-source](/reference/license.md), stand-alone, self-contained, small library that allows Hadoop jobs (whether using Map/Reduce or libraries built upon it such as Hive or new upcoming libraries like Apache Spark ) to *interact* with {{es}}. One can think of it as a *connector* that allows data to flow *bi-directionaly* so that applications can leverage transparently the {{es}} engine capabilities to significantly enrich their capabilities and increase the performance.

Elasticsearch for Apache Hadoop offers first-class support for vanilla Map/Reduce and Hive so that using {{es}} is literally like using resources within the Hadoop cluster. As such, Elasticsearch for Apache Hadoop is a *passive* component, allowing Hadoop jobs to use it as a library and interact with {{es}} through Elasticsearch for Apache Hadoop APIs.

$$$project-name-alias$$$
While the official name of the project is Elasticsearch for Apache Hadoop throughout the documentation the term elasticsearch-hadoop will be used instead to increase readability.

::::{admonition}
This document assumes the reader already has a basic familiarity with {{es}} and Hadoop concepts; see the [Appendix A, *Resources*](/reference/resources.md) section for more information.

While every effort has been made to ensure that this documentation is comprehensive and without error, nevertheless some topics might require more explanations and some typos might have crept in. If you do spot any mistakes or even more serious errors and you have a few cycles during lunch, please do bring the error to the attention of the elasticsearch-hadoop team by raising an issue or [contact us](http://www.elastic.co/community).

Thank you.

::::


::::{tip}
If you are looking for {{es}} HDFS Snapshot/Restore plugin (a separate project), please refer to its [home page](https://github.com/elasticsearch/elasticsearch-hadoop/tree/master/repository-hdfs).
::::


