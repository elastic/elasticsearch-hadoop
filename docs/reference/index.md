---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/hadoop/current/float.html
  - https://www.elastic.co/guide/en/elasticsearch/hadoop/current/index.html
  - https://www.elastic.co/guide/en/elasticsearch/hadoop/current/doc-sections.html
  - https://www.elastic.co/guide/en/elasticsearch/hadoop/current/reference.html
applies_to:
  stack: ga
  serverless: unavailable
---
# {{esh-full}}

{{esh-full}} is an umbrella project consisting of two similar, yet independent sub-projects: `elasticsearch-hadoop` and `repository-hdfs`.
This documentation pertains to `elasticsearch-hadoop`. For information about `repository-hdfs` and using HDFS as a back-end repository for doing snapshot or restore from or to {{es}}, go to [Hadoop HDFS repository plugin](elasticsearch://reference/elasticsearch-plugins/repository-hdfs.md).

{{esh-full}} is an [open-source](./license.md), stand-alone, self-contained, small library that allows big data processing frameworks (specifically Apache Hadoop Map/Reduce and Apache Spark) to *interact* with {{es}}. One can think of it as a *connector* that allows data to flow *bi-directionaly* so that applications can leverage transparently the {{es}} engine capabilities to significantly enrich their capabilities and improve performance.

{{esh-full}} provides native integration for Map/Reduce, Spark, and Hive, making {{es}} accessible as if it were a native resource within your data processing cluster. As such, {{esh-full}} operates as a library that processing jobs import and use through its APIs to read from and write to {{es}}.

$$$project-name-alias$$$
While the official name of the project is {{esh-full}} throughout the documentation the term elasticsearch-hadoop will be used instead to increase readability.

::::{admonition}
This document assumes the reader already has a basic familiarity with {{es}}, and Hadoop and/or Spark concepts. For more information, refer to [](./resources.md).
% While every effort has been made to ensure that this documentation is comprehensive and without error, nevertheless some topics might  require more explanations and some typos might have crept in. If you do spot any mistakes or even more serious errors and you have a few cycles during lunch, please do bring the error to the attention of the elasticsearch-hadoop team by raising an issue or [contact us](http://www.elastic.co/community).
% Thank you.
::::
