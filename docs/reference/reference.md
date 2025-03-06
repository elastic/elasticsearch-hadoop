---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/hadoop/current/doc-sections.html#_reference_documentation
navigation_title: Reference
---
# {{esh-full}} reference

This part of the documentation explains the core functionality of elasticsearch-hadoop starting with the configuration options and architecture and gradually explaining the various major features. At a higher level the reference is broken down into architecture and configuration section which are general, Map/Reduce and the libraries built on top of it, upcoming computation libraries (like Apache Spark) and finally mapping, metrics and troubleshooting.

We recommend going through the entire documentation even superficially when trying out elasticsearch-hadoop for the first time, however those in a rush, can jump directly to the desired sections:

[*Architecture*](./architecture.md)
:   Overview of the elasticsearch-hadoop architecture and how it maps on top of Hadoop.

[*Configuration*](./configuration.md)
:   Explore the various configuration switches in elasticsearch-hadoop.

[*Map/Reduce integration*](./mapreduce-integration.md)
:   Describes how to use elasticsearch-hadoop in vanilla Map/Reduce environments - typically useful for those interested in data loading and saving to/from {{es}} without little, if any, ETL (extract-transform-load).

[*Apache Hive integration*](./apache-hive-integration.md)
:   Hive users should refer to this section.

[*Apache Spark support*](./apache-spark-support.md)
:   Describes how to use Apache Spark with {{es}} through elasticsearch-hadoop.

[*Mapping and types*](./mapping-types.md)
:   A deep-dive into the strategies employed by elasticsearch-hadoop for doing type conversion and mapping to and from {{es}}.

[*Hadoop Metrics*](./hadoop-metrics.md)
:   Elasticsearch Hadoop metrics.

[*Troubleshooting*](docs-content://troubleshoot/elasticsearch/elasticsearch-hadoop/elasticsearch-for-apache-hadoop.md)
:   Tips on troubleshooting and getting help.