---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/hadoop/current/float.html
  - https://www.elastic.co/guide/en/elasticsearch/hadoop/current/index.html
---

# Preface [float]

Elasticsearch for Apache Hadoop is an umbrella project consisting of two similar, yet independent sub-projects with their own, dedicated, section in the documentation:

elasticsearch-hadoop proper
:   Interact with {{es}} from within a Hadoop environment. If you are using Map/Reduce, Hive, or Apache Spark, this project is for you. For feature requests or bugs, please open an issue in the [Elasticsearch-Hadoop repository](https://github.com/elastic/elasticsearch-hadoop/issues).

repository-hdfs
:   Use HDFS as a back-end repository for doing snapshot/restore from/to {{es}}. For more information, refer to its [home page](elasticsearch://docs/reference/elasticsearch-plugins/repository-hdfs.md). For feature requests or bugs, please open an issue in the [Elasticsearch repository](https://github.com/elastic/elasticsearch/issues) with the ":Plugin Repository HDFS" tag.

Thus, while all projects fall under the Hadoop umbrella, each is covering a certain aspect of it so please be sure to read the appropriate documentation. For general questions around any of these projects, the [Elastic Discuss forum](https://discuss.elastic.co/c/elasticsearch-and-hadoop) is a great place to interact with other users in the community.

