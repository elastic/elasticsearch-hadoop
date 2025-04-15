---
navigation_title: "Breaking changes"
---

# Elasticsearch for Apache Hadoop breaking changes [elasticsearch-apache-hadoop-breaking-changes]
Breaking changes can impact your Elastic applications, potentially disrupting normal operations. Before you upgrade, carefully review the Elasticsearch for Apache Hadoop breaking changes and take the necessary steps to mitigate any issues. To learn how to upgrade, check [Upgrade](docs-content://deploy-manage/upgrade.md).

% ## Next version [elasticsearch-apache-hadoop-nextversion-breaking-changes]

% ::::{dropdown} Title of breaking change 
% Description of the breaking change.
% For more information, check [PR #](PR link).
% **Impact**<br> Impact of the breaking change.
% **Action**<br> Steps for mitigating deprecation impact.
% ::::

% ## 9.0.0 [elasticsearch-apache-hadoop-900-breaking-changes]

% ::::{dropdown} Title of breaking change 
% Description of the breaking change.
% For more information, check [PR #](PR link).
% **Impact**<br> Impact of the breaking change.
% **Action**<br> Steps for mitigating deprecation impact.
% ::::
## 9.0.0 [elasticsearch-hadoop-900-breaking-changes]
::::{dropdown} Spark 2.x is no longer supported in 9.0+
Development for Apache Spark's 2.x version line has concluded. As such, we have removed support for this version in the Spark connector in 9.0. Spark 3.x is now the default supported version of Spark. 
For more information, check [#2316](https://github.com/elastic/elasticsearch-hadoop/pull/2316).
**Impact**<br> Deployments on Spark 2.x are no longer supported and will need to be updated.
**Action**<br> Any integrations using the Spark connector on a version of Spark before 3.x should update their version of Spark to a compatible version before upgrading Elasticsearch for Apache Hadoop/Spark.
::::