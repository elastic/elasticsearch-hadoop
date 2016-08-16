package org.elasticsearch.spark.sql

import org.elasticsearch.spark.rdd.CompatibilityLevel

/**
  * For determining Spark Version Compatibility
  */
class SparkSQLCompatibilityLevel extends CompatibilityLevel {
  val versionId = "13"
  val versionDescription = "1.3-1.6"
}
