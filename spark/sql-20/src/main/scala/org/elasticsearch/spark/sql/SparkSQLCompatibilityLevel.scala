package org.elasticsearch.spark.sql

import org.elasticsearch.spark.rdd.CompatibilityLevel

/**
  * For determining Spark Version Compatibility
  */
class SparkSQLCompatibilityLevel extends CompatibilityLevel {
  val versionId = "20"
  val versionDescription = "2.0"
}
