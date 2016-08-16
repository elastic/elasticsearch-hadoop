package org.elasticsearch.spark.rdd

trait CompatibilityLevel {
  def versionId: String
  def versionDescription: String
}
