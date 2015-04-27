package org.elasticsearch.spark.sql

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.ArrayBuffer
import org.apache.commons.logging.Log
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.Row
import org.elasticsearch.hadoop.cfg.Settings
import org.elasticsearch.hadoop.rest.RestService.PartitionDefinition
import org.elasticsearch.hadoop.util.StringUtils
import org.elasticsearch.spark.rdd.AbstractEsRDDIterator
import scala.collection.mutable.LinkedHashMap


abstract class AbstractEsRowIterator[T] (
  context: TaskContext,
  partition: PartitionDefinition)
  extends AbstractEsRDDIterator[T](context, partition) {

  protected val rowOrder = new LinkedHashMap[String, Int]

  override def initReader(settings: Settings, log: Log) = {
    val csv = settings.getScrollFields()
    val readMetadata = settings.getReadMetadata()

    if (StringUtils.hasText(csv)) {
      val fields = StringUtils.tokenize(csv).asScala
      for (i <- 0 until fields.length) {
        rowOrder.put(fields(i), i)
      }
      // add _metadata if it's not present
      if (readMetadata) {
        rowOrder.getOrElseUpdate(settings.getReadMetadataField, fields.length)
      }
    }
  }
}