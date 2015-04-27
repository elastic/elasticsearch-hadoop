package org.elasticsearch.spark.sql

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.Map
import scala.collection.mutable.LinkedHashMap
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.spark.Partition
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.Row
import org.elasticsearch.hadoop.cfg.Settings
import org.elasticsearch.hadoop.rest.InitializationUtils
import org.elasticsearch.hadoop.rest.RestService.PartitionDefinition
import org.elasticsearch.spark.rdd.AbstractEsRDD
import org.elasticsearch.spark.rdd.AbstractEsRDDIterator
import org.elasticsearch.spark.rdd.EsPartition
import org.elasticsearch.spark.serialization.ScalaValueReader
import org.elasticsearch.hadoop.util.StringUtils
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListMap


// while we could have just wrapped the ScalaEsRDD and unpack the top-level data into a Row the issue is the underlying Maps are StructTypes
// and as such need to be mapped as Row resulting in either nested wrapping or using a ValueReader and which point wrapping becomes unyielding since the class signatures clash

private[spark] class ScalaEsRowRDD(
  @transient sc: SparkContext,
  params: Map[String, String] = Map.empty)
  extends AbstractEsRDD[Row](sc, params) {

  override def compute(split: Partition, context: TaskContext): ScalaEsRowRDDIterator = {
    new ScalaEsRowRDDIterator(context, split.asInstanceOf[EsPartition].esPartition)
  }
}

private[spark] class ScalaEsRowRDDIterator(
  context: TaskContext,
  partition: PartitionDefinition)
  extends AbstractEsRDDIterator[Row](context, partition) {

  private val rowOrder = new LinkedHashMap[String, Int]

  override def getLogger() = LogFactory.getLog(classOf[ScalaEsRowRDD])

  override def initReader(settings: Settings, log: Log) = {
    InitializationUtils.setValueReaderIfNotSet(settings, classOf[ScalaValueReader], log)

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

  override def createValue(value: Array[Object]): Row = {
    // drop the ID and convert the value (the Map) into a Row
    // convert the map into a Row
    val struct = value(1).asInstanceOf[Map[AnyRef, Any]]

    val buffer: ArrayBuffer[Any] = if (rowOrder.isEmpty) new ArrayBuffer[Any]() else ArrayBuffer.fill(rowOrder.size)(null)

    for ((k,v) <- struct) {
      if (rowOrder.isEmpty) buffer.append(v) else { rowOrder(k.toString); buffer.update(rowOrder(k.toString), v) }
    }
    new ScalaEsRow(buffer)
  }
}