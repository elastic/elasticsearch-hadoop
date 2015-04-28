package org.elasticsearch.spark.sql

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.Map
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.spark.Partition
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.catalyst.expressions.Row
import org.elasticsearch.hadoop.cfg.Settings
import org.elasticsearch.hadoop.rest.InitializationUtils
import org.elasticsearch.hadoop.rest.RestService.PartitionDefinition
import org.elasticsearch.spark.rdd.AbstractEsRDD
import org.elasticsearch.spark.rdd.AbstractEsRDDIterator
import org.elasticsearch.spark.rdd.EsPartition
import scala.collection.mutable.ArrayBuffer
import org.elasticsearch.hadoop.util.StringUtils
import scala.collection.mutable.LinkedHashMap
import org.elasticsearch.spark.serialization.ScalaValueReader

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
  extends AbstractEsRowIterator[Row](context, partition) {

  override def getLogger() = LogFactory.getLog(classOf[ScalaEsRowRDD])

  override def initReader(settings: Settings, log: Log) = {
    InitializationUtils.setValueReaderIfNotSet(settings, classOf[ScalaValueReader], log)
    super.initReader(settings, log)
  }

   override def createValue(value: Array[Object]): Row = {
    // drop the ID and convert the value (the Map) into a Row
    // convert the map into a Row
    val struct = value(1).asInstanceOf[LinkedHashMap[AnyRef, Any]]

    val buffer: ArrayBuffer[Any] = if (rowOrder.isEmpty) new ArrayBuffer[Any]() else ArrayBuffer.fill(rowOrder.size)(null)

    for ((k,v) <- struct) {
      if (rowOrder.isEmpty) buffer.append(v) else { rowOrder.get(k.toString); buffer.update(rowOrder(k.toString), v) }
    }

    new ScalaEsRow(buffer)
  }
}