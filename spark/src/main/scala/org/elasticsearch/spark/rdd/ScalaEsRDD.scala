package org.elasticsearch.spark.rdd

import scala.collection.Map

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.Partition
import org.elasticsearch.hadoop.cfg.Settings
import org.elasticsearch.spark.serialization.ScalaValueReader
import org.elasticsearch.hadoop.rest.InitializationUtils
import org.elasticsearch.hadoop.rest.RestService.PartitionDefinition

private[spark] class ScalaEsRDD(
  @transient sc: SparkContext,
  params: Map[String, String] = Map.empty)
  extends AbstractEsRDD[(String, Map[String, Any])](sc, params) {

  override def compute(split: Partition, context: TaskContext): ScalaEsRDDIterator = {
    new ScalaEsRDDIterator(context, split.asInstanceOf[EsPartition].esPartition)
  }
}

private[spark] class ScalaEsRDDIterator(
  context: TaskContext,
  partition: PartitionDefinition)
  extends AbstractEsRDDIterator[(String, Map[String, Any])](context, partition) {

  override def getLogger() = LogFactory.getLog(classOf[ScalaEsRDD])

  override def initReader(settings: Settings, log: Log) = {
    InitializationUtils.setValueReaderIfNotSet(settings, classOf[ScalaValueReader], log)
  }

  override def createValue(value: Array[Object]): (String, Map[String, Any]) = {
    (value(0).toString() -> value(1).asInstanceOf[Map[String, Any]])
  }
}