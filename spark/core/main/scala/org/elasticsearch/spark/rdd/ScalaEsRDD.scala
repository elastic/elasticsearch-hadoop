package org.elasticsearch.spark.rdd

import scala.collection.Map

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.spark.Partition
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.elasticsearch.hadoop.cfg.Settings
import org.elasticsearch.hadoop.rest.InitializationUtils
import org.elasticsearch.hadoop.rest.RestService.PartitionDefinition
import org.elasticsearch.spark.serialization.ScalaValueReader

private[spark] class ScalaEsRDD[T](
  @transient sc: SparkContext,
  params: Map[String, String] = Map.empty)
  extends AbstractEsRDD[(String, T)](sc, params) {

  override def compute(split: Partition, context: TaskContext): ScalaEsRDDIterator[T] = {
    new ScalaEsRDDIterator(context, split.asInstanceOf[EsPartition].esPartition)
  }
}

private[spark] class ScalaEsRDDIterator[T](
  context: TaskContext,
  partition: PartitionDefinition)
  extends AbstractEsRDDIterator[(String, T)](context, partition) {

  override def getLogger() = LogFactory.getLog(ScalaEsRDD.getClass())

  override def initReader(settings: Settings, log: Log) = {
    InitializationUtils.setValueReaderIfNotSet(settings, classOf[ScalaValueReader], log)
  }

  override def createValue(value: Array[Object]): (String, T) = {
    (value(0).toString() -> value(1).asInstanceOf[T])
  }
}