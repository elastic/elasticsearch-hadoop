package org.elasticsearch.spark.rdd

import java.util.Collections
import java.util.{Map => JMap}
import org.apache.commons.logging.LogFactory
import org.apache.commons.logging.Log
import org.apache.spark.SparkContext
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.elasticsearch.hadoop.cfg.Settings
import org.elasticsearch.hadoop.rest.InitializationUtils
import org.elasticsearch.hadoop.rest.RestService.PartitionDefinition
import org.elasticsearch.hadoop.serialization.builder.JdkValueReader


private[spark] class JavaEsRDD(
    @transient sc: SparkContext,
    config: scala.collection.Map[String, String] = scala.collection.Map.empty)
  extends AbstractEsRDD[(String, JMap[String, Object])](sc, config){

  override def compute(split: Partition, context: TaskContext): JavaEsRDDIterator = {
    new JavaEsRDDIterator(context, split.asInstanceOf[EsPartition].esPartition)
  }
}
  
private[spark] class JavaEsRDDIterator(
    context: TaskContext,
    partition: PartitionDefinition)
 extends AbstractEsRDDIterator[(String, JMap[String, Object])](context, partition) {

  override def getLogger() = LogFactory.getLog(classOf[JavaEsRDD])
  
  override def initReader(settings:Settings, log: Log) = {
    InitializationUtils.setValueReaderIfNotSet(settings, classOf[JdkValueReader], log)
  }

  override def createValue(value: Array[Object]): (String, JMap[String, Object]) = {
	(value(0).toString(), value(1).asInstanceOf[JMap[String, Object]])
  }
}