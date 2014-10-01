package org.elasticsearch.spark.sql

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
import org.elasticsearch.spark.rdd.ScalaEsRDD
import org.elasticsearch.spark.rdd.ScalaEsRDDIterator
import org.apache.spark.rdd.RDD
import org.elasticsearch.spark.rdd.AbstractEsRDDIterator
import org.elasticsearch.spark.rdd.AbstractEsRDD
import org.elasticsearch.spark.rdd.EsPartition
import org.apache.spark.TaskContext
import org.apache.spark.SparkContext
import org.elasticsearch.spark.rdd.AbstractEsRDD
import org.elasticsearch.spark.rdd.EsPartition
import org.elasticsearch.spark.rdd.AbstractEsRDDIterator
import org.apache.spark.sql.api.java.Row

// see the comments in ScalaEsRowRDD 
private[spark] class JavaEsRowRDD(
  @transient sc: SparkContext,
  params: Map[String, String] = Map.empty)
  extends AbstractEsRDD[Row](sc, params) {

  override def compute(split: Partition, context: TaskContext): JavaEsRowRDDIterator = {
    new JavaEsRowRDDIterator(context, split.asInstanceOf[EsPartition].esPartition)
  }
}

private[spark] class JavaEsRowRDDIterator(
  context: TaskContext,
  partition: PartitionDefinition)
  extends AbstractEsRDDIterator[Row](context, partition) {

  override def getLogger() = LogFactory.getLog(classOf[JavaEsRowRDD])

  override def initReader(settings: Settings, log: Log) = {
    InitializationUtils.setValueReaderIfNotSet(settings, classOf[JdkRowValueReader], log)
  }

  override def createValue(value: Array[Object]): Row = {
    value(1).asInstanceOf[Row]
  }
}

