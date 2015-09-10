package org.elasticsearch.spark.sql

import scala.collection.Map

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.spark.Partition
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.sql.api.java.Row
import org.elasticsearch.hadoop.cfg.Settings
import org.elasticsearch.hadoop.rest.InitializationUtils
import org.elasticsearch.hadoop.rest.RestService.PartitionDefinition
import org.elasticsearch.spark.rdd.AbstractEsRDD
import org.elasticsearch.spark.rdd.AbstractEsRDDIterator
import org.elasticsearch.spark.rdd.EsPartition

// see the comments in ScalaEsRowRDD
private[spark] class JavaEsRowRDD(
  @transient sc: SparkContext,
  params: Map[String, String] = Map.empty,
  schema: SchemaUtils.Schema)
  extends AbstractEsRDD[Row](sc, params) {

  override def compute(split: Partition, context: TaskContext): JavaEsRowRDDIterator = {
    new JavaEsRowRDDIterator(context, split.asInstanceOf[EsPartition].esPartition, schema)
  }
}

private[spark] class JavaEsRowRDDIterator(
  context: TaskContext,
  partition: PartitionDefinition,
  schema: SchemaUtils.Schema)
  extends AbstractEsRDDIterator[Row](context, partition) {

  override def getLogger() = LogFactory.getLog(classOf[JavaEsRowRDD])

  override def initReader(settings: Settings, log: Log) = {
    InitializationUtils.setValueReaderIfNotSet(settings, classOf[JavaEsRowValueReader], log)

    // parse the structure and save the order (requested by Spark) for each Row (root and nested)
    // since the data returned from Elastic is likely to not be in the same order
    SchemaUtils.setRowOrder(settings, schema.struct)
  }

  override def createValue(value: Array[Object]): Row = {
    // drop the ID
    value(1).asInstanceOf[JavaEsRow]
  }
}