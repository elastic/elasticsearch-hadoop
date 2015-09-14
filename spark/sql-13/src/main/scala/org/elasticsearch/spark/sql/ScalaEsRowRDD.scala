package org.elasticsearch.spark.sql

import scala.collection.Map

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.spark.Partition
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.sql.Row
import org.elasticsearch.hadoop.cfg.Settings
import org.elasticsearch.hadoop.rest.InitializationUtils
import org.elasticsearch.hadoop.rest.RestService.PartitionDefinition
import org.elasticsearch.spark.rdd.AbstractEsRDD
import org.elasticsearch.spark.rdd.AbstractEsRDDIterator
import org.elasticsearch.spark.rdd.EsPartition

// while we could have just wrapped the ScalaEsRDD and unpack the top-level data into a Row the issue is the underlying Maps are StructTypes
// and as such need to be mapped as Row resulting in either nested wrapping or using a ValueReader and which point wrapping becomes unyielding since the class signatures clash
private[spark] class ScalaEsRowRDD(
  @transient sc: SparkContext,
  params: Map[String, String] = Map.empty,
  schema: SchemaUtils.Schema)
  extends AbstractEsRDD[Row](sc, params) {

  override def compute(split: Partition, context: TaskContext): ScalaEsRowRDDIterator = {
    new ScalaEsRowRDDIterator(context, split.asInstanceOf[EsPartition].esPartition, schema)
  }
}

private[spark] class ScalaEsRowRDDIterator(
  context: TaskContext,
  partition: PartitionDefinition,
  schema: SchemaUtils.Schema)
  extends AbstractEsRDDIterator[Row](context, partition) {

  override def getLogger() = LogFactory.getLog(classOf[ScalaEsRowRDD])

  override def initReader(settings: Settings, log: Log) = {
    InitializationUtils.setValueReaderIfNotSet(settings, classOf[ScalaRowValueReader], log)

    // parse the structure and save the order (requested by Spark) for each Row (root and nested)
    // since the data returned from Elastic is likely to not be in the same order
    SchemaUtils.setRowInfo(settings, schema.struct)
  }

  override def createValue(value: Array[Object]): Row = {
    // drop the ID
    value(1).asInstanceOf[ScalaEsRow]
  }
}