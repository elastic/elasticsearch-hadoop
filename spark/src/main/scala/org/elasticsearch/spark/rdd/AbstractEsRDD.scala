package org.elasticsearch.spark.rdd;

import scala.collection.JavaConversions.collectionAsScalaIterable
import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.commons.logging.LogFactory
import org.apache.spark.Partition
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.elasticsearch.hadoop.rest.RestService
import org.elasticsearch.hadoop.rest.RestService.PartitionDefinition
import org.elasticsearch.spark.cfg.SparkSettingsManager

private[spark] abstract class AbstractEsRDD[T: ClassTag](
  @transient sc: SparkContext,
  val params: scala.collection.Map[String, String] = Map.empty)
  extends RDD[T](sc, Nil) {

  protected var logger = LogFactory.getLog(this.getClass())

  override def getPartitions: Array[Partition] = {
    val sparkPartitions = new Array[Partition](esPartitions.size)
    var idx: Int = 0
    for (esPartition <- esPartitions) {
      sparkPartitions(idx) = new EsPartition(id, idx, esPartition)
      idx += 1
    }
    sparkPartitions
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val esSplit = split.asInstanceOf[EsPartition]
    Seq(esSplit.esPartition.nodeIp)
  }

  override def checkpoint() {
    // Do nothing. Elasticsearch RDD should not be checkpointed.
  }

  @transient private[spark] lazy val esCfg = {
    val cfg = new SparkSettingsManager().load(sc.getConf).copy();
    cfg.merge(params)
  }

  @transient private[spark] lazy val esPartitions = {
    RestService.findPartitions(esCfg, logger)
  }
}

private[spark] class EsPartition(rddId: Int, idx: Int, val esPartition: PartitionDefinition)
  extends Partition {

  override def hashCode(): Int = 41 * (41 * (41 + rddId) + idx) + esPartition.hashCode()

  override val index: Int = idx
}