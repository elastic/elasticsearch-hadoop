package org.elasticsearch.spark.rdd;

import scala.collection.JavaConversions.collectionAsScalaIterable
import scala.collection.JavaConversions.mapAsJavaMap
import scala.reflect.ClassTag
import org.apache.commons.logging.LogFactory
import org.apache.spark.Partition
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.elasticsearch.hadoop.rest.RestService
import org.elasticsearch.hadoop.rest.RestService.PartitionDefinition
import org.elasticsearch.hadoop.util.ObjectUtils
import org.elasticsearch.spark.cfg.SparkSettingsManager
import org.elasticsearch.hadoop.rest.RestClient
import org.elasticsearch.hadoop.rest.RestRepository

private[spark] abstract class AbstractEsRDD[T: ClassTag](
  @transient sc: SparkContext,
  val params: scala.collection.Map[String, String] = Map.empty)
  extends RDD[T](sc, Nil) {

  private val init = { ObjectUtils.loadClass("org.elasticsearch.spark.rdd.CompatUtils", classOf[ObjectUtils].getClassLoader) }

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
    val ip = esSplit.esPartition.nodeIp
    if (ip != null) Seq(ip) else Nil
  }

  override def checkpoint() {
    // Do nothing. Elasticsearch RDD should not be checkpointed.
  }

  def esCount(): Long = {
    val repo = new RestRepository(esCfg)
    try {
      return repo.count(true)
    } finally {
      repo.close()
    }
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