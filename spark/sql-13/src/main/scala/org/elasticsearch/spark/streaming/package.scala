package org.elasticsearch.spark

import org.apache.spark.streaming.dstream.DStream
import org.elasticsearch.hadoop.util.ObjectUtils

import scala.reflect.ClassTag

package object streaming {

  private val init = { ObjectUtils.loadClass("org.elasticsearch.spark.rdd.CompatUtils", classOf[ObjectUtils].getClassLoader)}

  implicit def sparkDStreamFunctions(ds: DStream[_]): SparkDStreamFunctions = new SparkDStreamFunctions(ds)

  class SparkDStreamFunctions(ds: DStream[_]) extends Serializable {
    def saveToEs(resource: String) { EsSparkStreaming.saveToEs(ds, resource) }
    def saveToEs(resource: String, cfg: Map[String, String]) { EsSparkStreaming.saveToEs(ds, resource, cfg) }
    def saveToEs(cfg: Map[String, String]) { EsSparkStreaming.saveToEs(ds, cfg) }
  }

  implicit def sparkStringJsonDStreamFunctions(ds: DStream[String]): SparkJsonDStreamFunctions[String] = new SparkJsonDStreamFunctions[String](ds)
  implicit def sparkByteArrayJsonDStreamFunctions(ds: DStream[Array[Byte]]): SparkJsonDStreamFunctions[Array[Byte]] = new SparkJsonDStreamFunctions[Array[Byte]](ds)

  class SparkJsonDStreamFunctions[T : ClassTag](ds: DStream[T]) extends Serializable {
    def saveJsonToEs(resource: String) { EsSparkStreaming.saveJsonToEs(ds, resource) }
    def saveJsonToEs(resource: String, cfg: Map[String, String]) { EsSparkStreaming.saveJsonToEs(ds, resource, cfg) }
    def saveJsonToEs(cfg: Map[String, String]) { EsSparkStreaming.saveJsonToEs(ds, cfg) }
  }

  implicit def sparkPairDStreamFunctions[K : ClassTag, V : ClassTag](ds: DStream[(K,V)]): SparkPairDStreamFunctions[K,V] = new SparkPairDStreamFunctions[K,V](ds)

  class SparkPairDStreamFunctions[K : ClassTag, V : ClassTag](ds: DStream[(K, V)]) extends Serializable {
    def saveToEsWithMeta(resource: String) { EsSparkStreaming.saveToEsWithMeta(ds, resource) }
    def saveToEsWithMeta(resource: String, cfg: Map[String, String]) { EsSparkStreaming.saveToEsWithMeta(ds, resource, cfg) }
    def saveToEsWithMeta(cfg: Map[String, String]) { EsSparkStreaming.saveToEsWithMeta(ds, cfg) }
  }

}
