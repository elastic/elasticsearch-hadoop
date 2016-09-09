package org.elasticsearch.spark.streaming

import java.util.UUID

import org.apache.spark.streaming.dstream.DStream
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
import org.elasticsearch.hadoop.cfg.InternalConfigurationOptions.INTERNAL_TRANSPORT_POOLING_KEY
import org.elasticsearch.spark.rdd.EsSpark

import scala.collection.Map

object EsSparkStreaming {

  // Save methods
  def saveToEs(ds: DStream[_], resource: String) {
    saveToEs(ds, Map(ES_RESOURCE_WRITE -> resource))
  }
  def saveToEs(ds: DStream[_], resource: String, cfg: Map[String, String]) {
    saveToEs(ds, collection.mutable.Map(cfg.toSeq: _*) += (ES_RESOURCE_WRITE -> resource))
  }
  def saveToEs(ds: DStream[_], cfg: Map[String, String]) {
    doSaveToEs(ds, cfg, hasMeta = false)
  }

  // Save with metadata
  def saveToEsWithMeta[K,V](ds: DStream[(K,V)], resource: String) {
    saveToEsWithMeta(ds, Map(ES_RESOURCE_WRITE -> resource))
  }
  def saveToEsWithMeta[K,V](ds: DStream[(K,V)], resource: String, cfg: Map[String, String]) {
    saveToEsWithMeta(ds, collection.mutable.Map(cfg.toSeq: _*) += (ES_RESOURCE_WRITE -> resource))
  }
  def saveToEsWithMeta[K,V](ds: DStream[(K,V)], cfg: Map[String, String]) {
    doSaveToEs(ds, cfg, hasMeta = true)
  }

  // Save as JSON
  def saveJsonToEs(ds: DStream[_], resource: String) {
    saveToEs(ds, resource, Map(ES_INPUT_JSON -> true.toString))
  }
  def saveJsonToEs(ds: DStream[_], resource: String, cfg: Map[String, String]) {
    saveToEs(ds, resource, collection.mutable.Map(cfg.toSeq: _*) += (ES_INPUT_JSON -> true.toString))
  }
  def saveJsonToEs(ds: DStream[_], cfg: Map[String, String]) {
    saveToEs(ds, collection.mutable.Map(cfg.toSeq: _*) += (ES_INPUT_JSON -> true.toString))
  }

  // Implementation
  def doSaveToEs(ds: DStream[_], cfg: Map[String, String], hasMeta: Boolean): Unit = {
    // Set the transport pooling key and delegate to the standard EsSpark save.
    // IMPORTANT: Do not inline this into the lambda expression below
    val config = collection.mutable.Map(cfg.toSeq: _*) += (INTERNAL_TRANSPORT_POOLING_KEY -> UUID.randomUUID().toString)
    ds.foreachRDD(rdd => EsSpark.doSaveToEs(rdd, config, hasMeta))
  }
}
