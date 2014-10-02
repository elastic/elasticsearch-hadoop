package org.elasticsearch.spark.rdd

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.Map

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_INPUT_JSON
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_QUERY
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_RESOURCE_READ
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_RESOURCE_WRITE
import org.elasticsearch.hadoop.cfg.PropertiesSettings
import org.elasticsearch.spark.cfg.SparkSettingsManager

object EsSpark {

  def esRDD(sc: SparkContext) = new ScalaEsRDD(sc)
  def esRDD(sc: SparkContext, map: Map[String, String]) = new ScalaEsRDD(sc, map)
  def esRDD(sc: SparkContext, resource: String) = new ScalaEsRDD(sc, Map(ES_RESOURCE_READ -> resource))
  def esRDD(sc: SparkContext, resource: String, query: String) = 
    new ScalaEsRDD(sc, Map(ES_RESOURCE_READ -> resource, ES_QUERY -> query))

  def saveToEs(rdd: RDD[_], resource: String) {
    saveToEs(rdd, Map(ES_RESOURCE_WRITE -> resource))
  }
  def saveToEs(rdd: RDD[_], resource: String, params: Map[String, String]) {
    saveToEs(rdd, collection.mutable.Map(params.toSeq: _*) += (ES_RESOURCE_WRITE -> resource))
  }
  def saveToEs(rdd: RDD[_], params: Map[String, String]) {
    val sparkCfg = new SparkSettingsManager().load(rdd.sparkContext.getConf)
    val cfg = new PropertiesSettings().load(sparkCfg.save())
    cfg.merge(params.asJava)
      
    rdd.sparkContext.runJob(rdd, new EsRDDWriter(cfg.save()).write _)
  }
  
  // JSON variant
  
  def saveJsonToEs(rdd: RDD[_], resource: String) {
    saveToEs(rdd, resource, Map(ES_INPUT_JSON -> true.toString))
  }
  def saveJsonToEs(rdd: RDD[_], resource: String, params: Map[String, String]) {
    saveToEs(rdd, resource, collection.mutable.Map(params.toSeq: _*) += (ES_INPUT_JSON -> true.toString))
  }
  def saveJsonToEs(rdd: RDD[_], params: Map[String, String]) {
    saveToEs(rdd, collection.mutable.Map(params.toSeq: _*) += (ES_INPUT_JSON -> true.toString))
  }
}