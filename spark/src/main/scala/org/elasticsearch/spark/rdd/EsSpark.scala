package org.elasticsearch.spark.rdd

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.Map

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_INPUT_JSON
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_OUTPUT_JSON
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_QUERY
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_RESOURCE_READ
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_RESOURCE_WRITE
import org.elasticsearch.hadoop.cfg.PropertiesSettings
import org.elasticsearch.spark.cfg.SparkSettingsManager

object EsSpark {
  //
  // Load methods
  // 
  
  type M = Map[String, Object]
  type RM = RDD[(String, M)]

  def esRDD(sc: SparkContext): RM = new ScalaEsRDD(sc)
  def esRDD(sc: SparkContext, cfg: Map[String, String]): RM = new ScalaEsRDD[M](sc, cfg)
  def esRDD(sc: SparkContext, resource: String): RM = new ScalaEsRDD[M](sc, Map(ES_RESOURCE_READ -> resource))
  def esRDD(sc: SparkContext, resource: String, query: String): RM = new ScalaEsRDD[M](sc, Map(ES_RESOURCE_READ -> resource, ES_QUERY -> query))

  // load data as JSON
  type J = String
  type RJ = RDD[(String, J)]
  
  def esJsonRDD(sc: SparkContext): RJ = new ScalaEsRDD[J](sc, Map(ES_OUTPUT_JSON -> true.toString))
  def esJsonRDD(sc: SparkContext, cfg: Map[String, String]): RJ = 
    new ScalaEsRDD[J](sc, collection.mutable.Map(cfg.toSeq: _*) += (ES_OUTPUT_JSON -> true.toString))
  def esJsonRDD(sc: SparkContext, resource: String): RJ = 
    new ScalaEsRDD[J](sc, Map(ES_RESOURCE_READ -> resource, ES_OUTPUT_JSON -> true.toString))
  def esJsonRDD(sc: SparkContext, resource: String, query: String): RJ = 
    new ScalaEsRDD[J](sc, Map(ES_RESOURCE_READ -> resource, ES_QUERY -> query, ES_OUTPUT_JSON -> true.toString))

  //
  // Save methods
  //
    
  def saveToEs(rdd: RDD[_], resource: String) { saveToEs(rdd, Map(ES_RESOURCE_WRITE -> resource)) }
  def saveToEs(rdd: RDD[_], resource: String, cfg: Map[String, String]) {
    saveToEs(rdd, collection.mutable.Map(cfg.toSeq: _*) += (ES_RESOURCE_WRITE -> resource))
  }
  def saveToEs(rdd: RDD[_], cfg: Map[String, String]) {
    val sparkCfg = new SparkSettingsManager().load(rdd.sparkContext.getConf)
    val config = new PropertiesSettings().load(sparkCfg.save())
    config.merge(cfg.asJava)

    rdd.sparkContext.runJob(rdd, new EsRDDWriter(config.save()).write _)
  }

  // JSON variant
  def saveJsonToEs(rdd: RDD[_], resource: String) { saveToEs(rdd, resource, Map(ES_INPUT_JSON -> true.toString)) }
  def saveJsonToEs(rdd: RDD[_], resource: String, params: Map[String, String]) {
    saveToEs(rdd, resource, collection.mutable.Map(params.toSeq: _*) += (ES_INPUT_JSON -> true.toString))
  }
  def saveJsonToEs(rdd: RDD[_], params: Map[String, String]) {
    saveToEs(rdd, collection.mutable.Map(params.toSeq: _*) += (ES_INPUT_JSON -> true.toString))
  }
}