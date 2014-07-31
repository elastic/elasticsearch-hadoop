package org.elasticsearch.hadoop.spark.rdd

import scala.collection.JavaConverters._
import scala.collection.Map
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_QUERY
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_RESOURCE_READ
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_RESOURCE_WRITE
import org.elasticsearch.hadoop.cfg.PropertiesSettings
import org.elasticsearch.hadoop.spark.cfg.SparkSettingsManager
import org.apache.spark.api.java.JavaSparkContext

private[spark] object EsRDDFunctions {

  def esRDD(sc: SparkContext) = new ScalaEsRDD(sc)
  def esRDD(sc: SparkContext, map: Map[String, String]) = new ScalaEsRDD(sc, map)
  def esRDD(sc: SparkContext, resource: String) = new ScalaEsRDD(sc, Map(ES_RESOURCE_READ -> resource))
  def esRDD(sc: SparkContext, resource: String, query: String) = 
    new ScalaEsRDD(sc, Map(ES_RESOURCE_READ -> resource, ES_QUERY -> query))

  def esRDD(jsc: JavaSparkContext) = new JavaEsRDD(jsc.sc)
  def esRDD(jsc: JavaSparkContext, map: Map[String, String]) = new JavaEsRDD(jsc.sc, map)
  def esRDD(jsc: JavaSparkContext, resource: String) = new JavaEsRDD(jsc.sc, Map(ES_RESOURCE_READ -> resource))
  def esRDD(jsc: JavaSparkContext, resource: String, query: String) = 
    new ScalaEsRDD(jsc.sc, Map(ES_RESOURCE_READ -> resource, ES_QUERY -> query))

  def saveToEs(rdd: RDD[_], resource: String) {
    saveToEs(rdd, Map(ES_RESOURCE_WRITE -> resource))
  }
  
  def saveToEs(rdd: RDD[_], resource: String, params: Map[String, String]) {
    val copyParams = collection.mutable.Map(params.toSeq: _*)
    copyParams.put(ES_RESOURCE_WRITE, resource)
    saveToEs(rdd, copyParams)
  }
  
  def saveToEs(rdd: RDD[_], params: Map[String, String]) {
    val sparkCfg = new SparkSettingsManager().load(rdd.sparkContext.getConf)
    val cfg = new PropertiesSettings().load(sparkCfg.save())
    cfg.merge(params.asJava)
      
    rdd.sparkContext.runJob(rdd, new EsRDDWriter(cfg.save()).write _)
  }
}