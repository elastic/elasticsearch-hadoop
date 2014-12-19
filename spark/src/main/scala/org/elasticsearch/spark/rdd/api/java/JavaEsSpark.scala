package org.elasticsearch.spark.rdd.api.java

import java.util.{Map => JMap}

import scala.collection.JavaConverters.mapAsScalaMapConverter

import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.JavaPairRDD.fromRDD
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_QUERY
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_OUTPUT_JSON
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_RESOURCE_READ
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark.rdd.JavaEsRDD

object JavaEsSpark {

  // specify the return types to make sure the bytecode is generated properly (w/o any scala.collections in it)
  def esRDD(jsc: JavaSparkContext): JavaPairRDD[String, JMap[String, Object]] = fromRDD(new JavaEsRDD[JMap[String, Object]](jsc.sc))
  def esRDD(jsc: JavaSparkContext, resource: String): JavaPairRDD[String, JMap[String, Object]] = 
    fromRDD(new JavaEsRDD[JMap[String, Object]](jsc.sc, Map(ES_RESOURCE_READ -> resource)))
  def esRDD(jsc: JavaSparkContext, resource: String, query: String): JavaPairRDD[String, JMap[String, Object]] = 
    fromRDD(new JavaEsRDD[JMap[String, Object]](jsc.sc, Map(ES_RESOURCE_READ -> resource, ES_QUERY -> query)))
  def esRDD(jsc: JavaSparkContext, cfg: JMap[String, String]): JavaPairRDD[String, JMap[String, Object]] = 
    fromRDD(new JavaEsRDD[JMap[String, Object]](jsc.sc, cfg.asScala)) 

  def esJsonRDD(jsc: JavaSparkContext): JavaPairRDD[String, String] = fromRDD(new JavaEsRDD[String](jsc.sc, Map(ES_OUTPUT_JSON -> true.toString)))
  def esJsonRDD(jsc: JavaSparkContext, resource: String): JavaPairRDD[String, String] = 
    fromRDD(new JavaEsRDD[String](jsc.sc, Map(ES_RESOURCE_READ -> resource, ES_OUTPUT_JSON -> true.toString)))
  def esJsonRDD(jsc: JavaSparkContext, resource: String, query: String): JavaPairRDD[String, String] = 
    fromRDD(new JavaEsRDD[String](jsc.sc, Map(ES_RESOURCE_READ -> resource, ES_QUERY -> query, ES_OUTPUT_JSON -> true.toString)))
  def esJsonRDD(jsc: JavaSparkContext, cfg: JMap[String, String]): JavaPairRDD[String, String] = 
    fromRDD(new JavaEsRDD[String](jsc.sc, cfg.asScala += (ES_OUTPUT_JSON -> true.toString))) 
  
  def saveToEs(jrdd: JavaRDD[_], resource: String) = EsSpark.saveToEs(jrdd.rdd, resource)
  def saveToEs(jrdd: JavaRDD[_], resource: String, cfg: JMap[String, String]) = EsSpark.saveToEs(jrdd.rdd, resource, cfg.asScala)
  def saveToEs(jrdd: JavaRDD[_], cfg: JMap[String, String]) = EsSpark.saveToEs(jrdd.rdd, cfg.asScala)
  
  def saveJsonToEs(jrdd: JavaRDD[String], resource: String) = EsSpark.saveJsonToEs(jrdd.rdd, resource)
  def saveJsonToEs(jrdd: JavaRDD[String], resource: String, cfg: JMap[String, String]) = EsSpark.saveJsonToEs(jrdd.rdd, resource, cfg.asScala)
  def saveJsonToEs(jrdd: JavaRDD[String], cfg: JMap[String, String]) = EsSpark.saveJsonToEs(jrdd.rdd, cfg.asScala)

  def saveJsonByteArrayToEs(jrdd: JavaRDD[Array[Byte]], resource: String) = EsSpark.saveJsonToEs(jrdd.rdd, resource)
  def saveJsonByteArrayToEs(jrdd: JavaRDD[Array[Byte]], resource: String, cfg: JMap[String, String]) = EsSpark.saveJsonToEs(jrdd.rdd, resource, cfg.asScala)
  def saveJsonByteArrayToEs(jrdd: JavaRDD[Array[Byte]], cfg: JMap[String, String]) = EsSpark.saveJsonToEs(jrdd.rdd, cfg.asScala)
}