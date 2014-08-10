package org.elasticsearch.spark.api.java

import java.util.{Map => JMap}

import scala.collection.JavaConverters._

import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaRDD._
import org.apache.spark.rdd.RDD
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
import org.elasticsearch.hadoop.cfg.PropertiesSettings
import org.elasticsearch.spark.cfg.SparkSettingsManager
import org.elasticsearch.spark.rdd.{EsRDDFunctions => ERF}
import org.elasticsearch.spark.rdd.EsRDDWriter

object JavaEsSpark {

  // specify the return types to make sure the bytecode is generated properly (w/o any scala.collections in it)
  def esRDD(jsc: JavaSparkContext): JavaRDD[JMap[String, Object]] = fromRDD(ERF.esRDD(jsc))
  def esRDD(jsc: JavaSparkContext, resource: String): JavaRDD[JMap[String, Object]] = fromRDD(ERF.esRDD(jsc, resource))
  def esRDD(jsc: JavaSparkContext, resource: String, query: String): JavaRDD[JMap[String, Object]] = fromRDD(ERF.esRDD(jsc, resource, query))
  def esRDD(jsc: JavaSparkContext, cfg: JMap[String, String]): JavaRDD[JMap[String, Object]] = fromRDD(ERF.esRDD(jsc, cfg.asScala)) 

  def saveToEs(jrdd: JavaRDD[_], resource: String) = ERF.saveToEs(jrdd.rdd, resource)
  def saveToEs(jrdd: JavaRDD[_], resource: String, params: JMap[String, String]) = ERF.saveToEs(jrdd.rdd, params.asScala)
  def saveToEs(jrdd: JavaRDD[_], cfg: JMap[String, String]) = ERF.saveToEs(jrdd.rdd, cfg.asScala)
  
  def saveJsonToEs(jrdd: JavaRDD[String], resource: String) = ERF.saveJsonToEs(jrdd.rdd, resource)
  def saveJsonToEs(jrdd: JavaRDD[String], resource: String, params: JMap[String, String]) = ERF.saveJsonToEs(jrdd.rdd, params.asScala)
  def saveJsonToEs(jrdd: JavaRDD[String], cfg: JMap[String, String]) = ERF.saveJsonToEs(jrdd.rdd, cfg.asScala)

  def saveJsonByteArrayToEs(jrdd: JavaRDD[Array[Byte]], resource: String) = ERF.saveJsonToEs(jrdd.rdd, resource)
  def saveJsonByteArrayToEs(jrdd: JavaRDD[Array[Byte]], resource: String, params: JMap[String, String]) = ERF.saveJsonToEs(jrdd.rdd, params.asScala)
  def saveJsonByteArrayToEs(jrdd: JavaRDD[Array[Byte]], cfg: JMap[String, String]) = ERF.saveJsonToEs(jrdd.rdd, cfg.asScala)
}