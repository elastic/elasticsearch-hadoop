package org.elasticsearch.hadoop.spark.api.java

import java.util.{Map => JMap}

import scala.collection.JavaConverters._

import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaRDD._
import org.apache.spark.rdd.RDD
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
import org.elasticsearch.hadoop.cfg.PropertiesSettings
import org.elasticsearch.hadoop.spark.cfg.SparkSettingsManager
import org.elasticsearch.hadoop.spark.rdd.{EsRDDFunctions => ERF}
import org.elasticsearch.hadoop.spark.rdd.EsRDDWriter

object JavaEsSpark {

  def esRDD(jsc: JavaSparkContext) = fromRDD(ERF.esRDD(jsc))
  def esRDD(jsc: JavaSparkContext, resource: String) = fromRDD(ERF.esRDD(jsc, resource))
  def esRDD(jsc: JavaSparkContext, resource: String, query: String) = fromRDD(ERF.esRDD(jsc, resource, query))
  def esRDD(jsc: JavaSparkContext, cfg: JMap[String, String]) = fromRDD(ERF.esRDD(jsc, cfg.asScala)) 

  def saveToEs(jrdd: JavaRDD[_], resource: String) = ERF.saveToEs(jrdd.rdd, resource)
  def saveToEs(jrdd: JavaRDD[_], resource: String, params: JMap[String, String]) = ERF.saveToEs(jrdd.rdd, params.asScala)
  def saveToEs(jrdd: JavaRDD[_], cfg: JMap[String, String]) = ERF.saveToEs(jrdd.rdd, cfg.asScala)
}