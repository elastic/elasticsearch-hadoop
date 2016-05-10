package org.elasticsearch.spark.sql.api.java

import java.util.{Map => JMap}

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.{Map => SMap}

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_QUERY
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_RESOURCE_READ
import org.elasticsearch.spark.sql.EsSparkSQL

object JavaEsSparkSQL {

  // specify the return types to make sure the bytecode is generated properly (w/o any scala.collections in it)
  def esDF(sc: SQLContext): DataFrame = EsSparkSQL.esDF(sc, SMap.empty[String, String])
  def esDF(sc: SQLContext, resource: String): DataFrame = EsSparkSQL.esDF(sc, Map(ES_RESOURCE_READ -> resource))
  def esDF(sc: SQLContext, resource: String, query: String): DataFrame = EsSparkSQL.esDF(sc, Map(ES_RESOURCE_READ -> resource, ES_QUERY -> query))
  def esDF(sc: SQLContext, cfg: JMap[String, String]): DataFrame = EsSparkSQL.esDF(sc, cfg.asScala)
  def esDF(sc: SQLContext, resource: String, cfg: JMap[String, String]): DataFrame = EsSparkSQL.esDF(sc, resource, cfg.asScala)
  def esDF(sc: SQLContext, resource: String, query: String, cfg: JMap[String, String]): DataFrame = EsSparkSQL.esDF(sc, resource, query, cfg.asScala)

  
  def saveToEs(df: DataFrame, resource: String) = EsSparkSQL.saveToEs(df , resource)
  def saveToEs(df: DataFrame, resource: String, cfg: JMap[String, String]) = EsSparkSQL.saveToEs(df, resource, cfg.asScala)
  def saveToEs(df: DataFrame, cfg: JMap[String, String]) = EsSparkSQL.saveToEs(df, cfg.asScala)
}