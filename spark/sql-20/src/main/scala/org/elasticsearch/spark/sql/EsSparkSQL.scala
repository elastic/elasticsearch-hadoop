package org.elasticsearch.spark.sql

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.JavaConverters.propertiesAsScalaMapConverter
import scala.collection.Map

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_QUERY
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_RESOURCE_READ
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_RESOURCE_WRITE
import org.elasticsearch.hadoop.cfg.PropertiesSettings
import org.elasticsearch.hadoop.util.ObjectUtils
import org.elasticsearch.spark.cfg.SparkSettingsManager
import org.elasticsearch.hadoop.rest.InitializationUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset

object EsSparkSQL {

  private val init = { ObjectUtils.loadClass("org.elasticsearch.spark.rdd.CompatUtils", classOf[ObjectUtils].getClassLoader) }

  //
  // Read
  //
  
  def esDF(sc: SQLContext): DataFrame = esDF(sc, Map.empty[String, String])
  def esDF(sc: SQLContext, resource: String): DataFrame = esDF(sc, Map(ES_RESOURCE_READ -> resource))
  def esDF(sc: SQLContext, resource: String, query: String): DataFrame = esDF(sc, Map(ES_RESOURCE_READ -> resource, ES_QUERY -> query))
  def esDF(sc: SQLContext, cfg: Map[String, String]): DataFrame = {
    val esConf = new SparkSettingsManager().load(sc.sparkContext.getConf).copy();
    esConf.merge(cfg.asJava)

    sc.read.format("org.elasticsearch.spark.sql").options(esConf.asProperties.asScala.toMap).load
  }

  def esDF(sc: SQLContext, resource: String, query: String, cfg: Map[String, String]): DataFrame = {
    esDF(sc, collection.mutable.Map(cfg.toSeq: _*) += (ES_RESOURCE_READ -> resource, ES_QUERY -> query))
  }

  def esDF(sc: SQLContext, resource: String, cfg: Map[String, String]): DataFrame = {
    esDF(sc, collection.mutable.Map(cfg.toSeq: _*) += (ES_RESOURCE_READ -> resource))
  }

  // SparkSession variant
  def esDF(ss: SparkSession): DataFrame = esDF(ss.sqlContext, Map.empty[String, String])
  def esDF(ss: SparkSession, resource: String): DataFrame = esDF(ss.sqlContext, Map(ES_RESOURCE_READ -> resource))
  def esDF(ss: SparkSession, resource: String, query: String): DataFrame = esDF(ss.sqlContext, Map(ES_RESOURCE_READ -> resource, ES_QUERY -> query))
  def esDF(ss: SparkSession, cfg: Map[String, String]): DataFrame = esDF(ss.sqlContext, cfg) 
  def esDF(ss: SparkSession, resource: String, query: String, cfg: Map[String, String]): DataFrame = esDF(ss.sqlContext, resource, query, cfg)
  def esDF(ss: SparkSession, resource: String, cfg: Map[String, String]): DataFrame = esDF(ss.sqlContext, resource, cfg)
  
  //
  // Write
  //
  
  def saveToEs(srdd: Dataset[_], resource: String) {
    saveToEs(srdd, Map(ES_RESOURCE_WRITE -> resource))
  }
  def saveToEs(srdd: Dataset[_], resource: String, cfg: Map[String, String]) {
    saveToEs(srdd, collection.mutable.Map(cfg.toSeq: _*) += (ES_RESOURCE_WRITE -> resource))
  }
  def saveToEs(srdd: Dataset[_], cfg: Map[String, String]) {
     if (srdd == null) {
      return
    }
     
    val sparkCtx = srdd.sqlContext.sparkContext
    val sparkCfg = new SparkSettingsManager().load(sparkCtx.getConf)
    val esCfg = new PropertiesSettings().load(sparkCfg.save())
    esCfg.merge(cfg.asJava)

    InitializationUtils.checkIdForOperation(esCfg);
    InitializationUtils.checkIndexExistence(esCfg, null);
    
    sparkCtx.runJob(srdd.toDF().rdd, new EsDataFrameWriter(srdd.schema, esCfg.save()).write _)
  }
}