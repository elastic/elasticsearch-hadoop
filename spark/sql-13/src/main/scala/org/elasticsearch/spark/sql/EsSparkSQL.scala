package org.elasticsearch.spark.sql

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.Map

import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.api.java.JavaRDD.fromRDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_QUERY
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_RESOURCE_READ
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_RESOURCE_WRITE
import org.elasticsearch.hadoop.cfg.PropertiesSettings
import org.elasticsearch.spark.cfg.SparkSettingsManager

object EsSparkSQL {

  def esDF(sc: SQLContext): DataFrame = esDF(sc, Map.empty[String, String])
  def esDF(sc: SQLContext, resource: String): DataFrame = esDF(sc, Map(ES_RESOURCE_READ -> resource))
  def esDF(sc: SQLContext, resource: String, query: String): DataFrame = esDF(sc, Map(ES_RESOURCE_READ -> resource, ES_QUERY -> query))
  def esDF(sc: SQLContext, cfg: Map[String, String]): DataFrame = {
    val rowRDD = new ScalaEsRowRDD(sc.sparkContext, cfg)
    val schema = MappingUtils.discoverMapping(rowRDD.esCfg)
    sc.createDataFrame(rowRDD, schema)
  }

  def saveToEs(srdd: DataFrame, resource: String) {
    saveToEs(srdd, Map(ES_RESOURCE_WRITE -> resource))
  }
  def saveToEs(srdd: DataFrame, resource: String, cfg: Map[String, String]) {
    saveToEs(srdd, collection.mutable.Map(cfg.toSeq: _*) += (ES_RESOURCE_WRITE -> resource))
  }
  def saveToEs(srdd: DataFrame, cfg: Map[String, String]) {
    val sparkCtx = srdd.sqlContext.sparkContext
    val sparkCfg = new SparkSettingsManager().load(sparkCtx.getConf)
    val esCfg = new PropertiesSettings().load(sparkCfg.save())
    esCfg.merge(cfg.asJava)
    
    sparkCtx.runJob(srdd.rdd, new EsDataFrameWriter(srdd.schema, esCfg.save()).write _)
  }
}