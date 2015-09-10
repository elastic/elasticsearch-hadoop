package org.elasticsearch.spark.sql

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.JavaConverters.propertiesAsScalaMapConverter
import scala.collection.Map

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SchemaRDD
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_QUERY
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_RESOURCE_READ
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_RESOURCE_WRITE
import org.elasticsearch.hadoop.cfg.PropertiesSettings
import org.elasticsearch.spark.cfg.SparkSettingsManager

object EsSparkSQL {

  def esRDD(sc: SQLContext): SchemaRDD = esRDD(sc, Map.empty[String, String])
  def esRDD(sc: SQLContext, resource: String): SchemaRDD = esRDD(sc, Map(ES_RESOURCE_READ -> resource))
  def esRDD(sc: SQLContext, resource: String, query: String): SchemaRDD = esRDD(sc, Map(ES_RESOURCE_READ -> resource, ES_QUERY -> query))
  def esRDD(sc: SQLContext, cfg: Map[String, String]): SchemaRDD = {
    val esConf = new SparkSettingsManager().load(sc.sparkContext.getConf).copy();
    esConf.merge(cfg.asJava)

    val schema = SchemaUtils.discoverMapping(esConf)
    val rowRDD = new ScalaEsRowRDD(sc.sparkContext, esConf.asProperties.asScala, schema)
    sc.applySchema(rowRDD, schema.struct)
  }

  def esRDD(sc: SQLContext, resource: String, query: String, cfg: Map[String, String]): SchemaRDD = {
    esRDD(sc, collection.mutable.Map(cfg.toSeq: _*) += (ES_RESOURCE_READ -> resource, ES_QUERY -> query))
  }
  def esRDD(sc: SQLContext, resource: String, cfg: Map[String, String]): SchemaRDD = {
    esRDD(sc, collection.mutable.Map(cfg.toSeq: _*) += (ES_RESOURCE_READ -> resource))
  }

  def saveToEs(srdd: SchemaRDD, resource: String) {
    saveToEs(srdd, Map(ES_RESOURCE_WRITE -> resource))
  }
  def saveToEs(srdd: SchemaRDD, resource: String, cfg: Map[String, String]) {
    saveToEs(srdd, collection.mutable.Map(cfg.toSeq: _*) += (ES_RESOURCE_WRITE -> resource))
  }
  def saveToEs(srdd: SchemaRDD, cfg: Map[String, String]) {
    if (srdd == null || srdd.partitions.length == 0 || srdd.take(1).length == 0) {
      return
    }

    val sparkCtx = srdd.sparkContext
    val sparkCfg = new SparkSettingsManager().load(sparkCtx.getConf)
    val esCfg = new PropertiesSettings().load(sparkCfg.save())
    esCfg.merge(cfg.asJava)

    sparkCtx.runJob(srdd, new EsSchemaRDDWriter(srdd.schema, esCfg.save()).write _)
  }
}