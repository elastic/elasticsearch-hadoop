package org.elasticsearch.spark.sql.api.java

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.JavaConverters.propertiesAsScalaMapConverter
import scala.collection.Map

import java.util.{Map => JMap}
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.{Map => SMap}
import org.apache.spark.api.java.JavaRDD.fromRDD
import org.apache.spark.sql.api.java.JavaSQLContext
import org.apache.spark.sql.api.java.JavaSchemaRDD
import org.apache.spark.sql.api.java.{StructType => JStructType}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_QUERY
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_RESOURCE_READ
import org.elasticsearch.spark.sql.EsSparkSQL
import org.elasticsearch.spark.sql.JavaEsRowRDD
import org.elasticsearch.spark.sql.SchemaUtils
import org.elasticsearch.spark.cfg.SparkSettingsManager
import org.elasticsearch.spark.sql.Utils
import org.elasticsearch.spark.sql.Utils

object JavaEsSparkSQL {

  def esRDD(jsc: JavaSQLContext): JavaSchemaRDD = esRDD(jsc, SMap.empty[String, String])
  def esRDD(jsc: JavaSQLContext, resource: String): JavaSchemaRDD = esRDD(jsc, Map(ES_RESOURCE_READ -> resource))
  def esRDD(jsc: JavaSQLContext, resource: String, query: String): JavaSchemaRDD = esRDD(jsc, Map(ES_RESOURCE_READ -> resource, ES_QUERY -> query))
  def esRDD(jsc: JavaSQLContext, cfg: JMap[String, String]): JavaSchemaRDD = esRDD(jsc, cfg.asScala)

  private def esRDD(jsc: JavaSQLContext, cfg: SMap[String, String]): JavaSchemaRDD = {
    val esConf = new SparkSettingsManager().load(jsc.sqlContext.sparkContext.getConf).copy();
    esConf.merge(cfg.asJava)

    val schema = SchemaUtils.discoverMapping(esConf)
    val rowRDD = new JavaEsRowRDD(jsc.sqlContext.sparkContext, esConf.asProperties.asScala, schema)
    val jSchema = SQLUtils.asJavaDataType(schema.struct).asInstanceOf[JStructType]
    jsc.applySchema(rowRDD, jSchema)
  }

  def esRDD(jsc: JavaSQLContext, resource: String, query: String, cfg: Map[String, String]): JavaSchemaRDD = {
    esRDD(jsc, collection.mutable.Map(cfg.toSeq: _*) += (ES_RESOURCE_READ -> resource, ES_QUERY -> query))
  }
  def esRDD(jsc: JavaSQLContext, resource: String, cfg: Map[String, String]): JavaSchemaRDD = {
    esRDD(jsc, collection.mutable.Map(cfg.toSeq: _*) += (ES_RESOURCE_READ -> resource))
  }

  def saveToEs(jrdd: JavaSchemaRDD, resource: String) = EsSparkSQL.saveToEs(SQLUtils.baseSchemaRDD(jrdd) , resource)
  def saveToEs(jrdd: JavaSchemaRDD, resource: String, cfg: JMap[String, String]) = EsSparkSQL.saveToEs(SQLUtils.baseSchemaRDD(jrdd), resource, cfg.asScala)
  def saveToEs(jrdd: JavaSchemaRDD, cfg: JMap[String, String]) = EsSparkSQL.saveToEs(SQLUtils.baseSchemaRDD(jrdd), cfg.asScala)
}