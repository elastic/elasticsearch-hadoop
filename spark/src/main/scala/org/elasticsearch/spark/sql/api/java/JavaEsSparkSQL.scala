package org.elasticsearch.spark.sql.api.java

import java.util.{Map => JMap}

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.{Map => SMap}

import org.apache.spark.api.java.JavaRDD.fromRDD
import org.apache.spark.sql.api.java.JavaSQLContext
import org.apache.spark.sql.api.java.JavaSchemaRDD
import org.apache.spark.sql.api.java.{StructType => JStructType}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_QUERY
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_RESOURCE_READ
import org.elasticsearch.spark.sql.DataTypeConversions
import org.elasticsearch.spark.sql.EsSparkSQL
import org.elasticsearch.spark.sql.JavaEsRowRDD
import org.elasticsearch.spark.sql.MappingUtils

object JavaEsSparkSQL {

  // specify the return types to make sure the bytecode is generated properly (w/o any scala.collections in it)
  def esRDD(jsc: JavaSQLContext): JavaSchemaRDD = esRDD(jsc, SMap.empty[String, String])
  def esRDD(jsc: JavaSQLContext, resource: String): JavaSchemaRDD = esRDD(jsc, Map(ES_RESOURCE_READ -> resource))
  def esRDD(jsc: JavaSQLContext, resource: String, query: String): JavaSchemaRDD = esRDD(jsc, Map(ES_RESOURCE_READ -> resource, ES_QUERY -> query))
  def esRDD(jsc: JavaSQLContext, cfg: JMap[String, String]): JavaSchemaRDD = esRDD(jsc, cfg.asScala)
  
  private def esRDD(jsc: JavaSQLContext, cfg: SMap[String, String]): JavaSchemaRDD = {
    val rowRDD = new JavaEsRowRDD(jsc.sqlContext.sparkContext, cfg)
    val schema = DataTypeConversions.asJavaDataType(MappingUtils.discoverMapping(rowRDD.esCfg)).asInstanceOf[JStructType]
    jsc.applySchema(rowRDD, schema)
  }

  def saveToEs(jrdd: JavaSchemaRDD, resource: String) = EsSparkSQL.saveToEs(SQLUtils.baseSchemaRDD(jrdd) , resource)
  def saveToEs(jrdd: JavaSchemaRDD, resource: String, cfg: JMap[String, String]) = EsSparkSQL.saveToEs(SQLUtils.baseSchemaRDD(jrdd), resource, cfg.asScala)
  def saveToEs(jrdd: JavaSchemaRDD, cfg: JMap[String, String]) = EsSparkSQL.saveToEs(SQLUtils.baseSchemaRDD(jrdd), cfg.asScala)
}