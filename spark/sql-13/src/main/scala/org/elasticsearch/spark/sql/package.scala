package org.elasticsearch.spark;

import scala.language.implicitConversions

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.elasticsearch.spark.sql.EsSparkSQL

package object sql {

  implicit def sqlContextFunctions(sc: SQLContext)= new SQLContextFunctions(sc)

  class SQLContextFunctions(sc: SQLContext) extends Serializable {
    def esDF() = EsSparkSQL.esDF(sc)
    def esDF(resource: String) = EsSparkSQL.esDF(sc, resource)
    def esDF(resource: String, query: String) = EsSparkSQL.esDF(sc, resource, query)
    def esDF(cfg: scala.collection.Map[String, String]) = EsSparkSQL.esDF(sc, cfg)
    def esDF(resource: String, cfg: scala.collection.Map[String, String]) = EsSparkSQL.esDF(sc, resource, cfg)
    def esDF(resource: String, query: String, cfg: scala.collection.Map[String, String]) = EsSparkSQL.esDF(sc, resource, query, cfg)
  }

  implicit def sparkDataFrameFunctions(df: DataFrame) = new SparkDataFrameFunctions(df)

  class SparkDataFrameFunctions(df: DataFrame) extends Serializable {
    def saveToEs(resource: String) { EsSparkSQL.saveToEs(df, resource) }
    def saveToEs(resource: String, cfg: scala.collection.Map[String, String]) { EsSparkSQL.saveToEs(df, resource, cfg) }
    def saveToEs(cfg: scala.collection.Map[String, String]) { EsSparkSQL.saveToEs(df, cfg)    }
  }
}