package org.elasticsearch.spark;

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SchemaRDD


package object sql {

  implicit def sqlContextFunctions(sc: SQLContext)= new SQLContextFunctions(sc)

  class SQLContextFunctions(sc: SQLContext) extends Serializable {
    def esRDD() = EsSchemaRDDFunctions.esRDD(sc)
    def esRDD(resource: String) = EsSchemaRDDFunctions.esRDD(sc, resource)
    def esRDD(resource: String, query: String) = EsSchemaRDDFunctions.esRDD(sc, resource, query)
    def esRDD(params: scala.collection.Map[String, String]) = EsSchemaRDDFunctions.esRDD(sc, params)
  }
  
  implicit def sparkSchemaRDDFunctions(rdd: SchemaRDD) = new SparkSchemaRDDFunctions(rdd)

  class SparkSchemaRDDFunctions(rdd: SchemaRDD) extends Serializable {
    def saveToEs(resource: String) { EsSchemaRDDFunctions.saveToEs(rdd, resource) }
    def saveToEs(resource: String, params: scala.collection.Map[String, String]) { EsSchemaRDDFunctions.saveToEs(rdd, resource, params) }
    def saveToEs(params: scala.collection.Map[String, String]) { EsSchemaRDDFunctions.saveToEs(rdd, params)    }
  }
}