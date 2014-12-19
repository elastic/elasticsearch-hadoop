package org.elasticsearch;

import scala.reflect.ClassTag

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.elasticsearch.spark.rdd.EsSpark


package object spark {

  implicit def sparkContextFunctions(sc: SparkContext)= new SparkContextFunctions(sc)

  class SparkContextFunctions(sc: SparkContext) extends Serializable {
    def esRDD() = EsSpark.esRDD(sc)
    def esRDD(resource: String) = EsSpark.esRDD(sc, resource)
    def esRDD(resource: String, query: String) = EsSpark.esRDD(sc, resource, query)
    def esRDD(cfg: scala.collection.Map[String, String]) = EsSpark.esRDD(sc, cfg)
    
    def esJsonRDD() = EsSpark.esJsonRDD(sc)
    def esJsonRDD(resource: String) = EsSpark.esJsonRDD(sc, resource)
    def esJsonRDD(resource: String, query: String) = EsSpark.esJsonRDD(sc, resource, query)
    def esJsonRDD(params: scala.collection.Map[String, String]) = EsSpark.esJsonRDD(sc, params)

  }
  
  implicit def sparkRDDFunctions[T : ClassTag](rdd: RDD[T]) = new SparkRDDFunctions[T](rdd)

  class SparkRDDFunctions[T : ClassTag](rdd: RDD[T]) extends Serializable {
    def saveToEs(resource: String) { EsSpark.saveToEs(rdd, resource) }
    def saveToEs(resource: String, params: scala.collection.Map[String, String]) { EsSpark.saveToEs(rdd, resource, params) }
    def saveToEs(cfg: scala.collection.Map[String, String]) { EsSpark.saveToEs(rdd, cfg)    }
  }
    
  implicit def sparkStringJsonRDDFunctions(rdd: RDD[String]) = new SparkJsonRDDFunctions[String](rdd)
  implicit def sparkByteArrayJsonRDDFunctions(rdd: RDD[Array[Byte]]) = new SparkJsonRDDFunctions[Array[Byte]](rdd)

  class SparkJsonRDDFunctions[T : ClassTag](rdd: RDD[T]) extends Serializable {
    def saveJsonToEs(resource: String) { EsSpark.saveJsonToEs(rdd, resource) }
    def saveJsonToEs(resource: String, params: scala.collection.Map[String, String]) { EsSpark.saveJsonToEs(rdd, resource, params) }
    def saveJsonToEs(cfg: scala.collection.Map[String, String]) { EsSpark.saveJsonToEs(rdd, cfg) }
  }
}