package org.elasticsearch.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit.Test

class EsSparkSQLTest {

  @Test
  def parameters(): Unit = {
    val sparkConf = new SparkConf().setAppName("session conf").setMaster("local[*]")
    val session1 = SparkSession.builder().config(sparkConf).getOrCreate()
    session1.conf.set("es.nodes", "127.0.0.1")
    session1.conf.set("es.port", "9200")

    val session2 = SparkSession.builder().config(sparkConf).getOrCreate()
    session2.conf.set("es.nodes", "localhost")
    session2.conf.set("es.port", "9201")

    assertTrue(session1.conf.get("es.nodes").equals("127.0.0.1"))
    assertTrue(session1.conf.get("es.port").equals("9200"))

    assertTrue(session2.conf.get("es.nodes").equals("localhost"))
    assertTrue(session2.conf.get("es.port").equals("9201"))

  }
}