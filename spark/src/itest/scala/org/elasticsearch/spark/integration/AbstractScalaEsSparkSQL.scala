/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.spark.integration;

import java.util.concurrent.TimeUnit
import scala.annotation.migration
import scala.collection.JavaConversions.propertiesAsScalaMap
import scala.runtime.ScalaRunTime.stringOf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.PairRDDFunctions._
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
import org.elasticsearch.hadoop.mr.RestUtils
import org.elasticsearch.hadoop.util.TestSettings
import org.elasticsearch.hadoop.util.TestUtils
import org.elasticsearch.spark.sql._
import org.hamcrest.Matchers._
import org.junit.AfterClass
import org.junit.Assert._
import org.junit.Assert.assertTrue
import org.junit.BeforeClass
import org.junit.Test
import javax.xml.bind.DatatypeConverter
import java.sql.Timestamp
import org.junit.FixMethodOrder
import org.junit.runners.MethodSorters

object AbstractScalaEsScalaSparkSQL {
  @transient val conf = new SparkConf().setAll(TestSettings.TESTING_PROPS).setMaster("local").setAppName("estest");
  @transient var cfg: SparkConf = null
  @transient var sc: SparkContext = null
  @transient var sqc: SQLContext = null

  @BeforeClass
  def setup() {
    sc = new SparkContext(conf)
    sqc = new SQLContext(sc)
  }
  
  @AfterClass
  def cleanup() {
    if (sc != null) {
      sc.stop
      // give jetty time to clean its act up
      Thread.sleep(TimeUnit.SECONDS.toMillis(3))
    }
  }
}

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
class AbstractScalaEsScalaSparkSQL extends Serializable {

	val sc = AbstractScalaEsScalaSparkSQL.sc
	val sqc = AbstractScalaEsScalaSparkSQL.sqc
	
    @Test
    def testBasicRead() {
        val schemaRDD = artistsAsSchemaRDD
        assertTrue(schemaRDD.count > 300)
        schemaRDD.registerTempTable("datfile")
        println(schemaRDD.schemaString)
        //schemaRDD.take(5).foreach(println)
        val results = sqc.sql("SELECT name FROM datfile WHERE id >=1 AND id <=10")
        //results.take(5).foreach(println)
    }

    @Test
    def testEsSchemaRDD1Write() {
      val schemaRDD = artistsAsSchemaRDD

      val target = "sparksql-test/scala-basic-write"
      schemaRDD.saveToEs(target)
      assertTrue(RestUtils.exists(target))
      assertThat(RestUtils.get(target + "/_search?"), containsString("345"))
    }
    
    @Test
    def testEsSchemaRDD2Read() {
      val target = "sparksql-test/scala-basic-write"
        
      val schemaRDD = sqc.esRDD(target)
      assertTrue(schemaRDD.count > 300)
      val schema = schemaRDD.schemaString
      assertTrue(schema.contains("id: long"))
      assertTrue(schema.contains("name: string"))
      assertTrue(schema.contains("pictures: string"))
      assertTrue(schema.contains("time: long"))
      assertTrue(schema.contains("url: string"))
      
      //schemaRDD.take(5).foreach(println)
      
      schemaRDD.registerTempTable("basicRead")
      val nameRDD = sqc.sql("SELECT name FROM basicRead WHERE id >= 1 AND id <=10") 
      nameRDD.take(7).foreach(println)
      assertTrue(nameRDD.count == 10)
    }

    private def artistsAsSchemaRDD = {         
      val input = TestUtils.sampleArtistsDat()
        val data = sc.textFile(input)

        val schema = StructType(Seq(StructField("id", IntegerType, false), 
            StructField("name", StringType, false),
            StructField("url", StringType, true),
            StructField("pictures", StringType, true),
            StructField("time", TimestampType, true)))
        
        val rowRDD = data.map(_.split("\t")).map(r => Row(r(0).toInt, r(1), r(2), r(3), new Timestamp(DatatypeConverter.parseDateTime(r(4)).getTimeInMillis())))
        val schemaRDD = sqc.applySchema(rowRDD, schema)
        schemaRDD
    }
}