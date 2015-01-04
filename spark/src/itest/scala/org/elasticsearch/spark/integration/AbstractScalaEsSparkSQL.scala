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

import java.sql.Timestamp
import java.util.concurrent.TimeUnit
import scala.collection.JavaConversions.propertiesAsScalaMap
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.IntegerType
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.StringType
import org.apache.spark.sql.StructField
import org.apache.spark.sql.StructType
import org.apache.spark.sql.TimestampType
import org.elasticsearch.hadoop.mr.RestUtils
import org.elasticsearch.hadoop.util.TestSettings
import org.elasticsearch.hadoop.util.TestUtils
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
import org.elasticsearch.spark.sql.sqlContextFunctions
import org.hamcrest.Matchers.containsString
import org.hamcrest.Matchers.is
import org.junit.AfterClass
import org.junit.Assert.assertThat
import org.junit.Assert.assertTrue
import org.junit.BeforeClass
import org.junit.FixMethodOrder
import org.junit.runners.MethodSorters
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
import org.junit.Test
import javax.xml.bind.DatatypeConverter
import org.apache.spark.sql.catalyst.expressions.GenericRow


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
    def testEsSchemaRDD1WriteWithMapping() {
      val schemaRDD = artistsAsSchemaRDD

      val target = "sparksql-test/scala-basic-write-id-mapping"
      schemaRDD.saveToEs(target, Map(ES_MAPPING_ID -> "id"))
      assertTrue(RestUtils.exists(target))
      assertThat(RestUtils.get(target + "/_search?"), containsString("345"))
      assertThat(RestUtils.exists(target + "/1"), is(true))
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

    @Test
    def testEsSchemaRDD3WriteWithRichMapping() {
      val input = TestUtils.sampleArtistsDat()
      val data = sc.textFile(input)

      val schema = StructType(Seq(StructField("id", IntegerType, false), 
        StructField("name", StringType, false),
        StructField("url", StringType, true),
        StructField("pictures", StringType, true),
        StructField("time", TimestampType, true),
        StructField("nested", 
            StructType(Seq(StructField("id", IntegerType, false), 
		        StructField("name", StringType, false),
		        StructField("url", StringType, true),
		        StructField("pictures", StringType, true),
		        StructField("time", TimestampType, true))), true)))
    
      val rowRDD = data.map(_.split("\t")).map(r => Row(r(0).toInt, r(1), r(2), r(3), new Timestamp(DatatypeConverter.parseDateTime(r(4)).getTimeInMillis()),
    		  											Row(r(0).toInt, r(1), r(2), r(3), new Timestamp(DatatypeConverter.parseDateTime(r(4)).getTimeInMillis()))))
      val schemaRDD = sqc.applySchema(rowRDD, schema)

      val target = "sparksql-test/scala-basic-write-rich-mapping-id-mapping"
      schemaRDD.saveToEs(target, Map(ES_MAPPING_ID -> "id"))
      assertTrue(RestUtils.exists(target))
      assertThat(RestUtils.get(target + "/_search?"), containsString("345"))
      assertThat(RestUtils.exists(target + "/1"), is(true))
    }

    @Test
    def testEsSchemaRDD4ReadRichMapping() {
      val target = "sparksql-test/scala-basic-write-rich-mapping-id-mapping"
        
      val schemaRDD = sqc.esRDD(target)
      
      assertTrue(schemaRDD.count > 300)
      println(schemaRDD.schemaString)
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
    
    @Test
    def testEsSchemaReadAsDataSource() {
      val target = "sparksql-test/scala-basic-write"
      val schemaRDD = sqc.sql("CREATE TEMPORARY TABLE sqlbasicread " + 
              "USING org.elasticsearch.spark.sql " +
              "OPTIONS (resource '" + target + "')");
      
      
      val allRDD = sqc.sql("SELECT * FROM sqlbasicread WHERE id >= 1 AND id <=10")
      println(allRDD.schemaString)
      
      val nameRDD = sqc.sql("SELECT name FROM sqlbasicread WHERE id >= 1 AND id <=10")
      
      println(nameRDD.schemaString)
      assertTrue(nameRDD.count == 10)
      nameRDD.take(7).foreach(println)
    }
}