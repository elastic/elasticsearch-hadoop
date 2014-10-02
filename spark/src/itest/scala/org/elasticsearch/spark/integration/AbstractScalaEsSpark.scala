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
import org.elasticsearch.hadoop.mr.RestUtils
import org.elasticsearch.hadoop.util.TestSettings
import org.elasticsearch.hadoop.util.TestUtils
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark
import org.hamcrest.Matchers._
import org.junit.AfterClass
import org.junit.Assert._
import org.junit.Assert.assertTrue
import org.junit.BeforeClass
import org.junit.Test
import org.apache.spark.rdd.PairRDDFunctions

object AbstractScalaEsScalaSpark {
  @transient val conf = new SparkConf().setAll(TestSettings.TESTING_PROPS).setMaster("local").setAppName("estest");
  @transient var cfg: SparkConf = null
  @transient var sc: SparkContext = null

  @BeforeClass
  def setup() {
    sc = new SparkContext(conf)
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

class AbstractScalaEsScalaSpark extends Serializable {

	val sc = AbstractScalaEsScalaSpark.sc
	
    @Test
    def testBasicRead() {
        val input = TestUtils.sampleArtistsDat()
        val data = sc.textFile(input).cache();

        assertTrue(data.count > 300)
    }

    @Test
    def testEsRDDWrite() {
      val doc1 = Map("one" -> null, "two" -> Set("2"), "three" -> (".", "..", "..."))
      val doc2 = Map("OTP" -> "Otopeni", "SFO" -> "San Fran")

      sc.makeRDD(Seq(doc1, doc2)).saveToEs("spark-test/scala-basic-write")
      assertTrue(RestUtils.exists("spark-test/scala-basic-write"))
      assertThat(RestUtils.get("spark-test/scala-basic-write/_search?"), containsString(""))
    }

    @Test
    def testEsRDDWriteWithMappingId() {
      val doc1 = Map("one" -> null, "two" -> Set("2"), "three" -> (".", "..", "..."), "number" -> 1)
      val doc2 = Map("OTP" -> "Otopeni", "SFO" -> "San Fran", "number" -> 2)

      val target = "spark-test/scala-id-write";

      sc.makeRDD(Seq(doc1, doc2)).saveToEs(target, Map(ES_MAPPING_ID -> "number"))
      assertTrue(RestUtils.exists(target + "/1"));
      assertTrue(RestUtils.exists(target + "/2"));

      assertThat(RestUtils.get(target + "/_search?"), containsString("SFO"))
    }

    @Test
    def testEsMultiIndexRDDWrite() {
      val trip1 = Map("reason" -> "business", "airport" -> "SFO")
      val trip2 = Map("participants" -> 5, "airport" -> "OTP")

      sc.makeRDD(Seq(trip1, trip2)).saveToEs("spark-test/trip-{airport}")
      assertTrue(RestUtils.exists("spark-test/trip-OTP"))
      assertTrue(RestUtils.exists("spark-test/trip-SFO"))

      assertThat(RestUtils.get("spark-test/trip-SFO/_search?"), containsString("business"))
      assertThat(RestUtils.get("spark-test/trip-OTP/_search?"), containsString("participants"))
    }

    @Test
    def testEsWriteAsJsonMultiWrite() {
      val json1 = "{\"reason\" : \"business\",\"airport\" : \"SFO\"}";
      val json2 = "{\"participants\" : 5,\"airport\" : \"OTP\"}"

      sc.makeRDD(Seq(json1, json2)).saveJsonToEs("spark-test/json-{airport}")

      val json1BA = json1.getBytes()
      val json2BA = json2.getBytes()

      sc.makeRDD(Seq(json1BA, json2BA)).saveJsonToEs("spark-test/json-ba-{airport}")

      assertTrue(RestUtils.exists("spark-test/json-SFO"))
      assertTrue(RestUtils.exists("spark-test/json-OTP"))

      assertTrue(RestUtils.exists("spark-test/json-ba-SFO"))
      assertTrue(RestUtils.exists("spark-test/json-ba-OTP"))

      assertThat(RestUtils.get("spark-test/json-SFO/_search?"), containsString("business"))
      assertThat(RestUtils.get("spark-test/json-OTP/_search?"), containsString("participants"))
    }
    
    @Test
    def testEsRDDRead() {
      val target = "spark-test/scala-basic-read"
      RestUtils.touch("spark-test")
      RestUtils.putData(target, "{\"message\" : \"Hello World\",\"message_date\" : \"2014-05-25\"}".getBytes())
      RestUtils.putData(target, "{\"message\" : \"Goodbye World\",\"message_date\" : \"2014-05-25\"}".getBytes())
      RestUtils.refresh("spark-test");

      val esData = EsSpark.esRDD(sc, target)
      val messages = esData.filter(doc => doc._2.find(_.toString.contains("message")).nonEmpty)

      assertTrue(messages.count() ==  2)
      assertNotNull(messages.take(10))
      assertNotNull(messages)
    }
}