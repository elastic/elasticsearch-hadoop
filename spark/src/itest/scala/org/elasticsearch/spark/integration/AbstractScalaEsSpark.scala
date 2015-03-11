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
import scala.collection.JavaConversions.propertiesAsScalaMap
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_INPUT_JSON
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_MAPPING_EXCLUDE
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_MAPPING_ID
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_QUERY
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_RESOURCE
import org.elasticsearch.hadoop.mr.RestUtils
import org.elasticsearch.hadoop.util.TestSettings
import org.elasticsearch.hadoop.util.TestUtils
import org.elasticsearch.spark.rdd.Metadata.ID
import org.elasticsearch.spark.rdd.Metadata.TTL
import org.elasticsearch.spark.rdd.Metadata.VERSION
import org.elasticsearch.spark.sparkByteArrayJsonRDDFunctions
import org.elasticsearch.spark.sparkPairRDDFunctions
import org.elasticsearch.spark.sparkRDDFunctions
import org.elasticsearch.spark.sparkStringJsonRDDFunctions
import org.hamcrest.Matchers.containsString
import org.hamcrest.Matchers.not
import org.junit.AfterClass
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertThat
import org.junit.Assert.assertTrue
import org.junit.BeforeClass
import java.awt.Polygon
import org.elasticsearch.spark.rdd.EsSpark
import org.junit.Test
import org.elasticsearch.hadoop.serialization.EsHadoopSerializationException
import org.apache.spark.SparkException
import org.elasticsearch.spark.serialization.ReflectionUtils
import org.elasticsearch.spark.serialization.Bean

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
  
  case class ModuleCaseClass(id: Integer, departure: String, var arrival: String) {
    var l = math.Pi
  }
}

case class Trip(departure: String, arrival: String) {
  var extra = math.Pi
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

  @Test(expected = classOf[SparkException])
  def testNestedUnknownCharacter() {
    val doc = Map("itemId" -> "1", "map" -> Map("lat" -> 1.23, "lon" -> -70.12), "list" -> ("A", "B", "C"), "unknown" -> new Polygon())
    sc.makeRDD(Seq(doc)).saveToEs("spark-test/nested-map")
  }

  @Test
  def testEsRDDWriteCaseClass() {
    val javaBean = new Bean("bar", 1, true)
    val caseClass1 = Trip("OTP", "SFO")
    val caseClass2 = AbstractScalaEsScalaSpark.ModuleCaseClass(1, "OTP", "MUC")

    val vals = ReflectionUtils.caseClassValues(caseClass2)
    
    sc.makeRDD(Seq(javaBean, caseClass1)).saveToEs("spark-test/scala-basic-write-objects")
    sc.makeRDD(Seq(javaBean, caseClass2)).saveToEs("spark-test/scala-basic-write-objects", Map("es.mapping.id"->"id"))
    
    assertTrue(RestUtils.exists("spark-test/scala-basic-write-objects"))
    assertThat(RestUtils.get("spark-test/scala-basic-write-objects/_search?"), containsString(""))
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
  def testEsRDDWriteWithDynamicMapping() {
    val doc1 = Map("one" -> null, "two" -> Set("2"), "three" -> (".", "..", "..."), "number" -> 1)
    val doc2 = Map("OTP" -> "Otopeni", "SFO" -> "San Fran", "number" -> 2)

    val target = "spark-test/scala-dyn-id-write";

    val pairRDD = sc.makeRDD(Seq((3, doc1), (4, doc2))).saveToEsWithMeta(target)

    assertTrue(RestUtils.exists(target + "/3"))
    assertTrue(RestUtils.exists(target + "/4"))

    assertThat(RestUtils.get(target + "/_search?"), containsString("SFO"))
  }

  @Test
  def testEsRDDWriteWithDynamicMapMapping() {
    val doc1 = Map("one" -> null, "two" -> Set("2"), "three" -> (".", "..", "..."), "number" -> 1)
    val doc2 = Map("OTP" -> "Otopeni", "SFO" -> "San Fran", "number" -> 2)

    val target = "spark-test/scala-dyn-id-write";

    val metadata1 = Map(ID -> 5, TTL -> "1d")
    val metadata2 = Map(ID -> 6, TTL -> "2d", VERSION -> "23")

    assertEquals(5, metadata1.getOrElse(ID, null))
    assertEquals(6, metadata2.getOrElse(ID, null))

    val pairRDD = sc.makeRDD(Seq((metadata1, doc1), (metadata2, doc2)))

    pairRDD.saveToEsWithMeta(target)

    assertTrue(RestUtils.exists(target + "/5"))
    assertTrue(RestUtils.exists(target + "/6"))

    assertThat(RestUtils.get(target + "/_search?"), containsString("SFO"))
  }

  @Test
  def testEsRDDWriteWithMappingExclude() {
    val trip1 = Map("reason" -> "business", "airport" -> "SFO")
    val trip2 = Map("participants" -> 5, "airport" -> "OTP")

    val target = "spark-test/scala-write-exclude";

    sc.makeRDD(Seq(trip1, trip2)).saveToEs(target, Map(ES_MAPPING_EXCLUDE -> "airport"))
    assertTrue(RestUtils.exists(target))
    assertThat(RestUtils.get(target + "/_search?"), containsString("business"))
    assertThat(RestUtils.get(target +  "/_search?"), containsString("participants"))
    assertThat(RestUtils.get(target +  "/_search?"), not(containsString("airport")))
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

    assertTrue(messages.count() == 2)
    assertNotNull(messages.take(10))
    assertNotNull(messages)
  }

  @Test
  def testEsRDDReadQuery() {
    val target = "spark-test/scala-basic-query-read"
    RestUtils.touch("spark-test")
    RestUtils.putData(target, "{\"message\" : \"Hello World\",\"message_date\" : \"2014-05-25\"}".getBytes())
    RestUtils.putData(target, "{\"message\" : \"Goodbye World\",\"message_date\" : \"2014-05-25\"}".getBytes())
    RestUtils.refresh("spark-test");

    val queryTarget = "*/scala-basic-query-read"
    val esData = EsSpark.esRDD(sc, queryTarget, "?q=message:Hello World")
    val newData = EsSpark.esRDD(sc, Map(
      ES_RESOURCE -> queryTarget,
      ES_INPUT_JSON -> "true",
      ES_QUERY -> "?q=message:Hello World"));

    assertTrue(esData.count() == 2)
    assertTrue(newData.count() == 2)
    assertNotNull(esData.take(10))
    assertNotNull(newData.take(10))
    assertNotNull(esData)
  }

  @Test
  def testEsRDDReadAsJson() {
    val target = "spark-test/scala-basic-json-read"
    RestUtils.touch("spark-test")
    RestUtils.putData(target, "{\"message\" : \"Hello World\",\"message_date\" : \"2014-05-25\"}".getBytes())
    RestUtils.putData(target, "{\"message\" : \"Goodbye World\",\"message_date\" : \"2014-05-25\"}".getBytes())
    RestUtils.refresh("spark-test");

    val esData = EsSpark.esJsonRDD(sc, target)
    val messages = esData.filter(doc => doc._2.contains("message"))

    assertTrue(messages.count() == 2)
    assertNotNull(messages.take(10))
    assertNotNull(messages)
  }

  @Test
  def testIndexAlias() {
    val doc = """
        | { "number" : 1, "list" : [ "an array", "some value"], "song" : "Golden Eyes" }
        """.stripMargin
    val indexA = "spark-alias-indexa/type"
    val indexB = "spark-alias-indexb/type"
    val alias = "spark-alias-alias"

    RestUtils.putData(indexA + "/1", doc.getBytes())
    RestUtils.putData(indexB + "/1", doc.getBytes())

    val aliases = """
        |{"actions" : [
          | {"add":{"index":"spark-alias-indexa","alias":"spark-alias-alias"}},
          | {"add":{"index":"spark-alias-indexb","alias":"spark-alias-alias"}}
        |]}
        """.stripMargin
    RestUtils.putData("_aliases", aliases.getBytes());
    RestUtils.refresh(alias)

    val aliasRDD = EsSpark.esJsonRDD(sc, alias + "/type")
    assertEquals(2, aliasRDD.count())
  }

  //@Test
  def testLoadJsonFile() {
    val target = "lost/id"

    val createIndex = """
      |{"settings" : {
        |"index" : {
        |    "number_of_shards" : 10,
        |    "number_of_replicas" : 1
      	|}
      |}
      |}""".stripMargin
    RestUtils.putData("lost", createIndex.getBytes());

    val rdd = sc.textFile("some.json")
    EsSpark.saveJsonToEs(rdd, target, Map(
      ES_MAPPING_ID -> "id"))
    val esRDD = EsSpark.esRDD(sc, target);
    println(esRDD.count)
  }
}