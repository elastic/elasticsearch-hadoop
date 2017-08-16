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

import java.awt.Polygon
import java.{lang => jl}
import java.{util => ju}
import java.util.concurrent.TimeUnit

import scala.collection.JavaConversions.propertiesAsScalaMap
import scala.collection.JavaConverters.asScalaBufferConverter
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkException
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_INDEX_READ_MISSING_AS_EMPTY
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_INDEX_AUTO_CREATE
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_INPUT_JSON
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_MAPPING_EXCLUDE
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_MAPPING_ID
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_MAPPING_JOIN
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_MAPPING_TIMESTAMP
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_QUERY
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_READ_METADATA
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_RESOURCE
import org.elasticsearch.hadoop.mr.RestUtils
import org.elasticsearch.hadoop.util.{EsMajorVersion, StringUtils, TestSettings, TestUtils}
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark.rdd.Metadata.ID
import org.elasticsearch.spark.rdd.Metadata.TTL
import org.elasticsearch.spark.rdd.Metadata.VERSION
import org.elasticsearch.spark.serialization.Bean
import org.elasticsearch.spark.serialization.ReflectionUtils
import org.elasticsearch.spark.sparkByteArrayJsonRDDFunctions
import org.elasticsearch.spark.sparkContextFunctions
import org.elasticsearch.spark.sparkPairRDDFunctions
import org.elasticsearch.spark.sparkRDDFunctions
import org.elasticsearch.spark.sparkStringJsonRDDFunctions
import org.hamcrest.Matchers.both
import org.hamcrest.Matchers.containsString
import org.hamcrest.Matchers.not
import org.junit.{AfterClass, Assume, BeforeClass, Test}
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertThat
import org.junit.Assert.assertTrue
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException
import java.nio.file.Paths
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.net.URI

import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.hadoop.mr.RestUtils.ExtendedRestClient
object AbstractScalaEsScalaSpark {
  @transient val conf = new SparkConf()
              .setAppName("estest")
              .setAll(propertiesAsScalaMap(TestSettings.TESTING_PROPS));
  @transient var cfg: SparkConf = null
  @transient var sc: SparkContext = null

  @BeforeClass
  def setup() {
    conf.setAll(TestSettings.TESTING_PROPS);
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

  @Parameters
  def testParams(): ju.Collection[Array[jl.Object]] = {
    val list = new ju.ArrayList[Array[jl.Object]]()
    list.add(Array("default-", jl.Boolean.FALSE))
    list.add(Array("with-meta-", jl.Boolean.TRUE))
    list
  }

  case class ModuleCaseClass(id: Integer, departure: String, var arrival: String) {
    var l = math.Pi
  }
}

case class Trip(departure: String, arrival: String) {
  var extra = math.Pi
}

@RunWith(classOf[Parameterized])
class AbstractScalaEsScalaSpark(prefix: String, readMetadata: jl.Boolean) extends Serializable {

  val sc = AbstractScalaEsScalaSpark.sc
  val cfg = Map(ES_READ_METADATA -> readMetadata.toString())

  private def readAsRDD(uri: URI) = {
    // don't use the sc.read.json/textFile to avoid the whole Hadoop madness
    val path = Paths.get(uri)
    // because Windows
    val lines = Files.readAllLines(path, StandardCharsets.ISO_8859_1).asScala
    sc.parallelize(lines)
  }
  
  @Test
  def testBasicRead() {
    val input = TestUtils.sampleArtistsDatUri()
    val data = readAsRDD(input).cache();

    assertTrue(data.count > 300)
  }

  @Test
  def testRDDEmptyRead() {
    val target = wrapIndex("spark-test/empty-rdd")
    sc.emptyRDD.saveToEs(target, cfg)
  }

  @Test(expected=classOf[EsHadoopIllegalArgumentException])
  def testEsRDDWriteIndexCreationDisabled() {
    val doc1 = Map("one" -> null, "two" -> Set("2"), "three" -> (".", "..", "..."))
    val doc2 = Map("OTP" -> "Otopeni", "SFO" -> "San Fran")

    val target = wrapIndex("spark-test-nonexisting/scala-basic-write")

    sc.makeRDD(Seq(doc1, doc2)).saveToEs(target, collection.mutable.Map(cfg.toSeq: _*) += (
      ES_INDEX_AUTO_CREATE -> "no"))
    assertTrue(!RestUtils.exists(target))
  }
    
  @Test
  def testEsRDDWrite() {
    val doc1 = Map("one" -> null, "two" -> Set("2"), "three" -> (".", "..", "..."))
    val doc2 = Map("OTP" -> "Otopeni", "SFO" -> "San Fran")

    val target = wrapIndex("spark-test/scala-basic-write")

    sc.makeRDD(Seq(doc1, doc2)).saveToEs(target, cfg)
    assertTrue(RestUtils.exists(target))
    assertThat(RestUtils.get(target + "/_search?"), containsString(""))
  }

  @Test(expected = classOf[SparkException])
  def testNestedUnknownCharacter() {
    val doc = Map("itemId" -> "1", "map" -> Map("lat" -> 1.23, "lon" -> -70.12), "list" -> ("A", "B", "C"), "unknown" -> new Polygon())
    sc.makeRDD(Seq(doc)).saveToEs(wrapIndex("spark-test/nested-map"), cfg)
  }

  @Test
  def testEsRDDWriteCaseClass() {
    val javaBean = new Bean("bar", 1, true)
    val caseClass1 = Trip("OTP", "SFO")
    val caseClass2 = AbstractScalaEsScalaSpark.ModuleCaseClass(1, "OTP", "MUC")

    val vals = ReflectionUtils.caseClassValues(caseClass2)

     val target = wrapIndex("spark-test/scala-basic-write-objects")

    sc.makeRDD(Seq(javaBean, caseClass1)).saveToEs(target, cfg)
    sc.makeRDD(Seq(javaBean, caseClass2)).saveToEs(target, Map("es.mapping.id"->"id"))

    assertTrue(RestUtils.exists(target))
    assertEquals(3, EsSpark.esRDD(sc, target).count());
    assertThat(RestUtils.get(target + "/_search?"), containsString(""))
  }

  @Test
  def testEsRDDWriteWithMappingId() {
    val doc1 = Map("one" -> null, "two" -> Set("2"), "three" -> (".", "..", "..."), "number" -> 1)
    val doc2 = Map("OTP" -> "Otopeni", "SFO" -> "San Fran", "number" -> 2)

    val target = "spark-test/scala-id-write"

    sc.makeRDD(Seq(doc1, doc2)).saveToEs(target, Map(ES_MAPPING_ID -> "number"))

    assertEquals(2, EsSpark.esRDD(sc, target).count());
    assertTrue(RestUtils.exists(target + "/1"))
    assertTrue(RestUtils.exists(target + "/2"))

    assertThat(RestUtils.get(target + "/_search?"), containsString("SFO"))
  }
  
  //@Test - disabled since alpha4
  def testEsRDDWriteWithMappingTimestamp() {
    val mapping = """{ "scala-timestamp-write": {
      | "_timestamp" : {
      |   "enabled":true
      | }
      |}
      }""".stripMargin

    val index = "spark-test"
    val target = s"$index/scala-timestamp-write"
    RestUtils.touch(index)
    RestUtils.putMapping(target, mapping.getBytes(StringUtils.UTF_8))
    
    
    val doc1 = Map("one" -> null, "two" -> Set("2"), "number" -> 1, "date" -> "2016-05-18T16:39:39.317Z")
    val doc2 = Map("OTP" -> "Otopeni", "SFO" -> "San Fran", "number" -> 2, "date" -> "2016-03-18T10:11:28.123Z")

    sc.makeRDD(Seq(doc1, doc2)).saveToEs(target, Map(ES_MAPPING_ID -> "number", ES_MAPPING_TIMESTAMP -> "date", ES_MAPPING_EXCLUDE -> "date"))

    assertEquals(2, EsSpark.esRDD(sc, target).count());
    assertTrue(RestUtils.exists(target + "/1"))
    assertTrue(RestUtils.exists(target + "/2"))

    val search = RestUtils.get(target + "/_search?")
    assertThat(search, containsString("SFO"))
    assertThat(search, not(containsString("date")))
    assertThat(search, containsString("_timestamp"))
  }

  @Test
  def testEsRDDWriteWithDynamicMapping() {
    val doc1 = Map("one" -> null, "two" -> Set("2"), "three" -> (".", "..", "..."), "number" -> 1)
    val doc2 = Map("OTP" -> "Otopeni", "SFO" -> "San Fran", "number" -> 2)

    val target = wrapIndex("spark-test/scala-dyn-id-write")

    val pairRDD = sc.makeRDD(Seq((3, doc1), (4, doc2))).saveToEsWithMeta(target, cfg)

    assertEquals(2, EsSpark.esRDD(sc, target).count());
    assertTrue(RestUtils.exists(target + "/3"))
    assertTrue(RestUtils.exists(target + "/4"))

    assertThat(RestUtils.get(target + "/_search?"), containsString("SFO"))
  }

  @Test
  def testEsRDDWriteWithDynamicMapMapping() {
    val doc1 = Map("one" -> null, "two" -> Set("2"), "three" -> (".", "..", "..."), "number" -> 1)
    val doc2 = Map("OTP" -> "Otopeni", "SFO" -> "San Fran", "number" -> 2)

    val target = wrapIndex("spark-test/scala-dyn-id-write")

    val metadata1 = Map(ID -> 5, TTL -> "1d")
    val metadata2 = Map(ID -> 6, TTL -> "2d", VERSION -> "23")

    assertEquals(5, metadata1.getOrElse(ID, null))
    assertEquals(6, metadata2.getOrElse(ID, null))

    val pairRDD = sc.makeRDD(Seq((metadata1, doc1), (metadata2, doc2)))

    pairRDD.saveToEsWithMeta(target, cfg)

    assertTrue(RestUtils.exists(target + "/5"))
    assertTrue(RestUtils.exists(target + "/6"))

    assertThat(RestUtils.get(target + "/_search?"), containsString("SFO"))
  }

  @Test
  def testEsRDDWriteWithMappingExclude() {
    val trip1 = Map("reason" -> "business", "airport" -> "SFO")
    val trip2 = Map("participants" -> 5, "airport" -> "OTP")

    val target = wrapIndex("spark-test/scala-write-exclude")

    sc.makeRDD(Seq(trip1, trip2)).saveToEs(target, Map(ES_MAPPING_EXCLUDE -> "airport"))
    assertTrue(RestUtils.exists(target))
    assertThat(RestUtils.get(target + "/_search?"), containsString("business"))
    assertThat(RestUtils.get(target +  "/_search?"), containsString("participants"))
    assertThat(RestUtils.get(target +  "/_search?"), not(containsString("airport")))
  }

  @Test
  def testEsRDDWriteJoinField(): Unit = {
    // test mix of short-form and long-form joiner values
    val company1 = Map("id" -> "1", "company" -> "Elastic", "joiner" -> "company")
    val company2 = Map("id" -> "2", "company" -> "Fringe Cafe", "joiner" -> Map("name" -> "company"))
    val company3 = Map("id" -> "3", "company" -> "WATIcorp", "joiner" -> Map("name" -> "company"))

    val employee1 = Map("id" -> "10", "name" -> "kimchy", "joiner" -> Map("name" -> "employee", "parent" -> "1"))
    val employee2 = Map("id" -> "20", "name" -> "April Ryan", "joiner" -> Map("name" -> "employee", "parent" -> "2"))
    val employee3 = Map("id" -> "21", "name" -> "Charlie", "joiner" -> Map("name" -> "employee", "parent" -> "2"))
    val employee4 = Map("id" -> "30", "name" -> "Alvin Peats", "joiner" -> Map("name" -> "employee", "parent" -> "3"))

    val parents = Seq(company1, company2, company3)
    val children = Seq(employee1, employee2, employee3, employee4)
    val docs = parents ++ children

    {
      val index = wrapIndex("spark-test-scala-write-join-separate")
      val typename = "join"
      val target = s"$index/$typename"
      RestUtils.put(index, """{"settings":{"index.mapping.single_type":"true"}}""".getBytes())
      RestUtils.putMapping(index, typename, "data/join/mapping.json")

      sc.makeRDD(parents).saveToEs(target, Map(ES_MAPPING_ID -> "id", ES_MAPPING_JOIN -> "joiner"))
      sc.makeRDD(children).saveToEs(target, Map(ES_MAPPING_ID -> "id", ES_MAPPING_JOIN -> "joiner"))

      assertThat(RestUtils.get(target + "/10?routing=1"), containsString("kimchy"))
      assertThat(RestUtils.get(target + "/10?routing=1"), containsString(""""_routing":"1""""))

      val data = sc.esRDD(target).collectAsMap()

      {
        assertTrue(data.contains("1"))
        val record10 = data("1")
        assertTrue(record10.contains("joiner"))
        val joiner = record10("joiner").asInstanceOf[collection.mutable.Map[String, Object]]
        assertTrue(joiner.contains("name"))
      }

      {
        assertTrue(data.contains("10"))
        val record10 = data("10")
        assertTrue(record10.contains("joiner"))
        val joiner = record10("joiner").asInstanceOf[collection.mutable.Map[String, Object]]
        assertTrue(joiner.contains("name"))
        assertTrue(joiner.contains("parent"))
      }

    }
    {
      val index = wrapIndex("spark-test-scala-write-join-combined")
      val typename = "join"
      val target = s"$index/$typename"
      RestUtils.put(index, """{"settings":{"index.mapping.single_type":"true"}}""".getBytes())
      RestUtils.putMapping(index, typename, "data/join/mapping.json")

      sc.makeRDD(docs).saveToEs(target, Map(ES_MAPPING_ID -> "id", ES_MAPPING_JOIN -> "joiner"))

      assertThat(RestUtils.get(target + "/10?routing=1"), containsString("kimchy"))
      assertThat(RestUtils.get(target + "/10?routing=1"), containsString(""""_routing":"1""""))

      val data = sc.esRDD(target).collectAsMap()

      {
        assertTrue(data.contains("1"))
        val record10 = data("1")
        assertTrue(record10.contains("joiner"))
        val joiner = record10("joiner").asInstanceOf[collection.mutable.Map[String, Object]]
        assertTrue(joiner.contains("name"))
      }

      {
        assertTrue(data.contains("10"))
        val record10 = data("10")
        assertTrue(record10.contains("joiner"))
        val joiner = record10("joiner").asInstanceOf[collection.mutable.Map[String, Object]]
        assertTrue(joiner.contains("name"))
        assertTrue(joiner.contains("parent"))
      }
    }
  }

  @Test
  def testEsRDDIngest() {
    try {
      val versionTestingClient: RestUtils.ExtendedRestClient = new RestUtils.ExtendedRestClient
      try {
        val esMajorVersion: EsMajorVersion = versionTestingClient.remoteEsVersion
        Assume.assumeTrue("Ingest Supported in 5.x and above only", esMajorVersion.onOrAfter(EsMajorVersion.V_5_X))
      } finally {
        if (versionTestingClient != null) versionTestingClient.close()
      }
    }

    val client: RestUtils.ExtendedRestClient = new RestUtils.ExtendedRestClient
    val prefix: String = "spark"
    val pipeline: String = "{\"description\":\"Test Pipeline\",\"processors\":[{\"set\":{\"field\":\"pipeTEST\",\"value\":true,\"override\":true}}]}"
    client.put("/_ingest/pipeline/" + prefix + "-pipeline", StringUtils.toUTF(pipeline))
    client.close();

    val doc1 = Map("one" -> null, "two" -> Set("2"), "three" -> (".", "..", "..."))
    val doc2 = Map("OTP" -> "Otopeni", "SFO" -> "San Fran")

    val target = wrapIndex("spark-test/scala-ingest-write")

    val ingestCfg = cfg + (ConfigurationOptions.ES_INGEST_PIPELINE -> "spark-pipeline") + (ConfigurationOptions.ES_NODES_INGEST_ONLY -> "true")

    sc.makeRDD(Seq(doc1, doc2)).saveToEs(target, ingestCfg)
    assertTrue(RestUtils.exists(target))
    assertThat(RestUtils.get(target + "/_search?"), containsString(""))
    assertThat(RestUtils.get(target + "/_search?"), containsString("\"pipeTEST\":true"))
  }


  @Test
  def testEsMultiIndexRDDWrite() {
    val trip1 = Map("reason" -> "business", "airport" -> "SFO")
    val trip2 = Map("participants" -> 5, "airport" -> "OTP")

    val target = wrapIndex("spark-test/trip-{airport}")
    sc.makeRDD(Seq(trip1, trip2)).saveToEs(target, cfg)
    assertTrue(RestUtils.exists(wrapIndex("spark-test/trip-OTP")))
    assertTrue(RestUtils.exists(wrapIndex("spark-test/trip-SFO")))

    assertThat(RestUtils.get(wrapIndex("spark-test/trip-SFO/_search?")), containsString("business"))
    assertThat(RestUtils.get(wrapIndex("spark-test/trip-OTP/_search?")), containsString("participants"))
  }

  @Test
  def testEsWriteAsJsonMultiWrite() {
    val json1 = "{\"reason\" : \"business\",\"airport\" : \"SFO\"}";
    val json2 = "{\"participants\" : 5,\"airport\" : \"OTP\"}"

    sc.makeRDD(Seq(json1, json2)).saveJsonToEs(wrapIndex("spark-test/json-{airport}"), cfg)

    val json1BA = json1.getBytes()
    val json2BA = json2.getBytes()

    sc.makeRDD(Seq(json1BA, json2BA)).saveJsonToEs(wrapIndex("spark-test/json-ba-{airport}"), cfg)

    assertTrue(RestUtils.exists(wrapIndex("spark-test/json-SFO")))
    assertTrue(RestUtils.exists(wrapIndex("spark-test/json-OTP")))

    assertTrue(RestUtils.exists(wrapIndex("spark-test/json-ba-SFO")))
    assertTrue(RestUtils.exists(wrapIndex("spark-test/json-ba-OTP")))

    assertThat(RestUtils.get(wrapIndex("spark-test/json-SFO/_search?")), containsString("business"))
    assertThat(RestUtils.get(wrapIndex("spark-test/json-OTP/_search?")), containsString("participants"))
  }

  @Test
  def testEsRDDWriteWithUpsertScriptUsingBothObjectAndRegularString() {
    val mapping = """{
                    |  "contact": {
                    |    "properties": {
                    |      "id": {
                    |        "type": "string"
                    |      },
                    |      "note": {
                    |        "type": "string",
                    |        "index": "not_analyzed"
                    |      },
                    |      "address": {
                    |        "type": "nested",
                    |        "properties": {
                    |          "id":    { "type": "string"  },
                    |          "zipcode": { "type": "string"  }
                    |        }
                    |      }
                    |    }
                    |  }
                    |}""".stripMargin

    val index = "spark-test"
    val target = s"$index/contact"
    RestUtils.touch(index)
    RestUtils.putMapping(target, mapping.getBytes(StringUtils.UTF_8))
    RestUtils.postData(s"$target/1", """{ "id" : "1", "note": "First", "address": [] }""".getBytes(StringUtils.UTF_8))
    RestUtils.postData(s"$target/2", """{ "id" : "2", "note": "First", "address": [] }""".getBytes(StringUtils.UTF_8))

    val props = Map("es.write.operation" -> "upsert",
      "es.input.json" -> "true",
      "es.mapping.id" -> "id",
      "es.update.script.lang" -> "groovy"
    )

    // Upsert a value that should only modify the first document. Modification will add an address entry.
    val lines = sc.makeRDD(List("""{"id":"1","address":{"zipcode":"12345","id":"1"}}"""))
    val up_params = "new_address:address"
    val up_script = "ctx._source.address+=new_address"
    lines.saveToEs(target, props + ("es.update.script.params" -> up_params) + ("es.update.script" -> up_script))

    // Upsert a value that should only modify the second document. Modification will update the "note" field.
    val notes = sc.makeRDD(List("""{"id":"2","note":"Second"}"""))
    val note_up_params = "new_note:note"
    val note_up_script = "ctx._source.note=new_note"
    notes.saveToEs(target, props + ("es.update.script.params" -> note_up_params) + ("es.update.script" -> note_up_script))

    assertTrue(RestUtils.exists(s"$target/1"))
    assertThat(RestUtils.get(s"$target/1"), both(containsString(""""zipcode":"12345"""")).and(containsString(""""note":"First"""")))

    assertTrue(RestUtils.exists(s"$target/2"))
    assertThat(RestUtils.get(s"$target/2"), both(not(containsString(""""zipcode":"12345""""))).and(containsString(""""note":"Second"""")))
  }

  @Test
  def testEsRDDRead() {
    val target = wrapIndex("spark-test/scala-basic-read")
    RestUtils.touch(wrapIndex("spark-test"))
    RestUtils.postData(target, "{\"message\" : \"Hello World\",\"message_date\" : \"2014-05-25\"}".getBytes())
    RestUtils.postData(target, "{\"message\" : \"Goodbye World\",\"message_date\" : \"2014-05-25\"}".getBytes())
    RestUtils.refresh(wrapIndex("spark-test"))

    val esData = EsSpark.esRDD(sc, target, cfg)
    val messages = esData.filter(doc => doc._2.find(_.toString.contains("message")).nonEmpty)

    assertTrue(messages.count() == 2)
    assertNotNull(messages.take(10))
    assertNotNull(messages)
  }

  @Test
  def testEsRDDReadQuery() {
    val target = "spark-test/scala-basic-query-read"
    RestUtils.touch("spark-test")
    RestUtils.postData(target, "{\"message\" : \"Hello World\",\"message_date\" : \"2014-05-25\"}".getBytes())
    RestUtils.postData(target, "{\"message\" : \"Goodbye World\",\"message_date\" : \"2014-05-25\"}".getBytes())
    RestUtils.refresh("spark-test");

    val queryTarget = "*/scala-basic-query-read"
    val esData = EsSpark.esRDD(sc, queryTarget, "?q=message:Hello World", cfg)
    val newData = EsSpark.esRDD(sc, collection.mutable.Map(cfg.toSeq: _*) += (
      ES_RESOURCE -> queryTarget,
      ES_INPUT_JSON -> "true",
      ES_QUERY -> "?q=message:Hello World"))

    // on each run, 2 docs are added
    assertTrue(esData.count() % 2 == 0)
    assertTrue(newData.count() % 2 == 0)

    assertNotNull(esData.take(10))
    assertNotNull(newData.take(10))
    assertNotNull(esData)
  }

  @Test
  def testEsRDDReadAsJson() {
    val target = wrapIndex("spark-test/scala-basic-json-read")
    RestUtils.touch(wrapIndex("spark-test"))
    RestUtils.postData(target, "{\"message\" : \"Hello World\",\"message_date\" : \"2014-05-25\"}".getBytes())
    RestUtils.postData(target, "{\"message\" : \"Goodbye World\",\"message_date\" : \"2014-05-25\"}".getBytes())
    RestUtils.refresh(wrapIndex("spark-test"))

    val esData = EsSpark.esJsonRDD(sc, target, cfg)
    val messages = esData.filter(doc => doc._2.contains("message"))

    assertTrue(messages.count() == 2)
    assertNotNull(messages.take(10))
    assertNotNull(messages)
  }

  @Test
  def testEsRDDReadWithSourceFilter() {
    val target = wrapIndex("spark-test/scala-source-filter-read")
    RestUtils.touch(wrapIndex("spark-test"))
    RestUtils.postData(target, "{\"message\" : \"Hello World\",\"message_date\" : \"2014-05-25\"}".getBytes())
    RestUtils.postData(target, "{\"message\" : \"Goodbye World\",\"message_date\" : \"2014-05-25\"}".getBytes())
    RestUtils.refresh(wrapIndex("spark-test"))

    val testCfg = cfg + (ConfigurationOptions.ES_READ_SOURCE_FILTER -> "message_date")

    val esData = EsSpark.esRDD(sc, target, testCfg)
    val messages = esData.filter(doc => doc._2.contains("message_date"))

    assertTrue(messages.count() == 2)
    assertNotNull(messages.take(10))
    assertNotNull(messages)
  }

  @Test
  def testIndexAlias() {
    val doc = """
        | { "number" : 1, "list" : [ "an array", "some value"], "song" : "Golden Eyes" }
        """.stripMargin
    val indexA = wrapIndex("spark-alias-indexa/type")
    val indexB = wrapIndex("spark-alias-indexb/type")
    val alias = wrapIndex("spark-alias-alias")

    RestUtils.postData(indexA + "/1", doc.getBytes())
    RestUtils.postData(indexB + "/1", doc.getBytes())

    val aliases = """
        |{"actions" : [
          | {"add":{"index":"""".stripMargin + wrapIndex("spark-alias-indexa") + """" ,"alias": """" + wrapIndex("spark-alias-alias") + """"}},
          | {"add":{"index":"""".stripMargin + wrapIndex("spark-alias-indexb") + """" ,"alias": """" + wrapIndex("spark-alias-alias") + """"}}
        |]}
        """.stripMargin

    println(aliases)
    RestUtils.postData("_aliases", aliases.getBytes());
    RestUtils.refresh(alias)

    val aliasRDD = EsSpark.esJsonRDD(sc, alias + "/type", cfg)
    assertEquals(2, aliasRDD.count())
  }

  @Test
  def testNullAsEmpty() {
    val data = Seq(
      Map("field1" -> 5.4, "field2" -> "foo"),
      Map("field2" -> "bar"),
      Map("field1" -> 0.0, "field2" -> "baz")
    )
    val target = wrapIndex("spark-test/nullasempty")
    sc.makeRDD(data).saveToEs(target)

    assertEquals(3, EsSpark.esRDD(sc, target, cfg).count())
  }

  @Test
  def testNewIndexWithTemplate() {
    val target = wrapIndex("spark-template-index/alias")

    val template = """
      |{"template" : """".stripMargin + "spark-template-*" + """",
        |"settings" : {
        |    "number_of_shards" : 1,
        |    "number_of_replicas" : 0
        |},
        |"mappings" : {
        |  "alias" : {
        |    "properties" : {
        |      "name" : { "type" : "string" },
        |      "number" : { "type" : "long" },
        |      "@ImportDate" : { "type" : "date" }
        |     }
        |   }
        | },
        |"aliases" : { "spark-temp-index" : {} }
      |}""".stripMargin
    RestUtils.put("_template/" + wrapIndex("test_template"), template.getBytes)

    val rdd = readAsRDD(TestUtils.sampleArtistsJsonUri())
    EsSpark.saveJsonToEs(rdd, target)
    val esRDD = EsSpark.esRDD(sc, target, cfg)
    println(esRDD.count)
    println(RestUtils.getMapping(target))

    val erc = new ExtendedRestClient()
    erc.delete("_template/" + wrapIndex("test_template"))
    erc.close()
  }

  
  @Test
  def testEsSparkVsScCount() {
    val target = wrapIndex("spark-test/check-counting")
    val rawCore = List( Map("colint" -> 1, "colstr" -> "s"),
                         Map("colint" -> null, "colstr" -> null) )
    sc.parallelize(rawCore, 1).saveToEs(target)
    val qjson =
      """{"query":{"range":{"colint":{"from":null,"to":"9","include_lower":true,"include_upper":true}}}}"""
    
    val esRDD = EsSpark.esRDD(sc, target, qjson)
    val scRDD = sc.esRDD(target, qjson)
    assertEquals(esRDD.collect().size, scRDD.collect().size)
  }

  
  @Test
  def testMultiIndexNonExisting() {
    val rdd = EsSpark.esJsonRDD(sc, "bumpA,Stump", Map(ES_INDEX_READ_MISSING_AS_EMPTY -> "yes"))
    assertEquals(0, rdd.count)
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
    RestUtils.postData("lost", createIndex.getBytes());

    val rdd = readAsRDD(getClass().getResource("some.json").toURI())
    EsSpark.saveJsonToEs(rdd, target, collection.mutable.Map(cfg.toSeq: _*) += (
      ES_MAPPING_ID -> "id"))
    val esRDD = EsSpark.esRDD(sc, target, cfg)
    println(esRDD.count)
  }

  def wrapIndex(index: String) = {
    prefix + index
  }
}