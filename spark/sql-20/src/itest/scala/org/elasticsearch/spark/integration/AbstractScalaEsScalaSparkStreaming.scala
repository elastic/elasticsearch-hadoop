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
package org.elasticsearch.spark.integration

import java.util.concurrent.TimeUnit
import java.{lang => jl, util => ju}

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.scheduler.{StreamingListenerOutputOperationCompleted, _}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}
import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
import org.elasticsearch.hadoop.mr.RestUtils
import org.elasticsearch.hadoop.util.TestUtils
import org.elasticsearch.hadoop.util.{EsMajorVersion, StringUtils, TestSettings}
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark.rdd.Metadata._
import org.elasticsearch.spark.serialization.{Bean, ReflectionUtils}
import org.elasticsearch.spark.streaming._
import org.hamcrest.Matchers._
import org.junit.Assert._
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized.Parameters
import org.junit.runners.{MethodSorters, Parameterized}

import scala.collection.JavaConversions.propertiesAsScalaMap
import scala.collection.mutable
import scala.reflect.ClassTag

object AbstractScalaEsScalaSparkStreaming {
  @transient val conf = new SparkConf()
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .setMaster("local")
    .setAppName("estest")
    .set("spark.executor.extraJavaOptions", "-XX:MaxPermSize=256m")
    .setJars(SparkUtils.ES_SPARK_TESTING_JAR)
  @transient var sc: SparkContext = null
  @transient var ssc: StreamingContext = null

  @BeforeClass
  def setup(): Unit =  {
    conf.setAll(TestSettings.TESTING_PROPS)
    sc = new SparkContext(conf)
  }

  @AfterClass
  def cleanup(): Unit =  {
    if (sc != null) {
      sc.stop
      // give jetty time to clean its act up
      Thread.sleep(TimeUnit.SECONDS.toMillis(3))
    }
  }

  @Parameters
  def testParams(): ju.Collection[Array[jl.Object]] = {
    val list = new ju.ArrayList[Array[jl.Object]]()
    list.add(Array("stream-default", jl.Boolean.FALSE))
    list
  }
}

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(classOf[Parameterized])
class AbstractScalaEsScalaSparkStreaming(val prefix: String, readMetadata: jl.Boolean) extends Serializable {

  val sc = AbstractScalaEsScalaSparkStreaming.sc
  val cfg = Map(ConfigurationOptions.ES_READ_METADATA -> readMetadata.toString)
  val version = TestUtils.getEsVersion
  val keyword = if (version.onOrAfter(EsMajorVersion.V_5_X)) "keyword" else "string"

  var ssc: StreamingContext = _

  @Before
  def createStreamingContext(): Unit = {
    ssc = new StreamingContext(sc, Seconds(1))
  }

  @After
  def tearDownStreamingContext(): Unit = {
    if (ssc.getState() != StreamingContextState.STOPPED) {
      ssc.stop(stopSparkContext = false, stopGracefully = true)
    }
  }

  @Test
  def testEsRDDWriteIndexCreationDisabled(): Unit = {
    val expecting = ExpectingToThrow(classOf[EsHadoopIllegalArgumentException]).from(ssc)

    val doc1 = Map("one" -> null, "two" -> Set("2"), "three" -> (".", "..", "..."))
    val doc2 = Map("OTP" -> "Otopeni", "SFO" -> "San Fran")

    val target = wrapIndex("spark-test-nonexisting/scala-basic-write")

    val batch = sc.makeRDD(Seq(doc1, doc2))
    runStream(batch)(_.saveToEs(target, cfg + (ES_INDEX_AUTO_CREATE -> "no")))

    assertTrue(!RestUtils.exists(target))
    expecting.assertExceptionFound()
  }

  @Test
  def testEsDataFrame1Write(): Unit = {
    val doc1 = Map("one" -> null, "two" -> Set("2"), "three" ->(".", "..", "..."))
    val doc2 = Map("OTP" -> "Otopeni", "SFO" -> "San Fran")

    val target = wrapIndex("spark-streaming-test-scala-basic-write/data")

    val batch = sc.makeRDD(Seq(doc1, doc2))

    runStream(batch)(_.saveToEs(target, cfg))

    assertTrue(RestUtils.exists(target))
    assertThat(RestUtils.get(target + "/_search?"), containsString("OTP"))
    assertThat(RestUtils.get(target + "/_search?"), containsString("two"))
  }

  @Test
  def testNestedUnknownCharacter(): Unit = {
    val expected = ExpectingToThrow(classOf[SparkException]).from(ssc)
    val doc = Map("itemId" -> "1", "map" -> Map("lat" -> 1.23, "lon" -> -70.12), "list" -> ("A", "B", "C"), "unknown" -> new Garbage(0))
    val batch = sc.makeRDD(Seq(doc))
    runStream(batch)(_.saveToEs(wrapIndex("spark-streaming-test-nested-map/data"), cfg))
    expected.assertExceptionFound()
  }

  @Test
  def testEsRDDWriteCaseClass(): Unit = {
    val javaBean = new Bean("bar", 1, true)
    val caseClass1 = Trip("OTP", "SFO")
    val caseClass2 = AbstractScalaEsScalaSpark.ModuleCaseClass(1, "OTP", "MUC")

    val vals = ReflectionUtils.caseClassValues(caseClass2)

    val target = wrapIndex("spark-streaming-test-scala-basic-write-objects/data")

    val batch = sc.makeRDD(Seq(javaBean, caseClass1))
    runStreamRecoverably(batch)(_.saveToEs(target, cfg))

    val batch2 = sc.makeRDD(Seq(javaBean, caseClass2))
    runStream(batch2)(_.saveToEs(target, Map("es.mapping.id"->"id")))

    assertTrue(RestUtils.exists(target))
    assertEquals(3, EsSpark.esRDD(sc, target).count())
    assertThat(RestUtils.get(target + "/_search?"), containsString(""))
  }

  @Test
  def testEsRDDWriteWithMappingId(): Unit = {
    val doc1 = Map("one" -> null, "two" -> Set("2"), "three" -> (".", "..", "..."), "number" -> 1)
    val doc2 = Map("OTP" -> "Otopeni", "SFO" -> "San Fran", "number" -> 2)

    val target = wrapIndex("spark-streaming-test-scala-id-write/data")

    val batch = sc.makeRDD(Seq(doc1, doc2))
    runStream(batch)(_.saveToEs(target, Map(ES_MAPPING_ID -> "number")))

    assertEquals(2, EsSpark.esRDD(sc, target).count())
    assertTrue(RestUtils.exists(target + "/1"))
    assertTrue(RestUtils.exists(target + "/2"))

    assertThat(RestUtils.get(target + "/_search?"), containsString("SFO"))
  }

  @Test
  def testEsRDDWriteWithDynamicMapping(): Unit = {
    val doc1 = Map("one" -> null, "two" -> Set("2"), "three" -> (".", "..", "..."), "number" -> 1)
    val doc2 = Map("OTP" -> "Otopeni", "SFO" -> "San Fran", "number" -> 2)

    val target = wrapIndex("spark-streaming-test-scala-dyn-id-write/data")

    val pairRDD = sc.makeRDD(Seq((3, doc1), (4, doc2)))
    runStream(pairRDD)(_.saveToEsWithMeta(target, cfg))

    println(RestUtils.get(target + "/_search?"))

    assertEquals(2, EsSpark.esRDD(sc, target).count())
    assertTrue(RestUtils.exists(target + "/3"))
    assertTrue(RestUtils.exists(target + "/4"))

    assertThat(RestUtils.get(target + "/_search?"), containsString("SFO"))
  }

  @Test
  def testEsRDDWriteWithDynamicMapMapping(): Unit = {
    val doc1 = Map("one" -> null, "two" -> Set("2"), "three" -> (".", "..", "..."), "number" -> 1)
    val doc2 = Map("OTP" -> "Otopeni", "SFO" -> "San Fran", "number" -> 2)

    val target = wrapIndex("spark-streaming-test-scala-dyn-id-write-map/data")

    val metadata1 = Map(ID -> 5)
    val metadata2 = Map(ID -> 6, VERSION -> "23")

    assertEquals(5, metadata1.getOrElse(ID, null))
    assertEquals(6, metadata2.getOrElse(ID, null))

    val pairRDD = sc.makeRDD(Seq((metadata1, doc1), (metadata2, doc2)))

    runStream(pairRDD)(_.saveToEsWithMeta(target, cfg))

    assertTrue(RestUtils.exists(target + "/5"))
    assertTrue(RestUtils.exists(target + "/6"))

    assertThat(RestUtils.get(target + "/_search?"), containsString("SFO"))
  }

  @Test
  def testEsRDDWriteWithMappingExclude(): Unit = {
    val trip1 = Map("reason" -> "business", "airport" -> "SFO")
    val trip2 = Map("participants" -> 5, "airport" -> "OTP")

    val target = wrapIndex("spark-streaming-test-scala-write-exclude/data")

    val batch = sc.makeRDD(Seq(trip1, trip2))
    runStream(batch)(_.saveToEs(target, Map(ES_MAPPING_EXCLUDE -> "airport")))

    assertTrue(RestUtils.exists(target))
    assertThat(RestUtils.get(target + "/_search?"), containsString("business"))
    assertThat(RestUtils.get(target +  "/_search?"), containsString("participants"))
    assertThat(RestUtils.get(target +  "/_search?"), not(containsString("airport")))
  }

  @Test
  def testEsRDDIngest(): Unit = {
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
    val pipelineName: String = prefix + "-pipeline"
    val pipeline: String = "{\"description\":\"Test Pipeline\",\"processors\":[{\"set\":{\"field\":\"pipeTEST\",\"value\":true,\"override\":true}}]}"
    client.put("/_ingest/pipeline/" + pipelineName, StringUtils.toUTF(pipeline))
    client.close()

    val doc1 = Map("one" -> null, "two" -> Set("2"), "three" -> (".", "..", "..."))
    val doc2 = Map("OTP" -> "Otopeni", "SFO" -> "San Fran")

    val target = wrapIndex("spark-streaming-test-scala-ingest-write/data")

    val ingestCfg = cfg + (ConfigurationOptions.ES_INGEST_PIPELINE -> pipelineName) + (ConfigurationOptions.ES_NODES_INGEST_ONLY -> "true")

    val batch = sc.makeRDD(Seq(doc1, doc2))
    runStream(batch)(_.saveToEs(target, ingestCfg))

    assertTrue(RestUtils.exists(target))
    assertThat(RestUtils.get(target + "/_search?"), containsString("\"pipeTEST\":true"))
  }


  @Test
  def testEsMultiIndexRDDWrite(): Unit = {
    val trip1 = Map("reason" -> "business", "airport" -> "sfo")
    val trip2 = Map("participants" -> 5, "airport" -> "otp")

    val target = wrapIndex("spark-streaming-test-trip-{airport}/data")
    val batch = sc.makeRDD(Seq(trip1, trip2))
    runStream(batch)(_.saveToEs(target, cfg))

    assertTrue(RestUtils.exists(wrapIndex("spark-streaming-test-trip-otp/data")))
    assertTrue(RestUtils.exists(wrapIndex("spark-streaming-test-trip-sfo/data")))

    assertThat(RestUtils.get(wrapIndex("spark-streaming-test-trip-sfo/data/_search?")), containsString("business"))
    assertThat(RestUtils.get(wrapIndex("spark-streaming-test-trip-otp/data/_search?")), containsString("participants"))
  }

  @Test
  def testEsWriteAsJsonMultiWrite(): Unit = {
    val json1 = "{\"reason\" : \"business\",\"airport\" : \"sfo\"}"
    val json2 = "{\"participants\" : 5,\"airport\" : \"otp\"}"

    val batch = sc.makeRDD(Seq(json1, json2))
    runStreamRecoverably(batch)(_.saveJsonToEs(wrapIndex("spark-streaming-test-json-{airport}/data"), cfg))

    val json1BA = json1.getBytes()
    val json2BA = json2.getBytes()

    val batch2 = sc.makeRDD(Seq(json1BA, json2BA))
    runStream(batch2)(_.saveJsonToEs(wrapIndex("spark-streaming-test-json-ba-{airport}/data"), cfg))

    assertTrue(RestUtils.exists(wrapIndex("spark-streaming-test-json-sfo/data")))
    assertTrue(RestUtils.exists(wrapIndex("spark-streaming-test-json-otp/data")))

    assertTrue(RestUtils.exists(wrapIndex("spark-streaming-test-json-ba-sfo/data")))
    assertTrue(RestUtils.exists(wrapIndex("spark-streaming-test-json-ba-otp/data")))

    assertThat(RestUtils.get(wrapIndex("spark-streaming-test-json-sfo/data/_search?")), containsString("business"))
    assertThat(RestUtils.get(wrapIndex("spark-streaming-test-json-otp/data/_search?")), containsString("participants"))
  }

  @Test
  def testEsRDDWriteWithUpsertScriptUsingBothObjectAndRegularString(): Unit = {
    val mapping = s"""{
                    |  "data": {
                    |    "properties": {
                    |      "id": {
                    |        "type": "$keyword"
                    |      },
                    |      "note": {
                    |        "type": "$keyword"
                    |      },
                    |      "address": {
                    |        "type": "nested",
                    |        "properties": {
                    |          "id":    { "type": "$keyword"  },
                    |          "zipcode": { "type": "$keyword"  }
                    |        }
                    |      }
                    |    }
                    |  }
                    |}""".stripMargin

    val index = "spark-streaming-test-contact"
    val typed = "data"
    val target = s"$index/$typed"
    RestUtils.touch(index)
    RestUtils.putMapping(index, typed, mapping.getBytes(StringUtils.UTF_8))
    RestUtils.postData(s"$target/1", """{ "id" : "1", "note": "First", "address": [] }""".getBytes(StringUtils.UTF_8))
    RestUtils.postData(s"$target/2", """{ "id" : "2", "note": "First", "address": [] }""".getBytes(StringUtils.UTF_8))

    val lang = if (version.onOrAfter(EsMajorVersion.V_5_X)) "painless" else "groovy"
    val props = Map("es.write.operation" -> "upsert",
      "es.input.json" -> "true",
      "es.mapping.id" -> "id",
      "es.update.script.lang" -> lang
    )

    // Upsert a value that should only modify the first document. Modification will add an address entry.
    val lines = sc.makeRDD(List("""{"id":"1","address":{"zipcode":"12345","id":"1"}}"""))
    val up_params = "new_address:address"
    val up_script = if (version.onOrAfter(EsMajorVersion.V_5_X)) {
      "ctx._source.address.add(params.new_address)"
    } else {
      "ctx._source.address+=new_address"
    }
    runStreamRecoverably(lines)(_.saveToEs(target, props + ("es.update.script.params" -> up_params) + ("es.update.script" -> up_script)))

    // Upsert a value that should only modify the second document. Modification will update the "note" field.
    val notes = sc.makeRDD(List("""{"id":"2","note":"Second"}"""))
    val note_up_params = "new_note:note"
    val note_up_script = if (version.onOrAfter(EsMajorVersion.V_5_X)) {
      "ctx._source.note = params.new_note"
    } else {
      "ctx._source.note=new_note"
    }
    runStream(notes)(_.saveToEs(target, props + ("es.update.script.params" -> note_up_params) + ("es.update.script" -> note_up_script)))

    assertTrue(RestUtils.exists(s"$target/1"))
    assertThat(RestUtils.get(s"$target/1"), both(containsString(""""zipcode":"12345"""")).and(containsString(""""note":"First"""")))

    assertTrue(RestUtils.exists(s"$target/2"))
    assertThat(RestUtils.get(s"$target/2"), both(not(containsString(""""zipcode":"12345""""))).and(containsString(""""note":"Second"""")))
  }

  @Test
  def testNullAsEmpty(): Unit = {
    val data = Seq(
      Map("field1" -> 5.4, "field2" -> "foo"),
      Map("field2" -> "bar"),
      Map("field1" -> 0.0, "field2" -> "baz")
    )
    val target = wrapIndex("spark-streaming-test-nullasempty/data")
    val batch = sc.makeRDD(data)

    runStream(batch)(_.saveToEs(target))

    assertEquals(3, EsSpark.esRDD(sc, target, cfg).count())
  }

  /**
   * Run a streaming job. Streaming jobs for this test case will not be runnable after this method.
   * In cases where you must run multiple streaming jobs in a test case, use runStreamRecoverably
   */
  def runStream[T: ClassTag](rdd: RDD[T])(f: DStream[T] => Unit): Unit = {
    val rddQueue = mutable.Queue(rdd)
    val stream = ssc.queueStream(rddQueue, oneAtATime = true)
    f(stream)
    ssc.start()
    TimeUnit.SECONDS.sleep(2) // Let the stream processing happen
    ssc.stop(stopSparkContext = false, stopGracefully = true)
  }

  /**
   * Run a streaming job in a way that other streaming jobs may be run
   */
  def runStreamRecoverably[T: ClassTag](rdd: RDD[T])(f: DStream[T] => Unit): Unit = {
    val rddQueue = mutable.Queue(rdd)
    val stream = ssc.queueStream(rddQueue, oneAtATime = true)
    f(stream)
    ssc.start()
    TimeUnit.SECONDS.sleep(2) // Let the stream processing happen
    ssc.stop(stopSparkContext = false, stopGracefully = true)
    ssc = new StreamingContext(sc, Seconds(1))
  }

  def wrapIndex(index: String) = {
    prefix + index
  }


  object ExpectingToThrow {
    def apply[T<:Throwable](expected: Class[T]): ExpectingToThrow[T] = new ExpectingToThrow(expected)
  }

  /**
   * Implement a StreamingListener to listen for failed output operations
   * on an StreamingContext. If there's an exception that matches, keep track.
   * At the end of the test you should call assertExceptionFound to note if
   * the expected outcome occurred.
   */
  class ExpectingToThrow[T <: Throwable](expected: Class[T]) extends StreamingListener {
    var foundException: Boolean = false
    var exceptionType: Any = _

    override def onOutputOperationCompleted(outputOperationCompleted: StreamingListenerOutputOperationCompleted): Unit = {
      val exceptionName = outputOperationCompleted.outputOperationInfo.failureReason match {
        case Some(value) => value.substring(0, value.indexOf(':'))
        case None => ""
      }

      foundException = foundException || expected.getCanonicalName.equals(exceptionName)
      exceptionType = if (foundException) exceptionName
    }

    def from(ssc: StreamingContext): ExpectingToThrow[T] = {
      ssc.addStreamingListener(this)
      this
    }

    def assertExceptionFound(): Unit = {
      if (!foundException) {
        exceptionType match {
          case s: String => Assert.fail(s"Expected ${expected.getCanonicalName} but got $s")
          case _ => Assert.fail(s"Expected ${expected.getCanonicalName} but no Exceptions were thrown")
        }
      }
    }
  }
}