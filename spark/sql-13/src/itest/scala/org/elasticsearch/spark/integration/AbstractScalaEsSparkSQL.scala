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

import java.{lang => jl}
import java.sql.Timestamp
import java.{util => ju}
import java.util.concurrent.TimeUnit
import scala.collection.JavaConversions.propertiesAsScalaMap
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkException
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.storage.StorageLevel._
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
import org.elasticsearch.hadoop.cfg.PropertiesSettings
import org.elasticsearch.hadoop.mr.RestUtils
import org.elasticsearch.hadoop.util.StringUtils
import org.elasticsearch.hadoop.util.TestSettings
import org.elasticsearch.hadoop.util.TestUtils
import org.elasticsearch.spark._
import org.elasticsearch.spark.cfg._
import org.elasticsearch.spark.sql._
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL
import org.elasticsearch.spark.sql.sqlContextFunctions
import org.hamcrest.Matchers.containsString
import org.hamcrest.Matchers.is
import org.hamcrest.Matchers.not
import org.junit.AfterClass
import org.junit.Assert._
import org.junit.Assume._
import org.junit.BeforeClass
import org.junit.FixMethodOrder
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters
import org.junit.runners.MethodSorters
import com.esotericsoftware.kryo.io.{Input => KryoInput}
import com.esotericsoftware.kryo.io.{Output => KryoOutput}
import javax.xml.bind.DatatypeConverter
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException
import org.elasticsearch.hadoop.serialization.EsHadoopSerializationException
import org.apache.spark.sql.types.DoubleType

object AbstractScalaEsScalaSparkSQL {
  @transient val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .setMaster("local").setAppName("estest").set("spark.executor.extraJavaOptions", "-XX:MaxPermSize=256m")
    .setJars(SparkUtils.ES_SPARK_TESTING_JAR)
  @transient var cfg: SparkConf = null
  @transient var sc: SparkContext = null
  @transient var sqc: SQLContext = null

  @BeforeClass
  def setup() {
    conf.setAll(TestSettings.TESTING_PROPS);
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

  @Parameters
  def testParams(): ju.Collection[Array[jl.Object]] = {
    val list = new ju.ArrayList[Array[jl.Object]]()
    // no query
    val noQuery = ""
    list.add(Array("default", jl.Boolean.FALSE, jl.Boolean.TRUE, jl.Boolean.FALSE, jl.Boolean.TRUE, noQuery))
    list.add(Array("defaultstrict", jl.Boolean.FALSE, jl.Boolean.TRUE, jl.Boolean.TRUE, jl.Boolean.TRUE, noQuery))
    list.add(Array("defaultnopush", jl.Boolean.FALSE, jl.Boolean.FALSE, jl.Boolean.FALSE, jl.Boolean.TRUE, noQuery))
    list.add(Array("withmeta", jl.Boolean.TRUE, jl.Boolean.TRUE, jl.Boolean.FALSE, jl.Boolean.TRUE, noQuery))
    list.add(Array("withmetastrict", jl.Boolean.TRUE, jl.Boolean.TRUE, jl.Boolean.TRUE, jl.Boolean.TRUE, noQuery))
    list.add(Array("withmetanopush", jl.Boolean.TRUE, jl.Boolean.FALSE, jl.Boolean.FALSE, jl.Boolean.TRUE, noQuery))

    // disable double filtering
    list.add(Array("default_skiphandled", jl.Boolean.FALSE, jl.Boolean.TRUE, jl.Boolean.FALSE, jl.Boolean.FALSE, noQuery))
    list.add(Array("defaultstrict_skiphandled", jl.Boolean.FALSE, jl.Boolean.TRUE, jl.Boolean.TRUE, jl.Boolean.FALSE, noQuery))
    list.add(Array("defaultnopush_skiphandled", jl.Boolean.FALSE, jl.Boolean.FALSE, jl.Boolean.FALSE, jl.Boolean.FALSE, noQuery))
    list.add(Array("withmeta_skiphandled", jl.Boolean.TRUE, jl.Boolean.TRUE, jl.Boolean.FALSE, jl.Boolean.FALSE, noQuery))
    list.add(Array("withmetastrict_skiphandled", jl.Boolean.TRUE, jl.Boolean.TRUE, jl.Boolean.TRUE, jl.Boolean.FALSE, noQuery))
    list.add(Array("withmetanopush_skiphandled", jl.Boolean.TRUE, jl.Boolean.FALSE, jl.Boolean.FALSE, jl.Boolean.FALSE, noQuery))

    // uri query
    val uriQuery = "?q=*"
    list.add(Array("defaulturiquery", jl.Boolean.FALSE, jl.Boolean.TRUE, jl.Boolean.FALSE, jl.Boolean.TRUE, uriQuery))
    list.add(Array("defaulturiquerystrict", jl.Boolean.FALSE, jl.Boolean.TRUE, jl.Boolean.TRUE, jl.Boolean.TRUE, uriQuery))
    list.add(Array("defaulturiquerynopush", jl.Boolean.FALSE, jl.Boolean.FALSE, jl.Boolean.FALSE, jl.Boolean.TRUE, uriQuery))
    list.add(Array("withmetauri_query", jl.Boolean.TRUE, jl.Boolean.TRUE, jl.Boolean.FALSE, jl.Boolean.TRUE, uriQuery))
    list.add(Array("withmetauri_querystrict", jl.Boolean.TRUE, jl.Boolean.TRUE, jl.Boolean.TRUE, jl.Boolean.TRUE, uriQuery))
    list.add(Array("withmetauri_querynopush", jl.Boolean.TRUE, jl.Boolean.FALSE, jl.Boolean.FALSE, jl.Boolean.TRUE, uriQuery))

    // disable double filtering
    list.add(Array("defaulturiquery_skiphandled", jl.Boolean.FALSE, jl.Boolean.TRUE, jl.Boolean.FALSE, jl.Boolean.FALSE, uriQuery))
    list.add(Array("defaulturiquerystrict_skiphandled", jl.Boolean.FALSE, jl.Boolean.TRUE, jl.Boolean.TRUE, jl.Boolean.FALSE, uriQuery))
    list.add(Array("defaulturiquerynopush_skiphandled", jl.Boolean.FALSE, jl.Boolean.FALSE, jl.Boolean.FALSE, jl.Boolean.FALSE, uriQuery))
    list.add(Array("withmetauri_query_skiphandled", jl.Boolean.TRUE, jl.Boolean.TRUE, jl.Boolean.FALSE, jl.Boolean.FALSE, uriQuery))
    list.add(Array("withmetauri_querystrict_skiphandled", jl.Boolean.TRUE, jl.Boolean.TRUE, jl.Boolean.TRUE, jl.Boolean.FALSE, uriQuery))
    list.add(Array("withmetauri_querynopush_skiphandled", jl.Boolean.TRUE, jl.Boolean.FALSE, jl.Boolean.FALSE, jl.Boolean.FALSE, uriQuery))

    // dsl query
    val dslQuery = """ {"query" : { "match_all" : { } } } """
    list.add(Array("defaultdslquery", jl.Boolean.FALSE, jl.Boolean.TRUE, jl.Boolean.FALSE, jl.Boolean.TRUE, dslQuery))
    list.add(Array("defaultstrictdslquery", jl.Boolean.FALSE, jl.Boolean.TRUE, jl.Boolean.TRUE, jl.Boolean.TRUE, dslQuery))
    list.add(Array("defaultnopushdslquery", jl.Boolean.FALSE, jl.Boolean.FALSE, jl.Boolean.FALSE, jl.Boolean.TRUE, dslQuery))
    list.add(Array("withmetadslquery", jl.Boolean.TRUE, jl.Boolean.TRUE, jl.Boolean.FALSE, jl.Boolean.TRUE, dslQuery))
    list.add(Array("withmetastrictdslquery", jl.Boolean.TRUE, jl.Boolean.TRUE, jl.Boolean.TRUE, jl.Boolean.TRUE, dslQuery))
    list.add(Array("withmetanopushdslquery", jl.Boolean.TRUE, jl.Boolean.FALSE, jl.Boolean.FALSE, jl.Boolean.TRUE, dslQuery))

    // disable double filtering
    list.add(Array("defaultdslquery_skiphandled", jl.Boolean.FALSE, jl.Boolean.TRUE, jl.Boolean.FALSE, jl.Boolean.FALSE, dslQuery))
    list.add(Array("defaultstrictdslquery_skiphandled", jl.Boolean.FALSE, jl.Boolean.TRUE, jl.Boolean.TRUE, jl.Boolean.FALSE, dslQuery))
    list.add(Array("defaultnopushdslquery_skiphandled", jl.Boolean.FALSE, jl.Boolean.FALSE, jl.Boolean.FALSE, jl.Boolean.FALSE, dslQuery))
    list.add(Array("withmetadslquery_skiphandled", jl.Boolean.TRUE, jl.Boolean.TRUE, jl.Boolean.FALSE, jl.Boolean.FALSE, dslQuery))
    list.add(Array("withmetastrictdslquery_skiphandled", jl.Boolean.TRUE, jl.Boolean.TRUE, jl.Boolean.TRUE, jl.Boolean.FALSE, dslQuery))
    list.add(Array("withmetanopushdslquery_skiphandled", jl.Boolean.TRUE, jl.Boolean.FALSE, jl.Boolean.FALSE, jl.Boolean.FALSE, dslQuery))

    list
  }
}

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(classOf[Parameterized])
class AbstractScalaEsScalaSparkSQL(prefix: String, readMetadata: jl.Boolean, pushDown: jl.Boolean, strictPushDown: jl.Boolean, doubleFiltering: jl.Boolean, query: String = "") extends Serializable {

  val sc = AbstractScalaEsScalaSparkSQL.sc
  val sqc = AbstractScalaEsScalaSparkSQL.sqc
  val cfg = Map(ES_QUERY -> query,
                ES_READ_METADATA -> readMetadata.toString(),
                "es.internal.spark.sql.pushdown" -> pushDown.toString(),
                "es.internal.spark.sql.pushdown.strict" -> strictPushDown.toString(),
                "es.internal.spark.sql.pushdown.keep.handled.filters" -> doubleFiltering.toString())

  val datInput = TestUtils.sampleArtistsDat()

  @Test
  def test1KryoScalaEsRow() {
    val kryo = SparkUtils.sparkSerializer(sc.getConf)
    val row = new ScalaEsRow(new ArrayBuffer() ++= StringUtils.tokenize("foo,bar,tar").asScala)

    val storage = Array.ofDim[Byte](512)
    val output = new KryoOutput(storage)
    val input = new KryoInput(storage)

    kryo.writeClassAndObject(output, row)
    val serialized = kryo.readClassAndObject(input).asInstanceOf[ScalaEsRow]
    println(serialized.rowOrder)
  }

  @Test(expected = classOf[EsHadoopIllegalArgumentException])
  def testNoIndexExists() {
    val idx = sqc.read.format("org.elasticsearch.spark.sql").load("existing_index/not_existing_mapping")
    idx.printSchema()
  }

  @Test(expected = classOf[EsHadoopIllegalArgumentException])
  def testNoMappingExists() {
    val index = wrapIndex("spark-index-ex")
    RestUtils.touch(index)
    val idx = sqc.read.format("org.elasticsearch.spark.sql").load(s"$index/no_such_mapping")
    idx.printSchema()
  }

  @Test
  def testArrayMappingFirstLevel() {
    val mapping = """{ "array-mapping-top-level": {
      | "properties" : {
      |   "arr" : {
      |     "properties" : {
      |          "one" : { "type" : "string" },
      |          "two" : { "type" : "string" }
      |     }
      |   },
      |   "top-level" : { "type" : "string" }
      | }
      |}
      }""".stripMargin

    val index = wrapIndex("sparksql-test")
    val indexAndType = s"$index/array-mapping-top-level"
    RestUtils.touch(index)
    RestUtils.putMapping(indexAndType, mapping.getBytes(StringUtils.UTF_8))

    // add some data
    val doc1 = """{"arr" : [{"one" : "1", "two" : "2"}, {"one" : "unu", "two" : "doi"}], "top-level" : "root" }""".stripMargin

    RestUtils.postData(indexAndType, doc1.getBytes(StringUtils.UTF_8))
    RestUtils.refresh(index)

    val newCfg = collection.mutable.Map(cfg.toSeq: _*) += (ES_READ_FIELD_AS_ARRAY_INCLUDE -> "arr")

    val df = sqc.read.options(newCfg).format("org.elasticsearch.spark.sql").load(indexAndType)
    df.printSchema()
    df.take(1).foreach(println)
    assertEquals(1, df.count())
  }

  @Test
  def testMultiFieldsWithSameName {
    val index = wrapIndex("sparksql-test")
    val indexAndType = s"$index/array-mapping-nested"
    RestUtils.touch(index)

    // add some data
    val jsonDoc = """{
    |  "bar" : {
    |    "bar" : {
    |      "bar" : [{
    |          "bar" : 1
    |        }, {
    |          "bar" : 2
    |        }
    |      ],
    |      "level" : 2,
    |      "level3" : true
    |    },
    |    "foo2" : 10,
    |    "level" : 1,
    |    "level2" : 2
    |  },
    |  "foo1" : "text",
    |  "level" : 0,
    |  "level1" : "string"
    |}
    """.stripMargin
    RestUtils.postData(indexAndType, jsonDoc.getBytes(StringUtils.UTF_8))
    RestUtils.refresh(index)

    val newCfg = collection.mutable.Map(cfg.toSeq: _*) += (ES_READ_FIELD_AS_ARRAY_INCLUDE -> "bar.bar.bar", "es.resource" -> indexAndType)
    val cfgSettings = new SparkSettingsManager().load(sc.getConf).copy().merge(newCfg.asJava)
    val schema = SchemaUtilsTestable.discoverMapping(cfgSettings)
    val mapping = SchemaUtilsTestable.rowInfo(cfgSettings)

    val df = sqc.read.options(newCfg).format("org.elasticsearch.spark.sql").load(indexAndType)
    df.printSchema()
    df.take(1).foreach(println)
    assertEquals(1, df.count())
  }

  @Test
  def testNestedFieldArray {
    val index = wrapIndex("sparksql-test")
    val indexAndType = s"$index/nested-same-name-fields"
    RestUtils.touch(index)

    // add some data
    val jsonDoc = """{"foo" : 5, "nested": { "bar" : [{"date":"2015-01-01", "age":20},{"date":"2015-01-01", "age":20}], "what": "now" } }"""
    sc.makeRDD(Seq(jsonDoc)).saveJsonToEs(indexAndType)
    RestUtils.refresh(index)

    val newCfg = collection.mutable.Map(cfg.toSeq: _*) += (ES_READ_FIELD_AS_ARRAY_INCLUDE -> "nested.bar")

    val df = sqc.read.options(newCfg).format("org.elasticsearch.spark.sql").load(indexAndType)
    df.printSchema()
    df.take(1).foreach(println)
    assertEquals(1, df.count())
  }

  @Test
  def testArrayValue {
    val index = wrapIndex("sparksql-test")
    val indexAndType = s"$index/array-value"
    RestUtils.touch(index)

    // add some data
    val jsonDoc = """{"array" : [1, 2, 4, 5] }"""
    sc.makeRDD(Seq(jsonDoc)).saveJsonToEs(indexAndType)
    RestUtils.refresh(index)

    val newCfg = collection.mutable.Map(cfg.toSeq: _*) += (ES_READ_FIELD_AS_ARRAY_INCLUDE -> "array")

    val df = sqc.read.options(newCfg).format("org.elasticsearch.spark.sql").load(indexAndType)
    
    assertEquals("array", df.schema("array").dataType.typeName)
    assertEquals("long", df.schema("array").dataType.asInstanceOf[ArrayType].elementType.typeName)
    assertEquals(1, df.count())
    
    val first = df.first()
    val array = first.getSeq[Long](0)
    assertEquals(1l, array(0))
    assertEquals(4l, array(2))
  }


  @Test
  def testBasicRead() {
    val dataFrame = artistsAsDataFrame
    assertTrue(dataFrame.count > 300)
    dataFrame.registerTempTable("datfile")
    println(dataFrame.schema.treeString)
    //dataFrame.take(5).foreach(println)
    val results = sqc.sql("SELECT name FROM datfile WHERE id >=1 AND id <=10")
    //results.take(5).foreach(println)
  }

  @Test
  def testEsDataFrame1Write() {
    val dataFrame = artistsAsDataFrame

    val target = wrapIndex("sparksql-test/scala-basic-write")
    dataFrame.saveToEs(target, cfg)
    assertTrue(RestUtils.exists(target))
    assertThat(RestUtils.get(target + "/_search?"), containsString("345"))
  }

  @Test
  def testEsDataFrame1WriteCount() {
    val target = wrapIndex("sparksql-test/scala-basic-write")

    val dataFrame = sqc.esDF(target, cfg)
    assertEquals(345, dataFrame.count())
  }

  @Test
  def testEsDataFrame1WriteWithMapping() {
    val dataFrame = artistsAsDataFrame

    val target = wrapIndex("sparksql-test/scala-basic-write-id-mapping")
    val newCfg = collection.mutable.Map(cfg.toSeq: _*) += (ES_MAPPING_ID -> "id", ES_MAPPING_EXCLUDE -> "url")

    dataFrame.saveToEs(target, newCfg)
    assertTrue(RestUtils.exists(target))
    assertThat(RestUtils.get(target + "/_search?"), containsString("345"))
    assertThat(RestUtils.exists(target + "/1"), is(true))
    assertThat(RestUtils.get(target + "/_search?"), not(containsString("url")))
  }

  @Test
  def testEsDataFrame2Read() {
    val target = wrapIndex("sparksql-test/scala-basic-write")

    val dataFrame = sqc.esDF(target, cfg)
    dataFrame.printSchema()
    val schema = dataFrame.schema.treeString
    assertTrue(schema.contains("id: long"))
    assertTrue(schema.contains("name: string"))
    assertTrue(schema.contains("pictures: string"))
    assertTrue(schema.contains("time: long"))
    assertTrue(schema.contains("url: string"))

    assertTrue(dataFrame.count > 300)

    //dataFrame.take(5).foreach(println)

    val tempTable = wrapIndex("basicRead")
    dataFrame.registerTempTable(tempTable)
    val nameRDD = sqc.sql(s"SELECT name FROM $tempTable WHERE id >= 1 AND id <=10")
    nameRDD.take(7).foreach(println)
    assertEquals(10, nameRDD.count)
  }

  @Test
  def testEsDataFrame2ReadWithIncludeFields() {
    val target = wrapIndex("sparksql-test/scala-basic-write")

    val newCfg = collection.mutable.Map(cfg.toSeq: _*) += (ES_READ_FIELD_INCLUDE -> "id, name, url")

    val dataFrame = sqc.esDF(target, newCfg)
    val schema = dataFrame.schema.treeString
    assertTrue(schema.contains("id: long"))
    assertTrue(schema.contains("name: string"))
    assertFalse(schema.contains("pictures: string"))
    assertFalse(schema.contains("time:"))
    assertTrue(schema.contains("url: string"))

    assertTrue(dataFrame.count > 300)

    //dataFrame.take(5).foreach(println)

    val tempTable = wrapIndex("basicRead")
    dataFrame.registerTempTable(tempTable)
    val nameRDD = sqc.sql(s"SELECT name FROM $tempTable WHERE id >= 1 AND id <=10")
    nameRDD.take(7).foreach(println)
    assertEquals(10, nameRDD.count)
  }

  @Test
  def testEsDataFrame2ReadWithAndWithoutQuery() {
    val target = wrapIndex("sparksql-test/scala-basic-write")

    val dfNoQuery = sqc.esDF(target, cfg)
    val dfWQuery = sqc.esDF(target, "?q=name:me*", cfg)

    println(dfNoQuery.head())
    println(dfWQuery.head())

    //assertEquals(dfNoQuery.head().toString(), dfWQuery.head().toString())
  }

  @Test
  def testEsDataFrame2ReadWithAndWithoutQueryInJava() {
    val target = wrapIndex("sparksql-test/scala-basic-write")

    val dfNoQuery = JavaEsSparkSQL.esDF(sqc, target, cfg.asJava)
    val query = s"""{ "query" : { "query_string" : { "query" : "name:me*" } } //, "fields" : ["name"]
                }"""
    val dfWQuery = JavaEsSparkSQL.esDF(sqc, target, query, cfg.asJava)

    println(dfNoQuery.head())
    println(dfWQuery.head())
    dfNoQuery.show(3)
    dfWQuery.show(3)

    //assertEquals(dfNoQuery.head().toString(), dfWQuery.head().toString())
  }

  @Test
  def testEsDataFrame3WriteWithRichMapping() {
    val input = datInput
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
    val dataFrame = sqc.createDataFrame(rowRDD, schema)

    val target = wrapIndex("sparksql-test/scala-basic-write-rich-mapping-id-mapping")
    dataFrame.saveToEs(target, Map(ES_MAPPING_ID -> "id"))
    assertTrue(RestUtils.exists(target))
    assertThat(RestUtils.get(target + "/_search?"), containsString("345"))
    assertThat(RestUtils.exists(target + "/1"), is(true))
  }

  @Test(expected = classOf[SparkException])
  def testEsDataFrame3WriteDecimalType() {
    val schema = StructType(Seq(StructField("decimal", DecimalType.USER_DEFAULT, false)))

    val rowRDD = sc.makeRDD(Seq(Row(Decimal(10))))
    val dataFrame = sqc.createDataFrame(rowRDD, schema)

    val target = wrapIndex("sparksql-test/decimal-exception")
    dataFrame.saveToEs(target)
  }

  @Test
  def testEsDataFrame4ReadRichMapping() {
    val target = wrapIndex("sparksql-test/scala-basic-write-rich-mapping-id-mapping")

    val dataFrame = sqc.esDF(target, cfg)

    assertTrue(dataFrame.count > 300)
    dataFrame.printSchema()
  }

  private def artistsAsDataFrame = {
    val input = datInput
    val data = sc.textFile(input)

    val schema = StructType(Seq(StructField("id", IntegerType, false),
      StructField("name", StringType, false),
      StructField("url", StringType, true),
      StructField("pictures", StringType, true),
      StructField("time", TimestampType, true)))

    val rowRDD = data.map(_.split("\t")).map(r => Row(r(0).toInt, r(1), r(2), r(3), new Timestamp(DatatypeConverter.parseDateTime(r(4)).getTimeInMillis())))
    val dataFrame = sqc.createDataFrame(rowRDD, schema)
    dataFrame
  }

  @Test
  def testEsDataFrame50ReadAsDataSource() {
    val target = wrapIndex("sparksql-test/scala-basic-write")
    var options = s"""resource '$target' """
    val table = wrapIndex("sqlbasicread1")

    val query = s"CREATE TEMPORARY TABLE $table "+
      " USING org.elasticsearch.spark.sql " +
      s" OPTIONS ($options)"

    println(query)

    val dataFrame = sqc.sql(query)

    val dsCfg = collection.mutable.Map(cfg.toSeq: _*) += ("path" -> target)
    val dfLoad = sqc.load("es", dsCfg.toMap)
    println("root data frame")
    dfLoad.printSchema()

    val results = dfLoad.filter(dfLoad("id") >= 1 && dfLoad("id") <= 10)
    println("results data frame")
    results.printSchema()

    val allRDD = sqc.sql(s"SELECT * FROM $table WHERE id >= 1 AND id <=10")
    println("select all rdd")
    allRDD.printSchema()

    val nameRDD = sqc.sql(s"SELECT name FROM $table WHERE id >= 1 AND id <=10")
    println("select name rdd")
    nameRDD.printSchema()

    assertEquals(10, nameRDD.count)
    nameRDD.take(7).foreach(println)
  }

  @Test
  def testEsDataFrameReadAsDataSourceWithMetadata() {
    assumeTrue(readMetadata)

    val target = wrapIndex("sparksql-test/scala-basic-write")
    val table = wrapIndex("sqlbasicread2")

    val options = s"""path '$target' , readMetadata "true" """
    val dataFrame = sqc.sql(s"CREATE TEMPORARY TABLE $table" +
      " USING es " +
      s" OPTIONS ($options)")

    val allRDD = sqc.sql(s"SELECT * FROM $table WHERE id >= 1 AND id <=10")
    allRDD.printSchema()
    allRDD.take(7).foreach(println)

    val dsCfg = collection.mutable.Map(cfg.toSeq: _*) += ("path" -> target)
    val dfLoad = sqc.load("es", dsCfg.toMap)
    dfLoad.show()
  }

  @Test
  def testDataSource0Setup() {
    val target = wrapIndex("spark-test/scala-sql-varcols")
    val table = wrapIndex("sqlvarcol")

    val trip1 = Map("reason" -> "business", "airport" -> "SFO", "tag" -> "jan", "date" -> "2015-12-28T20:03:10Z")
    val trip2 = Map("participants" -> 5, "airport" -> "OTP", "tag" -> "feb", "date" -> "2013-12-28T20:03:10Z")
    val trip3 = Map("participants" -> 3, "airport" -> "MUC OTP SFO JFK", "tag" -> "long", "date" -> "2012-12-28T20:03:10Z")

    sc.makeRDD(Seq(trip1, trip2, trip3)).saveToEs(target)
  }

  private def esDataSource(table: String) = {
    val target = wrapIndex("spark-test/scala-sql-varcols")

    var options = s"""resource "$target" """


//    sqc.sql(s"CREATE TEMPORARY TABLE $table" +
//      " USING org.elasticsearch.spark.sql " +
//      s" OPTIONS ($options)")

    val dsCfg = collection.mutable.Map(cfg.toSeq: _*) += ("path" -> target)
    sqc.load("org.elasticsearch.spark.sql", dsCfg.toMap)
  }

  @Test
  def testDataSourcePushDown01EqualTo() {
    val df = esDataSource("pd_equalto")
    val filter = df.filter(df("airport").equalTo("OTP"))

    filter.show

    if (strictPushDown) {
      assertEquals(0, filter.count())
      // however if we change the arguments to be lower cased, it will be Spark who's going to filter out the data
      return
    }
    else if (!keepHandledFilters) {
      // term query pick field with multi values
      assertEquals(2, filter.count())
      return
    }

    assertEquals(1, filter.count())
    assertEquals("feb", filter.select("tag").take(1)(0)(0))
  }

  @Test
  def testDataSourcePushDown015NullSafeEqualTo() {
    val df = esDataSource("pd_nullsafeequalto")
    val filter = df.filter(df("airport").eqNullSafe("OTP"))

    filter.show

    if (strictPushDown) {
      assertEquals(0, filter.count())
      // however if we change the arguments to be lower cased, it will be Spark who's going to filter out the data
      return
    }
    else if (!keepHandledFilters) {
      // term query pick field with multi values
      assertEquals(2, filter.count())
      return
    }

    assertEquals(1, filter.count())
    assertEquals("feb", filter.select("tag").take(1)(0)(0))
  }

  @Test
  def testDataSourcePushDown02GT() {
    val df = esDataSource("pd_gt")
    val filter = df.filter(df("participants").gt(3))
    assertEquals(1, filter.count())
    assertEquals("feb", filter.select("tag").take(1)(0)(0))
  }

  @Test
  def testDataSourcePushDown03GTE() {
    val df = esDataSource("pd_gte")
    val filter = df.filter(df("participants").geq(3))
    assertEquals(2, filter.count())
    assertEquals("long", filter.select("tag").sort("tag").take(2)(1)(0))
  }

  @Test
  def testDataSourcePushDown04LT() {
    val df = esDataSource("pd_lt")
    df.printSchema()
    val filter = df.filter(df("participants").lt(5))
    assertEquals(1, filter.count())
    assertEquals("long", filter.select("tag").take(1)(0)(0))
  }

  @Test
  def testDataSourcePushDown05LTE() {
    val df = esDataSource("pd_lte")
    val filter = df.filter(df("participants").leq(5))
    assertEquals(2, filter.count())
    assertEquals("long", filter.select("tag").sort("tag").take(2)(1)(0))
  }

  @Test
  def testDataSourcePushDown06IsNull() {
    val df = esDataSource("pd_is_null")
    val filter = df.filter(df("participants").isNull)
    assertEquals(1, filter.count())
    assertEquals("jan", filter.select("tag").take(1)(0)(0))
  }

  @Test
  def testDataSourcePushDown07IsNotNull() {
    val df = esDataSource("pd_is_not_null")
    val filter = df.filter(df("reason").isNotNull)
    assertEquals(1, filter.count())
    assertEquals("jan", filter.select("tag").take(1)(0)(0))
  }

  @Test
  def testDataSourcePushDown08In() {
    val df = esDataSource("pd_in")
    var filter = df.filter("airport IN ('OTP', 'SFO', 'MUC')")

    if (strictPushDown) {
      assertEquals(0, filter.count())
      // however if we change the arguments to be lower cased, it will be Spark who's going to filter out the data
      return
    }

    assertEquals(2, filter.count())
    assertEquals("jan", filter.select("tag").sort("tag").take(2)(1)(0))
  }

  @Test
  def testDataSourcePushDown08InWithNumbersAsStrings() {
    val df = esDataSource("pd_in_numbers_strings")
    var filter = df.filter("date IN ('2015-12-28', '2012-12-28')")

    if (strictPushDown) {
      assertEquals(0, filter.count())
      // however if we change the arguments to be lower cased, it will be Spark who's going to filter out the data
      return
    }

    assertEquals(0, filter.count())
  }

  @Test
  def testDataSourcePushDown08InWithNumber() {
    val df = esDataSource("pd_in_number")
    var filter = df.filter("participants IN (1, 2, 3)")

    assertEquals(1, filter.count())
    assertEquals("long", filter.select("tag").sort("tag").take(1)(0)(0))
  }

  @Test
  def testDataSourcePushDown08InWithNumberAndStrings() {
    val df = esDataSource("pd_in_number")
    var filter = df.filter("participants IN (2, 'bar', 1, 'foo')")

    assertEquals(0, filter.count())
  }

  @Test
  def testDataSourcePushDown09StartsWith() {
    val df = esDataSource("pd_starts_with")
    var filter = df.filter(df("airport").startsWith("O"))

    if (strictPushDown) {
      assertEquals(0, filter.count())
      return
    }

    if (!keepHandledFilters) {
      // term query pick field with multi values
      assertEquals(2, filter.count())
      return
    }

    filter.show
    assertEquals(1, filter.count())
    assertEquals("feb", filter.select("tag").take(1)(0)(0))
  }

  @Test
  def testDataSourcePushDown10EndsWith() {
    val df = esDataSource("pd_ends_with")
    var filter = df.filter(df("airport").endsWith("O"))

    if (strictPushDown) {
      assertEquals(0, filter.count())
      return
    }

    if (!keepHandledFilters) {
      // term query pick field with multi values
      assertEquals(2, filter.count())
      return
    }

    filter.show
    assertEquals(1, filter.count())
    assertEquals("jan", filter.select("tag").take(1)(0)(0))
  }

  @Test
  def testDataSourcePushDown11Contains() {
    val df = esDataSource("pd_contains")
    val filter = df.filter(df("reason").contains("us"))
    assertEquals(1, filter.count())
    assertEquals("jan", filter.select("tag").take(1)(0)(0))
  }

  @Test
  def testDataSourcePushDown12And() {
    val df = esDataSource("pd_and")
    var filter = df.filter(df("reason").isNotNull.and(df("airport").endsWith("O")))

    if (strictPushDown) {
      assertEquals(0, filter.count())
      return
    }

    assertEquals(1, filter.count())
    assertEquals("jan", filter.select("tag").take(1)(0)(0))
  }

  @Test
  def testDataSourcePushDown13Not() {
    val df = esDataSource("pd_not")
    val filter = df.filter(!df("reason").isNull)

    assertEquals(1, filter.count())
    assertEquals("jan", filter.select("tag").take(1)(0)(0))
  }

  @Test
  def testDataSourcePushDown14OR() {
    val df = esDataSource("pd_or")
    var filter = df.filter(df("reason").contains("us").or(df("airport").equalTo("OTP")))

    if (strictPushDown) {
      // OTP fails due to strict matching/analyzed
      assertEquals(1, filter.count())
      return
    }

    if (!keepHandledFilters) {
      // term query pick field with multi values
      assertEquals(3, filter.count())
      return
    }

    assertEquals(2, filter.count())
    assertEquals("feb", filter.select("tag").sort("tag").take(1)(0)(0))
  }

  @Test
  def testEsSchemaFromDocsWithDifferentProperties() {
    val table = wrapIndex("sqlvarcol")
    esDataSource(table)

    val target = wrapIndex("spark-test/scala-sql-varcols")

    var options = s"""resource '$target' """

    val s = sqc.sql(s"CREATE TEMPORARY TABLE $table" +
       " USING org.elasticsearch.spark.sql " +
       s" OPTIONS ($options)")

    val allResults = sqc.sql(s"SELECT * FROM $table")
    assertEquals(3, allResults.count())
    allResults.printSchema()

    val filter = sqc.sql(s"SELECT * FROM $table WHERE airport = 'OTP'")
    assertEquals(1, filter.count())

    val nullColumns = sqc.sql(s"SELECT reason, airport FROM $table ORDER BY airport")
    val rows = nullColumns.take(3)
    assertEquals("[null,MUC OTP SFO JFK]", rows(0).toString())
    assertEquals("[null,OTP]", rows(1).toString())
    assertEquals("[business,SFO]", rows(2).toString())
  }

  @Test
  def testJsonLoadAndSavedToEs() {
    val input = sqc.jsonFile(this.getClass.getResource("/simple.json").toURI().toString())
    println(input.schema.simpleString)

    val target = wrapIndex("spark-test/json-file")
    input.saveToEs(target, cfg)

    val basic = sqc.jsonFile(this.getClass.getResource("/basic.json").toURI().toString())
    println(basic.schema.simpleString)
    basic.saveToEs(target, cfg)
  }

  @Test
  def testJsonLoadAndSavedToEsSchema() {
    assumeFalse(readMetadata)
    val input = sqc.jsonFile(this.getClass.getResource("/multi-level-doc.json").toURI().toString())
    println("JSON schema")
    println(input.schema.treeString)
    println(input.schema)
    val sample = input.take(1)(0).toString()

    val target = wrapIndex("spark-test/json-file-schema")
    input.saveToEs(target, cfg)

    val dsCfg = collection.mutable.Map(cfg.toSeq: _*) += ("path" -> target)
    val dfLoad = sqc.load("org.elasticsearch.spark.sql", dsCfg.toMap)
    println("JSON schema")
    println(input.schema.treeString)
    println("Reading information from Elastic")
    println(dfLoad.schema.treeString)
    val item = dfLoad.take(1)(0)
    println(item.schema)
    println(item.toSeq)
    val nested = item.getStruct(1)
    println(nested.get(0))
    println(nested.get(0).getClass())

    assertEquals(input.schema.treeString, dfLoad.schema.treeString)
    assertEquals(sample, item.toString())
  }

  @Test
  def testTableJoining() {
    val index1Name = wrapIndex("sparksql-test/scala-basic-write")
    val index2Name = wrapIndex("sparksql-test/scala-basic-write-id-mapping")

    val table1 = sqc.load(index1Name, "org.elasticsearch.spark.sql")
    val table2 = sqc.load(index2Name, "org.elasticsearch.spark.sql")

    table1.persist(DISK_ONLY)
    table2.persist(DISK_ONLY_2)

    val table1Name = wrapIndex("table1")
    val table2Name = wrapIndex("table2")

    table1.registerTempTable(table1Name)
    table1.registerTempTable(table2Name)

    val join = sqc.sql(s"SELECT t1.name, t2.pictures FROM $table1Name t1, $table2Name t2 WHERE t1.id = t2.id")

    println(join.schema.treeString)
    println(join.take(1)(0).schema)
    println(join.take(1)(0)(0))
  }

  @Test
  def testEsDataFrame51WriteToExistingDataSource() {
    // to keep the select static
    assumeFalse(readMetadata)

    val index = wrapIndex("sparksql-test/scala-basic-write")
    val table = wrapIndex("table_insert")

    var options = s"resource '$index '"

    val dataFrame = sqc.sql(s"CREATE TEMPORARY TABLE $table " +
      s"USING org.elasticsearch.spark.sql " +
      s"OPTIONS ($options)");

    val insertRDD = sqc.sql(s"INSERT INTO TABLE $table SELECT 123456789, 'test-sql', 'http://test-sql.com', '', 12345")
    val df = sqc.table(table)
    println(df.count)
    assertTrue(df.count > 100)
  }

  @Test
  def testEsDataFrame52OverwriteExistingDataSource() {
    // to keep the select static
    assumeFalse(readMetadata)

    val srcFrame = artistsAsDataFrame

    val index = wrapIndex("sparksql-test/scala-sql-overwrite")
    srcFrame.saveToEs(index, cfg)

    val table = wrapIndex("table_overwrite")

    var options = s"resource '$index'"

    val dataFrame = sqc.sql(s"CREATE TEMPORARY TABLE $table " +
      s"USING org.elasticsearch.spark.sql " +
      s"OPTIONS ($options)");

    var df = sqc.table(table)
    assertTrue(df.count > 1)
    val insertRDD = sqc.sql(s"INSERT OVERWRITE TABLE $table SELECT 123456789, 'test-sql', 'http://test-sql.com', '', 12345")
    df = sqc.table(table)
    assertEquals(1, df.count)
  }

  @Test
  def testEsDataFrame53OverwriteExistingDataSourceFromAnotherDataSource() {
    // to keep the select static
    assumeFalse(readMetadata)

    val source = wrapIndex("sparksql-test/scala-basic-write")
    val index = wrapIndex("sparksql-test/scala-sql-overwrite-from-df")

    val dstFrame = artistsAsDataFrame
    dstFrame.saveToEs(index, cfg)

    val srcTable = wrapIndex("table_overwrite_src")
    val dstTable = wrapIndex("table_overwrite_dst")

    var dstOptions = s"resource '$source'"

    var srcOptions = s"resource '$index'"

    val srcFrame = sqc.sql(s"CREATE TEMPORARY TABLE $srcTable " +
      s"USING org.elasticsearch.spark.sql " +
      s"OPTIONS ($srcOptions)");

    val dataFrame = sqc.sql(s"CREATE TEMPORARY TABLE $dstTable " +
      s"USING org.elasticsearch.spark.sql " +
      s"OPTIONS ($dstOptions)");

    val insertRDD = sqc.sql(s"INSERT OVERWRITE TABLE $dstTable SELECT * FROM $srcTable")
    val df = sqc.table(dstTable)
    println(df.count)
    assertTrue(df.count > 100)
  }

  @Test
  def testEsDataFrame60DataSourceSaveModeError() {
    val srcFrame = sqc.jsonFile(this.getClass.getResource("/small-sample.json").toURI().toString())
    val index = wrapIndex("sparksql-test/savemode_error")
    val table = wrapIndex("save_mode_error")

    srcFrame.save(index, "org.elasticsearch.spark.sql", SaveMode.ErrorIfExists)
    try {
      srcFrame.save(index, "org.elasticsearch.spark.sql", SaveMode.ErrorIfExists)
      fail()
    } catch {
      case _: Throwable => // swallow
    }
  }

  @Test
  def testEsDataFrame60DataSourceSaveModeAppend() {
    val srcFrame = sqc.jsonFile(this.getClass.getResource("/small-sample.json").toURI().toString())
    srcFrame.printSchema()
    val index = wrapIndex("sparksql-test/savemode_append")
    val table = wrapIndex("save_mode_append")

    srcFrame.save(index, "org.elasticsearch.spark.sql", SaveMode.Append)
    val df = EsSparkSQL.esDF(sqc, index)

    assertEquals(3, df.count())
    srcFrame.save(index, "org.elasticsearch.spark.sql", SaveMode.Append)
    assertEquals(6, df.count())
  }

  @Test
  def testEsDataFrame60DataSourceSaveModeOverwrite() {
    val srcFrame = sqc.jsonFile(this.getClass.getResource("/small-sample.json").toURI().toString())
    val index = wrapIndex("sparksql-test/savemode_overwrite")
    val table = wrapIndex("save_mode_overwrite")

    srcFrame.save(index, "org.elasticsearch.spark.sql", SaveMode.Overwrite)
    val df = EsSparkSQL.esDF(sqc, index)

    assertEquals(3, df.count())
    srcFrame.save(index, "org.elasticsearch.spark.sql", SaveMode.Overwrite)
    assertEquals(3, df.count())
  }

  @Test
  def testEsDataFrame60DataSourceSaveModeIgnore() {
    val srcFrame = sqc.jsonFile(this.getClass.getResource("/small-sample.json").toURI().toString())
    val index = wrapIndex("sparksql-test/savemode_ignore")
    val table = wrapIndex("save_mode_ignore")

    srcFrame.save(index, "org.elasticsearch.spark.sql", SaveMode.Ignore)
    val df = EsSparkSQL.esDF(sqc, index)

    assertEquals(3, df.count())
    artistsAsDataFrame.save(index, "org.elasticsearch.spark.sql", SaveMode.Ignore)
    assertEquals(3, df.count())
  }

  @Test
  def testNestedEmptyArray() {
    val json = """{"foo" : 5, "nested": { "bar" : [], "what": "now" } }"""
    val index = wrapIndex("sparksql-test/empty-nested-array")
    sc.makeRDD(Seq(json)).saveJsonToEs(index)
    val df = sqc.read.format("es").load(index)
    
    assertEquals("long", df.schema("foo").dataType.typeName)
    assertEquals("struct", df.schema("nested").dataType.typeName)
    val struct = df.schema("nested").dataType.asInstanceOf[StructType]
    assertTrue(struct.fieldNames.contains("what"))
    assertEquals("string", struct("what").dataType.typeName)
        
    val head = df.head
    assertEquals(5l, head(0))
    assertEquals("now", head.getStruct(1)(0))
  }

  @Test
  def testDoubleNestedArray() {
    val json = """{"foo" : [5,6], "nested": { "bar" : [{"date":"2015-01-01", "scores":[1,2]},{"date":"2015-01-01", "scores":[3,4]}], "what": "now" } }"""
    val index = wrapIndex("sparksql-test/double-nested-array")
    sc.makeRDD(Seq(json)).saveJsonToEs(index)
    val df = sqc.read.format("es").option(ES_READ_FIELD_AS_ARRAY_INCLUDE, "nested.bar,foo,nested.bar.scores").load(index)

    assertEquals("array", df.schema("foo").dataType.typeName)
    val bar = df.schema("nested").dataType.asInstanceOf[StructType]("bar")
    assertEquals("array", bar.dataType.typeName)
    val scores = bar.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]("scores")
    assertEquals("array", scores.dataType.typeName)
    
    val head = df.head
    val foo = head.getSeq[Long](0)
    assertEquals(5, foo(0))
    assertEquals(6, foo(1))
    // nested
    val nested = head.getStruct(1)
    assertEquals("now", nested.getString(1))
    val nestedDate = nested.getSeq[Row](0)
    val nestedScores = nestedDate(0).getSeq[Long](1)
    assertEquals(2l, nestedScores(1))
  }

  @Test
  def testArrayExcludes() {
    val json = """{"foo" : 6, "nested": { "bar" : [{"date":"2015-01-01", "scores":[1,2]},{"date":"2015-01-01", "scores":[3,4]}], "what": "now" } }"""
    val index = wrapIndex("sparksql-test/nested-array-exclude")
    sc.makeRDD(Seq(json)).saveJsonToEs(index)
    val df = sqc.read.format("es").option(ES_READ_FIELD_EXCLUDE, "nested.bar").load(index)

    assertEquals("long", df.schema("foo").dataType.typeName)
    assertEquals("struct", df.schema("nested").dataType.typeName)
    val struct = df.schema("nested").dataType.asInstanceOf[StructType]
    assertTrue(struct.fieldNames.contains("what"))
    assertEquals("string", struct("what").dataType.typeName)

    val first = df.first
    assertEquals(6, first.getLong(0))
    assertEquals("now", first.getStruct(1).getString(0))
  }

  @Test
  def testMultiDepthArray() {
    val json = """{"rect":{"type":"multipoint","coordinates":[[ [50,32],[69,32],[69,50],[50,50],[50,32] ]]}}"""
    val index = wrapIndex("sparksql-test/geo")
    sc.makeRDD(Seq(json)).saveJsonToEs(index)
    val df = sqc.read.format("es").option(ES_READ_FIELD_AS_ARRAY_INCLUDE, "rect.coordinates:2").load(index)
    
    val coords = df.schema("rect").dataType.asInstanceOf[StructType]("coordinates")
    assertEquals("array", coords.dataType.typeName)
    val nested = coords.dataType.asInstanceOf[ArrayType].elementType
    assertEquals("array", nested.typeName)
    assertEquals("long", nested.asInstanceOf[ArrayType].elementType.typeName)

    val first = df.first
    val vals = first.getStruct(0).getSeq[Seq[Seq[Long]]](0)(0)(0)
    assertEquals(50, vals(0))
    assertEquals(32, vals(1))
  }

  @Test
  def testGeoPointAsLatLonString() {
    val mapping = """{ "geopoint": {
    |      "properties": {
    |        "name": {
    |          "type": "string"
    |        },
    |        "location": {
    |          "type": "geo_point",
    |          "geohash" : true,
    |          "fielddata" : {
    |            "format" : "compressed",
    |            "precision" : "1cm"
    |          }
    |        }
    |      }
    |    }
    |  }
    """.stripMargin

    val index = wrapIndex("sparksql-test-geopoint-latlonstring")
    val indexAndType = s"$index/geopoint"
    RestUtils.touch(index)
    RestUtils.putMapping(indexAndType, mapping.getBytes(StringUtils.UTF_8))

    val latLonString = """{ "name" : "Chipotle Mexican Grill", "location": "40.715, -74.011" }""".stripMargin
    sc.makeRDD(Seq(latLonString)).saveJsonToEs(indexAndType)
    
    RestUtils.refresh(index)
    
    val df = sqc.read.format("es").load(index)
 
    val dataType = df.schema("location").dataType
    assertEquals("string", dataType.typeName)

    val head = df.head()
    assertThat(head.getString(0), containsString("715, "))
    assertThat(head.getString(1), containsString("Chipotle"))
  }
  
  @Test
  def testGeoPointAsGeoHashString() {
    val mapping = """{ "geopoint": {
    |      "properties": {
    |        "name": {
    |          "type": "string"
    |        },
    |        "location": {
    |          "type": "geo_point"
    |        }
    |      }
    |    }
    |  }
    """.stripMargin

    val index = wrapIndex("sparksql-test-geopoint-geohash")
    val indexAndType = s"$index/geopoint"
    RestUtils.touch(index)
    RestUtils.putMapping(indexAndType, mapping.getBytes(StringUtils.UTF_8))

    val geohash = """{ "name": "Red Pepper Restaurant", "location": "9qh0kemfy5k3" }""".stripMargin
    sc.makeRDD(Seq(geohash)).saveJsonToEs(indexAndType)
    
    RestUtils.refresh(index)
    
    val df = sqc.read.format("es").load(index)
 
    val dataType = df.schema("location").dataType
    assertEquals("string", dataType.typeName)

    val head = df.head()
    assertThat(head.getString(0), containsString("9qh0"))
    assertThat(head.getString(1), containsString("Pepper"))
  }
    
  @Test
  def testGeoPointAsArrayOfDoubles() {
    val mapping = """{ "geopoint": {
    |      "properties": {
    |        "name": {
    |          "type": "string"
    |        },
    |        "location": {
    |          "type": "geo_point"
    |        }
    |      }
    |    }
    |  }
    """.stripMargin

    val index = wrapIndex("sparksql-test-geopoint-array")
    val indexAndType = s"$index/geopoint"
    RestUtils.touch(index)
    RestUtils.putMapping(indexAndType, mapping.getBytes(StringUtils.UTF_8))

    val arrayOfDoubles = """{ "name": "Mini Munchies Pizza", "location": [ -73.983, 40.719 ]}""".stripMargin
    sc.makeRDD(Seq(arrayOfDoubles)).saveJsonToEs(indexAndType)
    
    RestUtils.refresh(index)
    
    val df = sqc.read.format("es").load(index)

    val dataType = df.schema("location").dataType
    assertEquals("array", dataType.typeName)
    val array = dataType.asInstanceOf[ArrayType]
    assertEquals(DoubleType, array.elementType)
   
    val head = df.head()
    println(head(0))
    assertThat(head.getString(1), containsString("Mini"))
  }

  @Test
  def testGeoPointAsObject() {
    val mapping = """{ "geopoint": {
    |      "properties": {
    |        "name": {
    |          "type": "string"
    |        },
    |        "location": {
    |          "type": "geo_point"
    |        }
    |      }
    |    }
    |  }
    """.stripMargin

    val index = wrapIndex("sparksql-test-geopoint-object")
    val indexAndType = s"$index/geopoint"
    RestUtils.touch(index)
    RestUtils.putMapping(indexAndType, mapping.getBytes(StringUtils.UTF_8))

    val lonLatObject = """{ "name" : "Pala Pizza","location": {"lat":40.722, "lon":-73.989} }""".stripMargin
    sc.makeRDD(Seq(lonLatObject)).saveJsonToEs(indexAndType)
    
    RestUtils.refresh(index)
    
    val df = sqc.read.format("es").load(index)
    
    val dataType = df.schema("location").dataType
    assertEquals("struct", dataType.typeName)
    val struct = dataType.asInstanceOf[StructType]
    assertTrue(struct.fieldNames.contains("lon"))
    assertTrue(struct.fieldNames.contains("lat"))
    assertEquals("double", struct("lon").dataType.simpleString)
    assertEquals("double", struct("lat").dataType.simpleString)
    
    val head = df.head()
    println(head)
    val obj = head.getStruct(0)
    assertThat(obj.getDouble(0), is(40.722d))
    assertThat(obj.getDouble(1), is(-73.989d))
    
    assertThat(head.getString(1), containsString("Pizza"))
  }

  @Test
  def testGeoShapePoint() {
    val mapping = """{ "geoshape": {
    |      "properties": {
    |        "name": {
    |          "type": "string"
    |        },
    |        "location": {
    |          "type": "geo_shape"
    |        }
    |      }
    |    }
    |  }
    """.stripMargin

    val index = wrapIndex("sparksql-test-geoshape-point")
    val indexAndType = s"$index/geoshape"
    RestUtils.touch(index)
    RestUtils.putMapping(indexAndType, mapping.getBytes(StringUtils.UTF_8))

    val point = """{"name":"point","location":{ "type" : "point", "coordinates": [100.0, 0.0] }}""".stripMargin

    sc.makeRDD(Seq(point)).saveJsonToEs(indexAndType)
    val df = sqc.read.format("es").load(index)
 
    println(df.schema.treeString)
 
    val dataType = df.schema("location").dataType
    assertEquals("struct", dataType.typeName)

    val struct = dataType.asInstanceOf[StructType]
    assertTrue(struct.fieldNames.contains("type"))
    var coords = struct("coordinates").dataType
    assertEquals("array", coords.typeName)
    coords = coords.asInstanceOf[ArrayType].elementType
    assertEquals("double", coords.typeName)

    val head = df.head()
    val obj = head.getStruct(0)
    assertThat(obj.getString(0), is("point"))
    val array = obj.getSeq[Double](1)
    assertThat(array(0), is(100.0d))
    assertThat(array(1), is(0.0d))
  }

  @Test
  def testGeoShapeLine() {
    val mapping = """{ "geoshape": {
    |      "properties": {
    |        "name": {
    |          "type": "string"
    |        },
    |        "location": {
    |          "type": "geo_shape"
    |        }
    |      }
    |    }
    |  }
    """.stripMargin

    val index = wrapIndex("sparksql-test-geoshape-linestring")
    val indexAndType = s"$index/geoshape"
    RestUtils.touch(index)
    RestUtils.putMapping(indexAndType, mapping.getBytes(StringUtils.UTF_8))

    val line = """{"name":"line","location":{ "type": "linestring", "coordinates": [[-77.03, 38.89], [-77.00, 38.88]]} }""".stripMargin
      
    sc.makeRDD(Seq(line)).saveJsonToEs(indexAndType)
    val df = sqc.read.format("es").load(index)
 
    val dataType = df.schema("location").dataType
    assertEquals("struct", dataType.typeName)

    val struct = dataType.asInstanceOf[StructType]
    assertTrue(struct.fieldNames.contains("type"))
    var coords = struct("coordinates").dataType
    assertEquals("array", coords.typeName)
    coords = coords.asInstanceOf[ArrayType].elementType
    assertEquals("array", coords.typeName)
    assertEquals("double", coords.asInstanceOf[ArrayType].elementType.typeName)

    val head = df.head()
    val obj = head.getStruct(0)
    assertThat(obj.getString(0), is("linestring"))
    val array = obj.getSeq[Seq[Double]](1)
    assertThat(array(0)(0), is(-77.03d))
    assertThat(array(0)(1), is(38.89d))
  }

  @Test
  def testGeoShapePolygon() {
    val mapping = """{ "geoshape": {
    |      "properties": {
    |        "name": {
    |          "type": "string"
    |        },
    |        "location": {
    |          "type": "geo_shape"
    |        }
    |      }
    |    }
    |  }
    """.stripMargin

    val index = wrapIndex("sparksql-test-geoshape-poly")
    val indexAndType = s"$index/geoshape"
    RestUtils.touch(index)
    RestUtils.putMapping(indexAndType, mapping.getBytes(StringUtils.UTF_8))

    val polygon = """{"name":"polygon","location":{ "type" : "Polygon", "coordinates": [[ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ]], "crs":null, "foo":"bar" }}""".stripMargin
      
    sc.makeRDD(Seq(polygon)).saveJsonToEs(indexAndType)
    val df = sqc.read.format("es").load(index)
 
    val dataType = df.schema("location").dataType
    assertEquals("struct", dataType.typeName)

    val struct = dataType.asInstanceOf[StructType]
    assertTrue(struct.fieldNames.contains("type"))
    var coords = struct("coordinates").dataType
    // level 1
    assertEquals("array", coords.typeName)
    coords = coords.asInstanceOf[ArrayType].elementType
    // level 2
    assertEquals("array", coords.typeName)
    coords = coords.asInstanceOf[ArrayType].elementType
    // level 3
    assertEquals("double", coords.asInstanceOf[ArrayType].elementType.typeName)

    val head = df.head()
    val obj = head.getStruct(0)
    assertThat(obj.getString(0), is("Polygon"))
    val array = obj.getSeq[Seq[Seq[Double]]](1)
    assertThat(array(0)(0)(0), is(100.0d))
    assertThat(array(0)(0)(1), is(0.0d))
  }

  @Test
  def testGeoShapePointMultiPoint() {
    val mapping = """{ "geoshape": {
    |      "properties": {
    |        "name": {
    |          "type": "string"
    |        },
    |        "location": {
    |          "type": "geo_shape"
    |        }
    |      }
    |    }
    |  }
    """.stripMargin

    val index = wrapIndex("sparksql-test-geoshape-multipoint")
    val indexAndType = s"$index/geoshape"
    RestUtils.touch(index)
    RestUtils.putMapping(indexAndType, mapping.getBytes(StringUtils.UTF_8))

    val multipoint = """{"name":"multipoint","location":{ "type" : "multipoint", "coordinates": [ [100.0, 0.0], [101.0, 0.0] ] }}""".stripMargin
      
    sc.makeRDD(Seq(multipoint)).saveJsonToEs(indexAndType)
    val df = sqc.read.format("es").load(index)
 
    println(df.schema.treeString)
 
    val dataType = df.schema("location").dataType
    assertEquals("struct", dataType.typeName)

    val struct = dataType.asInstanceOf[StructType]
    assertTrue(struct.fieldNames.contains("type"))
    var coords = struct("coordinates").dataType
    assertEquals("array", coords.typeName)
    coords = coords.asInstanceOf[ArrayType].elementType
    assertEquals("array", coords.typeName)
    assertEquals("double", coords.asInstanceOf[ArrayType].elementType.typeName)
    
    val head = df.head()
    val obj = head.getStruct(0)
    assertThat(obj.getString(0), is("multipoint"))
    val array = obj.getSeq[Seq[Double]](1)
    assertThat(array(0)(0), is(100.0d))
    assertThat(array(0)(1), is(0.0d))
  }

  @Test
  def testGeoShapeMultiLine() {
    val mapping = """{ "geoshape": {
    |      "properties": {
    |        "name": {
    |          "type": "string"
    |        },
    |        "location": {
    |          "type": "geo_shape"
    |        }
    |      }
    |    }
    |  }
    """.stripMargin

    val index = wrapIndex("sparksql-test-geoshape-multiline")
    val indexAndType = s"$index/geoshape"
    RestUtils.touch(index)
    RestUtils.putMapping(indexAndType, mapping.getBytes(StringUtils.UTF_8))

    val multiline = """{"name":"multi-line","location":{ "type": "multilinestring", "coordinates":[ [[-77.0, 38.8], [-78.0, 38.8]], [[100.0, 0.0], [101.0, 1.0]] ]} }""".stripMargin
      
    sc.makeRDD(Seq(multiline)).saveJsonToEs(indexAndType)
    val df = sqc.read.format("es").load(index)
 
    println(df.schema.treeString)
 
    val dataType = df.schema("location").dataType
    assertEquals("struct", dataType.typeName)

    val struct = dataType.asInstanceOf[StructType]
    assertTrue(struct.fieldNames.contains("type"))
    var coords = struct("coordinates").dataType
    // level 1
    assertEquals("array", coords.typeName)
    coords = coords.asInstanceOf[ArrayType].elementType
    // level 2
    assertEquals("array", coords.typeName)
    coords = coords.asInstanceOf[ArrayType].elementType
    // level 3
    assertEquals("double", coords.asInstanceOf[ArrayType].elementType.typeName)

    val head = df.head()
    val obj = head.getStruct(0)
    assertThat(obj.getString(0), is("multilinestring"))
    val array = obj.getSeq[Seq[Seq[Double]]](1)
    assertThat(array(0)(0)(0), is(-77.0d))
    assertThat(array(0)(0)(1), is(38.8d))
  }
  
  @Test
  def testGeoShapeMultiPolygon() {
    val mapping = """{ "geoshape": {
    |      "properties": {
    |        "name": {
    |          "type": "string"
    |        },
    |        "location": {
    |          "type": "geo_shape"
    |        }
    |      }
    |    }
    |  }
    """.stripMargin

    val index = wrapIndex("sparksql-test-geoshape-multi-poly")
    val indexAndType = s"$index/geoshape"
    RestUtils.touch(index)
    RestUtils.putMapping(indexAndType, mapping.getBytes(StringUtils.UTF_8))

    val multipoly = """{"name":"multi-poly","location":{ "type" : "multipolygon", "coordinates": [ [[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 0.0] ]], [[[103.0, 0.0], [104.0, 0.0], [104.0, 1.0], [103.0, 0.0] ]] ]}}""".stripMargin
      
    sc.makeRDD(Seq(multipoly)).saveJsonToEs(indexAndType)
    val df = sqc.read.format("es").load(index)
 
    println(df.schema.treeString)
 
    val dataType = df.schema("location").dataType
    assertEquals("struct", dataType.typeName)

    val struct = dataType.asInstanceOf[StructType]
    assertTrue(struct.fieldNames.contains("type"))
    var coords = struct("coordinates").dataType
    // level 1
    assertEquals("array", coords.typeName)
    coords = coords.asInstanceOf[ArrayType].elementType
    // level 2
    assertEquals("array", coords.typeName)
    coords = coords.asInstanceOf[ArrayType].elementType
    // level 3
    assertEquals("array", coords.typeName)
    coords = coords.asInstanceOf[ArrayType].elementType
    // level 4
    assertEquals("double", coords.asInstanceOf[ArrayType].elementType.typeName)

    val head = df.head()
    val obj = head.getStruct(0)
    assertThat(obj.getString(0), is("multipolygon"))
    val array = obj.getSeq[Seq[Seq[Seq[Double]]]](1)
    assertThat(array(0)(0)(0)(0), is(100.0d))
    assertThat(array(0)(0)(0)(1), is(0.0d))
  }
          
  @Test
  def testGeoShapeEnvelope() {
    val mapping = """{ "geoshape": {
    |      "properties": {
    |        "name": {
    |          "type": "string"
    |        },
    |        "location": {
    |          "type": "geo_shape"
    |        }
    |      }
    |    }
    |  }
    """.stripMargin

    val index = wrapIndex("sparksql-test-geoshape-envelope")
    val indexAndType = s"$index/geoshape"
    RestUtils.touch(index)
    RestUtils.putMapping(indexAndType, mapping.getBytes(StringUtils.UTF_8))

    val envelope = """{"name":"envelope","location":{ "type" : "envelope", "coordinates": [[-45.0, 45.0], [45.0, -45.0] ] }}""".stripMargin
      
    sc.makeRDD(Seq(envelope)).saveJsonToEs(indexAndType)
    val df = sqc.read.format("es").load(index)
 
    val dataType = df.schema("location").dataType
    assertEquals("struct", dataType.typeName)

    val struct = dataType.asInstanceOf[StructType]
    assertTrue(struct.fieldNames.contains("type"))
    var coords = struct("coordinates").dataType
    assertEquals("array", coords.typeName)
    coords = coords.asInstanceOf[ArrayType].elementType
    assertEquals("array", coords.typeName)
    assertEquals("double", coords.asInstanceOf[ArrayType].elementType.typeName)

    val head = df.head()
    val obj = head.getStruct(0)
    assertThat(obj.getString(0), is("envelope"))
    val array = obj.getSeq[Seq[Double]](1)
    assertThat(array(0)(0), is(-45.0d))
    assertThat(array(0)(1), is(45.0d))
  }
  
  @Test
  def testGeoShapeCircle() {
    val mapping = """{ "geoshape": {
    |      "properties": {
    |        "name": {
    |          "type": "string"
    |        },
    |        "location": {
    |          "type": "geo_shape"
    |        }
    |      }
    |    }
    |  }
    """.stripMargin

    val index = wrapIndex("sparksql-test-geoshape-circle")
    val indexAndType = s"$index/geoshape"
    RestUtils.touch(index)
    RestUtils.putMapping(indexAndType, mapping.getBytes(StringUtils.UTF_8))

    val circle = """{"name":"circle", "location": {"type":"circle", "coordinates":[ -45.0, 45.0], "radius":"100m"} }""".stripMargin
      
    sc.makeRDD(Seq(circle)).saveJsonToEs(indexAndType)
    val df = sqc.read.format("es").load(index)
 
    val dataType = df.schema("location").dataType
    assertEquals("struct", dataType.typeName)

    val struct = dataType.asInstanceOf[StructType]
    assertTrue(struct.fieldNames.contains("type"))
    assertTrue(struct.fieldNames.contains("radius"))
    val coords = struct("coordinates").dataType
    assertEquals("array", coords.typeName)
    assertEquals("double", coords.asInstanceOf[ArrayType].elementType.typeName)

    val head = df.head()
    val obj = head.getStruct(0)
    assertThat(obj.getString(0), is("circle"))
    val array = obj.getSeq[Double](1)
    assertThat(array(0), is(-45.0d))
    assertThat(array(1), is(45.0d))
    assertThat(obj.getString(2), is("100m"))
  }

  @Test
  def testNested() {
    val mapping = """{ "nested": {
    |      "properties": {
    |        "name": { "type": "string" },
    |        "employees": {
    |          "type": "nested",
    |          "properties": {
    |            "name": {"type": "string"},
    |            "salary": {"type": "long"}
    |          }
    |        }
    |      }
    |    }
    |  }
    """.stripMargin

    val index = wrapIndex("sparksql-test-nested-simple")
    val indexAndType = s"$index/nested"
    RestUtils.touch(index)
    RestUtils.putMapping(indexAndType, mapping.getBytes(StringUtils.UTF_8))

    val data = """{"name":"nested-simple","employees":[{"name":"anne","salary":6},{"name":"bob","salary":100}, {"name":"charlie","salary":15}] }""".stripMargin
      
    sc.makeRDD(Seq(data)).saveJsonToEs(indexAndType)
    val df = sqc.read.format("es").load(index)

    println(df.schema.treeString)
    
    val dataType = df.schema("employees").dataType
    assertEquals("array", dataType.typeName)
    val array = dataType.asInstanceOf[ArrayType]
    assertEquals("struct", array.elementType.typeName)
    val struct = array.elementType.asInstanceOf[StructType]
    assertEquals("string", struct("name").dataType.typeName)
    assertEquals("long", struct("salary").dataType.typeName)

    val head = df.head()
    val nested = head.getSeq[Row](0);
    assertThat(nested.size, is(3))
    assertEquals(nested(0).getString(0), "anne")
    assertEquals(nested(0).getLong(1), 6)
  }

  
  @Test
  def testMultiIndexes() {
    // add some data
    val jsonDoc = """{"artist" : "buckethead", "album": "mirror realms" }"""
    val index1 = wrapIndex("sparksql-multi-index-1/doc")
    val index2 = wrapIndex("sparksql-multi-index-2/doc")
    sc.makeRDD(Seq(jsonDoc)).saveJsonToEs(index1)
    sc.makeRDD(Seq(jsonDoc)).saveJsonToEs(index2)
    RestUtils.refresh(wrapIndex("sparksql-multi-index-1"))
    RestUtils.refresh(wrapIndex("sparksql-multi-index-2"))
    val multiIndex = wrapIndex("sparksql-multi-index-1,") + index2
    val df = sqc.read.format("es").load(multiIndex)
    df.show
    println(df.selectExpr("count(*)").show(5))
    assertEquals(2, df.count())
  }

  def wrapIndex(index: String) = {
    prefix + index
  }

  private def keepHandledFilters = {
    !pushDown || (pushDown && doubleFiltering)
  }
}