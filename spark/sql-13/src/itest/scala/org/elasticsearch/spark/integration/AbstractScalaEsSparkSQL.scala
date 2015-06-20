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
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.storage.StorageLevel._
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
import org.elasticsearch.hadoop.mr.RestUtils
import org.elasticsearch.hadoop.util.StringUtils
import org.elasticsearch.hadoop.util.TestSettings
import org.elasticsearch.hadoop.util.TestUtils
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
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
import org.junit.runners.MethodSorters
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters

import com.esotericsoftware.kryo.io.{Input => KryoInput}
import com.esotericsoftware.kryo.io.{Output => KryoOutput}

import javax.xml.bind.DatatypeConverter

object AbstractScalaEsScalaSparkSQL {
  @transient val conf = new SparkConf().setAll(TestSettings.TESTING_PROPS).set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .setMaster("local").setAppName("estest").set("spark.executor.extraJavaOptions", "-XX:MaxPermSize=256m")
    .setJars(SparkUtils.ES_SPARK_TESTING_JAR)
  //.setMaster("local")
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

  @Parameters
  def testParams(): ju.Collection[Array[jl.Object]] = {
    val list = new ju.ArrayList[Array[jl.Object]]()
    // no query
    val noQuery = ""
    list.add(Array("default", jl.Boolean.FALSE, jl.Boolean.TRUE, jl.Boolean.FALSE, noQuery))
    list.add(Array("defaultstrict", jl.Boolean.FALSE, jl.Boolean.TRUE, jl.Boolean.TRUE, noQuery))
    list.add(Array("defaultnopush", jl.Boolean.FALSE, jl.Boolean.FALSE, jl.Boolean.FALSE, noQuery))
    list.add(Array("withmeta", jl.Boolean.TRUE, jl.Boolean.TRUE, jl.Boolean.FALSE, noQuery))
    list.add(Array("withmetastrict", jl.Boolean.TRUE, jl.Boolean.TRUE, jl.Boolean.TRUE, noQuery))
    list.add(Array("withmetanopush", jl.Boolean.TRUE, jl.Boolean.FALSE, jl.Boolean.FALSE, noQuery))

    // uri query
    val uriQuery = "?q=*"
    list.add(Array("defaulturiquery", jl.Boolean.FALSE, jl.Boolean.TRUE, jl.Boolean.FALSE, uriQuery))
    list.add(Array("defaulturiquerystrict", jl.Boolean.FALSE, jl.Boolean.TRUE, jl.Boolean.TRUE, uriQuery))
    list.add(Array("defaulturiquerynopush", jl.Boolean.FALSE, jl.Boolean.FALSE, jl.Boolean.FALSE, uriQuery))
    list.add(Array("withmetauri_query", jl.Boolean.TRUE, jl.Boolean.TRUE, jl.Boolean.FALSE, uriQuery))
    list.add(Array("withmetauri_querystrict", jl.Boolean.TRUE, jl.Boolean.TRUE, jl.Boolean.TRUE, uriQuery))
    list.add(Array("withmetauri_querynopush", jl.Boolean.TRUE, jl.Boolean.FALSE, jl.Boolean.FALSE, uriQuery))

    // dsl query
    val dslQuery = """ {"query" : { "match_all" : { } } } """
    list.add(Array("defaultdslquery", jl.Boolean.FALSE, jl.Boolean.TRUE, jl.Boolean.FALSE, dslQuery))
    list.add(Array("defaultstrictdslquery", jl.Boolean.FALSE, jl.Boolean.TRUE, jl.Boolean.TRUE, dslQuery))
    list.add(Array("defaultnopushdslquery", jl.Boolean.FALSE, jl.Boolean.FALSE, jl.Boolean.FALSE, dslQuery))
    list.add(Array("withmetadslquery", jl.Boolean.TRUE, jl.Boolean.TRUE, jl.Boolean.FALSE, dslQuery))
    list.add(Array("withmetastrictdslquery", jl.Boolean.TRUE, jl.Boolean.TRUE, jl.Boolean.TRUE, dslQuery))
    list.add(Array("withmetanopushdslquery", jl.Boolean.TRUE, jl.Boolean.FALSE, jl.Boolean.FALSE, dslQuery))

    list
  }
}

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(classOf[Parameterized])
class AbstractScalaEsScalaSparkSQL(prefix: String, readMetadata: jl.Boolean, pushDown: jl.Boolean, strictPushDown: jl.Boolean, query: String = "") extends Serializable {

  val sc = AbstractScalaEsScalaSparkSQL.sc
  val sqc = AbstractScalaEsScalaSparkSQL.sqc
  val cfg = Map(ES_QUERY -> query,
                ES_READ_METADATA -> readMetadata.toString(),
                "es.internal.spark.sql.pushdown" -> pushDown.toString(),
                "es.internal.spark.sql.pushdown.strict" -> strictPushDown.toString() )

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
    assertTrue(dataFrame.count > 300)
    val schema = dataFrame.schema.treeString
    assertTrue(schema.contains("id: long"))
    assertTrue(schema.contains("name: string"))
    assertTrue(schema.contains("pictures: string"))
    assertTrue(schema.contains("time: long"))
    assertTrue(schema.contains("url: string"))

    //dataFrame.take(5).foreach(println)

    val tempTable = wrapIndex("basicRead")
    dataFrame.registerTempTable(tempTable)
    val nameRDD = sqc.sql(s"SELECT name FROM $tempTable WHERE id >= 1 AND id <=10")
    nameRDD.take(7).foreach(println)
    assertEquals(10, nameRDD.count)
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
    if (readMetadata) {
      options = options + """,readMetadata "true" """
    }
    val table = wrapIndex("sqlbasicread1")

    val query = s"CREATE TEMPORARY TABLE $table "+
      " USING org.elasticsearch.spark.sql " +
      s" OPTIONS ($options)"
      
    println(query)
    
    val dataFrame = sqc.sql(query)

    val dsCfg = collection.mutable.Map(cfg.toSeq: _*) += ("path" -> target)
    val dfLoad = sqc.load("org.elasticsearch.spark.sql", dsCfg.toMap)
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

    val options = s"""resource '$target' , readMetadata "true" """
    val dataFrame = sqc.sql(s"CREATE TEMPORARY TABLE $table" +
      " USING org.elasticsearch.spark.sql " +
      s" OPTIONS ($options)")

    val allRDD = sqc.sql(s"SELECT * FROM $table WHERE id >= 1 AND id <=10")
    allRDD.printSchema()
    allRDD.take(7).foreach(println)

    val dsCfg = collection.mutable.Map(cfg.toSeq: _*) += ("path" -> target)
    val dfLoad = sqc.load("org.elasticsearch.spark.sql", dsCfg.toMap)
    dfLoad.show()
  }

  @Test
  def testDataSource0Setup() {
    val target = wrapIndex("spark-test/scala-sql-varcols")
    val table = wrapIndex("sqlvarcol")

    val trip1 = Map("reason" -> "business", "airport" -> "SFO", "tag" -> "jan")
    val trip2 = Map("participants" -> 5, "airport" -> "OTP", "tag" -> "feb")
    val trip3 = Map("participants" -> 3, "airport" -> "MUC OTP SFO JFK", "tag" -> "long")

    sc.makeRDD(Seq(trip1, trip2, trip3)).saveToEs(target)
  }

  private def esDataSource(table: String) = {
    val target = wrapIndex("spark-test/scala-sql-varcols")

    var options = s"""resource "$target" """
    if (readMetadata) {
      options = options + """ ,readMetadata "true" """
    }

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
    if (strictPushDown) {
      assertEquals(0, filter.count())
      // however if we change the arguments to be lower cased, it will Spark who's going to filter out the data
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
      // however if we change the arguments to be lower cased, it will Spark who's going to filter out the data
      return
    }

    assertEquals(2, filter.count())
    assertEquals("jan", filter.select("tag").sort("tag").take(2)(1)(0))
  }

  @Test
  def testDataSourcePushDown09StartsWith() {
    val df = esDataSource("pd_starts_with")
    var filter = df.filter(df("airport").startsWith("O"))

    if (strictPushDown) {
      assertEquals(0, filter.count())
      return
    }

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

    assertEquals(2, filter.count())
    assertEquals("feb", filter.select("tag").sort("tag").take(1)(0)(0))
  }

  @Test
  def testEsSchemaFromDocsWithDifferentProperties() {
    val table = wrapIndex("sqlvarcol")
    esDataSource(table)
    
    val target = wrapIndex("spark-test/scala-sql-varcols")

    var options = s"""resource '$target' """
    if (readMetadata) {
      options = options + """ , readMetadata 'true' """
    }

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
    if (readMetadata) {
      options = options + " ,readMetadata 'true'"
    }

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
    if (readMetadata) {
      dstOptions = dstOptions + " ,readMetadata 'true'"
    }

    var srcOptions = s"resource '$index'"
    if (readMetadata) {
      srcOptions = srcOptions + " ,readMetadata 'true'"
    }

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

  def wrapIndex(index: String) = {
    prefix + index
  }
}