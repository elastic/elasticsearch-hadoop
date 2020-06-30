package org.elasticsearch.spark.integration

import java.io.File
import java.nio.file.Files
import java.sql.Timestamp
import java.util.concurrent.TimeUnit
import java.{lang => jl}
import java.{util => ju}
import javax.xml.bind.DatatypeConverter

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.Decimal
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException
import org.elasticsearch.hadoop.EsHadoopIllegalStateException
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_INDEX_AUTO_CREATE
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_MAPPING_EXCLUDE
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_MAPPING_ID
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_SPARK_DATAFRAME_WRITE_NULL_VALUES
import org.elasticsearch.hadoop.mr.RestUtils
import org.elasticsearch.hadoop.mr.{EsAssume => EsAssume}
import org.elasticsearch.hadoop.serialization.EsHadoopSerializationException
import org.elasticsearch.hadoop.util.EsMajorVersion
import org.elasticsearch.hadoop.util.StringUtils
import org.elasticsearch.hadoop.util.TestSettings
import org.elasticsearch.hadoop.util.TestUtils
import org.elasticsearch.spark.sql.streaming.SparkSqlStreamingConfigs
import org.elasticsearch.spark.sql.streaming.StreamingQueryTestHarness
import org.hamcrest.Matchers.containsString
import org.hamcrest.Matchers.is
import org.hamcrest.Matchers.not
import org.junit.AfterClass
import org.junit.Assert
import org.junit.Assert.assertThat
import org.junit.Assert.assertTrue
import org.junit.BeforeClass
import org.junit.FixMethodOrder
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.MethodSorters
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters

import scala.collection.JavaConversions.propertiesAsScalaMap
import scala.io.Codec
import scala.io.Source

object AbstractScalaEsSparkStructuredStreaming {

  @transient val appName: String = "es-spark-sql-streaming-test"
  @transient var spark: Option[SparkSession] = None
  @transient val commitLogDir: String = commitLogDirectory()
  @transient val sparkConf: SparkConf = new SparkConf()
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .setMaster("local")
    .setAppName(appName)
    .set("spark.executor.extraJavaOptions", "-XX:MaxPermSize=256m")
    .setJars(SparkUtils.ES_SPARK_TESTING_JAR)

  @BeforeClass
  def setup(): Unit =  {
    sparkConf.setAll(TestSettings.TESTING_PROPS)
    spark = Some(
      SparkSession.builder()
        .config(sparkConf)
        .getOrCreate()
    )
  }

  def commitLogDirectory(): String = {
    val tempDir = File.createTempFile("es-spark-structured-streaming", "")
    tempDir.delete()
    tempDir.mkdir()
    val logDir = new File(tempDir, "logs")
    logDir.mkdir()
    logDir.getAbsolutePath
  }

  @AfterClass
  def cleanup(): Unit =  {
    spark.foreach((s: SparkSession) => {
      s.close()
      Thread.sleep(TimeUnit.SECONDS.toMillis(3))
    })
  }

  @Parameters
  def testParams(): ju.Collection[Array[jl.Object]] = {
    val list = new ju.ArrayList[Array[jl.Object]]()
    list.add(Array("default", jl.Boolean.FALSE))
    list
  }
}

object Products extends Serializable {
  // For sending straight strings
  case class Text(data: String)

  // Basic tuple pair
  case class Record(id: Int, name: String)

  // Meant to model the sampleArtistsDatUri
  case class WrappingRichData(id: Int, name: String, url: String, pictures: String, time: Timestamp, nested: RichData)
  case class RichData(id: Int, name: String, url: String, pictures: String, time: Timestamp)

  // Decimal data holder
  case class DecimalData(decimal: Decimal)
}

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(classOf[Parameterized])
class AbstractScalaEsSparkStructuredStreaming(prefix: String, something: Boolean) {

  private val tempFolderRule = new TemporaryFolder

  @Rule
  def tempFolder: TemporaryFolder = tempFolderRule

  val spark: SparkSession = AbstractScalaEsSparkStructuredStreaming.spark
    .getOrElse(throw new EsHadoopIllegalStateException("Spark not started..."))

  import org.elasticsearch.spark.integration.Products._
  import spark.implicits._

  def wrapIndex(name: String): String = {
    prefix + "-spark-struct-stream-" + name
  }

  def checkpoint(target: String): String = {
    s"${AbstractScalaEsSparkStructuredStreaming.commitLogDir}/$target"
  }

  def checkpointDir(target: String): String = {
    checkpoint(target)+"/sinks/elasticsearch"
  }

  @Test
  def test0Framework(): Unit = {
    val test = new StreamingQueryTestHarness[Record](spark)

    test.withInput(Record(1, "Spark"))
        .runTest {
          test.stream
            .map(_.name)
            .flatMap(_.split(" "))
            .writeStream
            .format("console")
            .start()
        }
  }

  @Test
  def test0FrameworkFailure(): Unit = {
    val test = new StreamingQueryTestHarness[Record](spark)

    test.withInput(Record(1, "Spark"))
        .expectingToThrow(classOf[StringIndexOutOfBoundsException])
        .runTest {
          test.stream
            .map(_.name)
            .flatMap(_.split(" "))
            .map(_.charAt(-4).toString)
            .writeStream
            .format("console")
            .start()
        }
  }

  @Test(expected = classOf[EsHadoopIllegalArgumentException])
  def test1FailOnIncorrectSaveCall(): Unit = {
    import org.elasticsearch.spark.sql._
    val target = wrapIndex("failed-on-save-call/data")
    val test = new StreamingQueryTestHarness[Record](spark)

    test.stream.saveToEs(target)

    Assert.fail("Should not launch job with saveToEs() method")
  }

  @Test(expected = classOf[EsHadoopIllegalArgumentException])
  def test1FailOnCompleteMode(): Unit = {
    val target = wrapIndex("failed-on-complete-mode/data")
    val test = new StreamingQueryTestHarness[Record](spark)

    test.withInput(Record(1, "Spark"))
        .runTest {
          test.stream
            .select("name").groupBy("name").count()
            .writeStream
            .outputMode(OutputMode.Complete())
            .option("checkpointLocation", checkpoint(target))
            .format("es")
            .start(target)
        }

    Assert.fail("Should not launch job with Complete mode specified")
  }

  @Test(expected = classOf[EsHadoopIllegalArgumentException])
  def test1FailOnPartitions(): Unit = {
    val target = wrapIndex("failed-on-partitions/data")
    val test = new StreamingQueryTestHarness[Record](spark)

    test.withInput(Record(1, "Spark"))
      .runTest {
        test.stream
          .writeStream
          .partitionBy("name")
          .option("checkpointLocation", checkpoint(target))
          .format("es")
          .start(target)
      }

    Assert.fail("Should not launch job with column partition")
  }

  @Test
  def test2BasicWriteWithoutCommitLog(): Unit = {
    val target = wrapIndex("test-basic-write-no-commit/data")
    val test = new StreamingQueryTestHarness[Record](spark)

    test.withInput(Record(1, "Spark"))
      .withInput(Record(2, "Hadoop"))
      .withInput(Record(3, "YARN"))
      .runTest {
        test.stream
          .writeStream
          .option(SparkSqlStreamingConfigs.ES_SINK_LOG_ENABLE, "false")
          .option("checkpointLocation", checkpoint(target))
          .format("es")
          .start(target)
      }

    assertTrue(RestUtils.exists(target))
    val searchResult = RestUtils.get(target + "/_search?")
    assertThat(searchResult, containsString("Spark"))
    assertThat(searchResult, containsString("Hadoop"))
    assertThat(searchResult, containsString("YARN"))

    assertThat(new File(s"${checkpointDir(target)}/0").exists(), not(true))
  }

  @Test
  def test2BasicWrite(): Unit = {
    val target = wrapIndex("test-basic-write/data")
    val test = new StreamingQueryTestHarness[Record](spark)

    test.withInput(Record(1, "Spark"))
        .withInput(Record(2, "Hadoop"))
        .withInput(Record(3, "YARN"))
        .runTest {
          test.stream
              .writeStream
              .option("checkpointLocation", checkpoint(target))
              .format("es")
              .start(target)
        }

    assertTrue(RestUtils.exists(target))
    val searchResult = RestUtils.get(target + "/_search?")
    assertThat(searchResult, containsString("Spark"))
    assertThat(searchResult, containsString("Hadoop"))
    assertThat(searchResult, containsString("YARN"))

    Source.fromFile(s"${checkpointDir(target)}/0").getLines().foreach(println)
  }

  @Test
  def test2BasicWriteUsingSessionCommitLog(): Unit = {
    try {
      val check = s"${AbstractScalaEsSparkStructuredStreaming.commitLogDir}/session1"
      spark.conf.set(SQLConf.CHECKPOINT_LOCATION.key, check)

      val target = wrapIndex("test-basic-write/data")
      val test = new StreamingQueryTestHarness[Record](spark)

      test.withInput(Record(1, "Spark"))
        .withInput(Record(2, "Hadoop"))
        .withInput(Record(3, "YARN"))
        .runTest {
          test.stream
            .writeStream
            .queryName("test-basic-write-session-commit")
            .format("es")
            .start(target)
        }

      assertTrue(RestUtils.exists(target))
      val searchResult = RestUtils.get(target + "/_search?")
      assertThat(searchResult, containsString("Spark"))
      assertThat(searchResult, containsString("Hadoop"))
      assertThat(searchResult, containsString("YARN"))

      Source.fromFile(s"${checkpointDir(target)}/0").getLines().foreach(println)

      assertThat(Files.exists(new File(s"$check/test-basic-write-session-commit/sinks/elasticsearch/0").toPath), is(true))
    } finally {
      spark.conf.unset(SQLConf.CHECKPOINT_LOCATION.key)
    }
  }

  @Test
  def test2BasicWriteUsingSessionCommitLogNoQueryName(): Unit = {
    try {
      val check = s"${AbstractScalaEsSparkStructuredStreaming.commitLogDir}/session2"
      spark.conf.set(SQLConf.CHECKPOINT_LOCATION.key, check)

      val target = wrapIndex("test-basic-write/data")
      val test = new StreamingQueryTestHarness[Record](spark)

      test.withInput(Record(1, "Spark"))
        .withInput(Record(2, "Hadoop"))
        .withInput(Record(3, "YARN"))
        .runTest {
          test.stream
            .writeStream
            .format("es")
            .start(target)
        }

      assertTrue(RestUtils.exists(target))
      val searchResult = RestUtils.get(target + "/_search?")
      assertThat(searchResult, containsString("Spark"))
      assertThat(searchResult, containsString("Hadoop"))
      assertThat(searchResult, containsString("YARN"))

      Source.fromFile(s"${checkpointDir(target)}/0").getLines().foreach(println)

      assertThat(Files.exists(new File(check).toPath), is(true))
      assertThat(Files.list(new File(check).toPath).count(), is(2L)) // A UUID for general checkpoint, and one for ES.
    } finally {
      spark.conf.unset(SQLConf.CHECKPOINT_LOCATION.key)
    }
  }

  @Test(expected = classOf[EsHadoopIllegalArgumentException])
  def test1FailOnIndexCreationDisabled(): Unit = {
    val target = wrapIndex("test-write-index-create-disabled/data")
    val test = new StreamingQueryTestHarness[Record](spark)

    test.withInput(Record(1, "Spark"))
      .withInput(Record(2, "Hadoop"))
      .withInput(Record(3, "YARN"))
      .runTest {
        test.stream
          .writeStream
          .option("checkpointLocation", checkpoint(target))
          .option(ES_INDEX_AUTO_CREATE, "no")
          .format("es")
          .start(target)
      }

    assertTrue("Index already exists! Index should not exist prior to this test!", !RestUtils.exists(target))
    Assert.fail("Should not be able to write to index if not already created.")
  }

  @Test
  def test2WriteWithMappingId(): Unit = {
    val target = wrapIndex("test-write-with-id/data")
    val test = new StreamingQueryTestHarness[Record](spark)

    test.withInput(Record(1, "Spark"))
      .withInput(Record(2, "Hadoop"))
      .withInput(Record(3, "YARN"))
      .runTest {
        test.stream
          .writeStream
          .option("checkpointLocation", checkpoint(target))
          .option(ES_MAPPING_ID, "id")
          .format("es")
          .start(target)
      }

    assertTrue(RestUtils.exists(target))
    assertTrue(RestUtils.exists(target + "/1"))
    assertTrue(RestUtils.exists(target + "/2"))
    assertTrue(RestUtils.exists(target + "/3"))
    val searchResult = RestUtils.get(target + "/_search?")
    assertThat(searchResult, containsString("Spark"))
    assertThat(searchResult, containsString("Hadoop"))
    assertThat(searchResult, containsString("YARN"))
  }

  @Test
  def test2WriteWithMappingExclude(): Unit = {
    val target = wrapIndex("test-write-with-exclude/data")
    val test = new StreamingQueryTestHarness[Record](spark)

    test.withInput(Record(1, "Spark"))
      .withInput(Record(2, "Hadoop"))
      .withInput(Record(3, "YARN"))
      .runTest {
        test.stream
          .writeStream
          .option("checkpointLocation", checkpoint(target))
          .option(ES_MAPPING_EXCLUDE, "id")
          .format("es")
          .start(target)
      }

    assertTrue(RestUtils.exists(target))
    val searchResult = RestUtils.get(target + "/_search?")
    assertThat(searchResult, containsString("Spark"))
    assertThat(searchResult, containsString("Hadoop"))
    assertThat(searchResult, containsString("YARN"))
    assertThat(searchResult, not(containsString(""""id":1""")))
  }

  @Test
  def test2WriteToIngestPipeline(): Unit = {
    EsAssume.versionOnOrAfter(EsMajorVersion.V_5_X, "Ingest Supported in 5.x and above only")

    val pipelineName: String = prefix + "-pipeline"
    val pipeline: String = """{"description":"Test Pipeline","processors":[{"set":{"field":"pipeTEST","value":true,"override":true}}]}"""
    RestUtils.put("/_ingest/pipeline/" + pipelineName, StringUtils.toUTF(pipeline))

    val target = wrapIndex("test-write-ingest/data")
    val test = new StreamingQueryTestHarness[Record](spark)

    test.withInput(Record(1, "Spark"))
      .withInput(Record(2, "Hadoop"))
      .withInput(Record(3, "YARN"))
      .runTest {
        test.stream
          .writeStream
          .option("checkpointLocation", checkpoint(target))
          .option(ConfigurationOptions.ES_INGEST_PIPELINE, pipelineName)
          .format("es")
          .start(target)
      }

    assertTrue(RestUtils.exists(target))
    val searchResult = RestUtils.get(target + "/_search?")
    assertThat(searchResult, containsString("Spark"))
    assertThat(searchResult, containsString("Hadoop"))
    assertThat(searchResult, containsString("YARN"))
    assertThat(searchResult, containsString(""""pipeTEST":true"""))
  }

  @Test
  def test2MultiIndexWrite(): Unit = {
    val target = wrapIndex("test-tech-{name}/data")
    val test = new StreamingQueryTestHarness[Record](spark)

    test.withInput(Record(1, "spark"))
      .withInput(Record(2, "hadoop"))
      .runTest {
        test.stream
          .writeStream
          .option("checkpointLocation", checkpoint(target))
          .format("es")
          .start(target)
      }

    assertTrue(RestUtils.exists(wrapIndex("test-tech-spark/data")))
    assertTrue(RestUtils.exists(wrapIndex("test-tech-hadoop/data")))
    assertThat(wrapIndex("test-tech-spark/data/_search?"), containsString("spark"))
    assertThat(wrapIndex("test-tech-hadoop/data/_search?"), containsString("hadoop"))
  }

  @Test
  def test2NullValueIgnored() {
    val target = wrapIndex("test-null-data-absent/data")
    val test = new StreamingQueryTestHarness[Record](spark)

    test.withInput(Record(1, "Spark"))
      .withInput(Record(2, null))
      .runTest {
        test.stream
          .writeStream
          .option("checkpointLocation", checkpoint(target))
          .option(ES_MAPPING_ID, "id")
          .format("es")
          .start(target)
      }

    assertTrue(RestUtils.exists(target))
    assertThat(RestUtils.get(target + "/1"), containsString("name"))
    assertThat(RestUtils.get(target + "/2"), not(containsString("name")))
  }

  @Test
  def test2NullValueWritten() {
    val target = wrapIndex("test-null-data-null/data")
    val test = new StreamingQueryTestHarness[Record](spark)

    test.withInput(Record(1, "Spark"))
      .withInput(Record(2, null))
      .runTest {
        test.stream
          .writeStream
          .option("checkpointLocation", checkpoint(target))
          .option(ES_MAPPING_ID, "id")
          .option(ES_SPARK_DATAFRAME_WRITE_NULL_VALUES, "true")
          .format("es")
          .start(target)
      }

    assertTrue(RestUtils.exists(target))
    assertThat(RestUtils.get(target + "/1"), containsString("name"))
    assertThat(RestUtils.get(target + "/2"), containsString("name"))
  }

  @Test
  def test2WriteWithRichMapping() {
    val target = wrapIndex("test-basic-write-rich-mapping-id/data")
    val test = new StreamingQueryTestHarness[Text](spark)

    Source.fromURI(TestUtils.sampleArtistsDatUri())(Codec.ISO8859).getLines().foreach(s => test.withInput(Text(s)))

    test
      .runTest {
        test.stream
          .map(_.data.split("\t"))
          .map(a => {
            val id = a(0).toInt
            val name = a(1)
            val url = a(2)
            val pictures = a(3)
            val time = new Timestamp(DatatypeConverter.parseDateTime(a(4)).getTimeInMillis)
            WrappingRichData(id, name, url, pictures, time, RichData(id, name, url, pictures, time))
          })
          .writeStream
          .option("checkpointLocation", checkpoint(target))
          .option(ES_MAPPING_ID, "id")
          .format("es")
          .start(target)
      }

    assertTrue(RestUtils.exists(target))
    assertThat(RestUtils.get(target + "/_search?"), containsString("345"))
    assertThat(RestUtils.exists(target+"/1"), is(true))
  }

  @Test
  def test1FailOnDecimalType() {
    val target = wrapIndex("test-decimal-exception/data")
    val test = new StreamingQueryTestHarness[DecimalData](spark)

    test.withInput(DecimalData(Decimal(10)))
      .expectingToThrow(classOf[EsHadoopSerializationException])
      .runTest {
        test.stream
          .writeStream
          .option("checkpointLocation", checkpoint(target))
          .format("es")
          .start(target)
      }
  }
}
