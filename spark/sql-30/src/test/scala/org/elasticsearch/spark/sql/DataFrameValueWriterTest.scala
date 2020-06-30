package org.elasticsearch.spark.sql

import java.io.ByteArrayOutputStream

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.MapType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.hadoop.cfg.Settings
import org.elasticsearch.hadoop.serialization.EsHadoopSerializationException
import org.elasticsearch.hadoop.serialization.json.JacksonJsonGenerator
import org.elasticsearch.hadoop.util.TestSettings
import org.junit.Assert._
import org.junit.Ignore
import org.junit.Test

class DataFrameValueWriterTest {

  private def serialize(value: Row, schema: StructType): String = serialize(value, schema, null)

  private def serialize(value: Row, schema: StructType, settings: Settings): String = {
    val out = new ByteArrayOutputStream()
    val generator = new JacksonJsonGenerator(out)

    val writer = new DataFrameValueWriter()
    if (settings != null) {
      writer.setSettings(settings)
    }
    val result = writer.write((value, schema), generator)
    if (result.isSuccesful == false) {
      throw new EsHadoopSerializationException("Could not serialize [" + result.getUnknownValue + "]")
    }
    generator.flush()

    new String(out.toByteArray)
  }

  case class SimpleCaseClass(s: String)
  class Garbage(i: Int) {
    def doNothing(): Unit = ()
  }

  @Test
  def testSimpleRow(): Unit = {
    val schema = StructType(Seq(StructField("a", StringType)))
    val row = Row("b")
    assertEquals("""{"a":"b"}""", serialize(row, schema))
  }

  @Test
  def testPrimitiveArray(): Unit = {
    val schema = StructType(Seq(StructField("a", ArrayType(IntegerType))))
    val row = Row(Array(1,2,3))
    assertEquals("""{"a":[1,2,3]}""", serialize(row, schema))
  }

  @Test
  def testPrimitiveSeq(): Unit = {
    val schema = StructType(Seq(StructField("a", ArrayType(IntegerType))))
    val row = Row(Seq(1,2,3))
    assertEquals("""{"a":[1,2,3]}""", serialize(row, schema))
  }

  @Test
  def testMapInArray(): Unit = {
    val schema = StructType(Seq(StructField("s", ArrayType(MapType(StringType, StringType)))))
    val row = Row(Array(Map("a" -> "b")))
    assertEquals("""{"s":[{"a":"b"}]}""", serialize(row, schema))
  }

  @Test
  def testMapInSeq(): Unit = {
    val schema = StructType(Seq(StructField("s", ArrayType(MapType(StringType, StringType)))))
    val row = Row(Seq(Map("a" -> "b")))
    assertEquals("""{"s":[{"a":"b"}]}""", serialize(row, schema))
  }

  @Test
  @Ignore("SparkSQL uses encoders internally to convert a case class into a Row object. We wont ever see this case.")
  def testCaseClass(): Unit = {
    val schema = StructType(Seq(StructField("a", ScalaReflection.schemaFor[SimpleCaseClass].dataType.asInstanceOf[StructType])))
    val row = Row(SimpleCaseClass("foo"))
    assertEquals("""{"a":{"s":"foo"}}""", serialize(row, schema))
  }

  @Test
  def testIgnoreScalaToJavaToScalaFieldExclusion(): Unit = {
    val settings = new TestSettings()
    settings.setProperty(ConfigurationOptions.ES_MAPPING_EXCLUDE, "skey.ignoreme")

    val schema = StructType(Seq(StructField("skey", StructType(Seq(StructField("jkey", StringType), StructField("ignoreme", StringType))))))
    val row = Row(Row("value", "value"))

    val serialized = serialize(row, schema, settings)
    println(serialized)
    assertEquals("""{"skey":{"jkey":"value"}}""", serialized)
  }

}