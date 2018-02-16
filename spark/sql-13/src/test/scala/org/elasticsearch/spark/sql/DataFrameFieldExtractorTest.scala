package org.elasticsearch.spark.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.elasticsearch.hadoop.serialization.field.ConstantFieldExtractor
import org.elasticsearch.hadoop.util.TestSettings
import org.junit.Assert._
import org.junit.Test

class DataFrameFieldExtractorTest {

  @Test
  def extractString(): Unit = {
    val settings = new TestSettings()
    settings.setProperty(ConstantFieldExtractor.PROPERTY, "test")
    val extractor = new DataFrameFieldExtractor()
    extractor.setSettings(settings)

    val data = (Row("value1", "value2", "target"), StructType(Seq(StructField("foo", StringType), StructField("bar", StringType), StructField("test", StringType))))
    val expected = "target"
    val actual = extractor.field(data)

    assertEquals(expected, actual)
  }

  @Test
  def extractNestedRow(): Unit = {
    val settings = new TestSettings()
    settings.setProperty(ConstantFieldExtractor.PROPERTY, "test")
    val extractor = new DataFrameFieldExtractor()
    extractor.setSettings(settings)

    val data = (Row("value1", "value2", Row("target")),
      StructType(Seq(
        StructField("foo", StringType),
        StructField("bar", StringType),
        StructField("test", StructType(Seq(
          StructField("test", StringType)
        )))
      )))
    val expected = Row("target")
    val actual = extractor.field(data)

    assertEquals(expected, actual)
  }

  @Test
  def extractNestedRowValue(): Unit = {
    val settings = new TestSettings()
    settings.setProperty(ConstantFieldExtractor.PROPERTY, "test.test")
    val extractor = new DataFrameFieldExtractor()
    extractor.setSettings(settings)

    val data = (Row("value1", "value2", Row("target")),
      StructType(Seq(
        StructField("foo", StringType),
        StructField("bar", StringType),
        StructField("test", StructType(Seq(
          StructField("test", StringType)
        )))
      )))
    val expected = "target"
    val actual = extractor.field(data)

    assertEquals(expected, actual)
  }
}