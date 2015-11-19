package org.elasticsearch.spark.cfg

import org.elasticsearch.spark.serialization.ReflectionUtils._
import org.junit.Test
import org.junit.Assert._
import org.hamcrest.Matchers._
import org.apache.spark.SparkConf
import org.elasticsearch.hadoop.cfg.PropertiesSettings

class SparkConfigTest {

  @Test
  def testProperties() {
    val cfg = new SparkConf().set("type", "onegative")
    val settings = new SparkSettingsManager().load(cfg)
    val props = new PropertiesSettings().load(settings.save())
    assertEquals("onegative", props.getProperty("type"))
  }
  
  @Test
  def testSparkProperties() {
    val cfg = new SparkConf().set("spark.type", "onegative")
    val settings = new SparkSettingsManager().load(cfg)
    val props = new PropertiesSettings().load(settings.save())
    assertEquals("onegative", props.getProperty("type"))
  }

  @Test
  def testSparkPropertiesOverride() {
    val cfg = new SparkConf().set("spark.type", "fail").set("type", "win")
    val settings = new SparkSettingsManager().load(cfg)
    val props = new PropertiesSettings().load(settings.save())
    assertEquals("win", props.getProperty("type"))
  }
}