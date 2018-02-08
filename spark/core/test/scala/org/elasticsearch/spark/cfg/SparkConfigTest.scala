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
package org.elasticsearch.spark.cfg

import org.elasticsearch.spark.serialization.ReflectionUtils._
import org.junit.Test
import org.junit.Assert._
import org.hamcrest.Matchers._
import org.apache.spark.SparkConf
import org.elasticsearch.hadoop.cfg.PropertiesSettings

class SparkConfigTest {

  @Test
  def testProperties(): Unit = {
    val cfg = new SparkConf().set("type", "onegative")
    val settings = new SparkSettingsManager().load(cfg)
    val props = new PropertiesSettings().load(settings.save())
    assertEquals("onegative", props.getProperty("type"))
  }
  
  @Test
  def testSparkProperties(): Unit = {
    val cfg = new SparkConf().set("spark.type", "onegative")
    val settings = new SparkSettingsManager().load(cfg)
    val props = new PropertiesSettings().load(settings.save())
    assertEquals("onegative", props.getProperty("type"))
  }

  @Test
  def testSparkPropertiesOverride(): Unit = {
    val cfg = new SparkConf().set("spark.type", "fail").set("type", "win")
    val settings = new SparkSettingsManager().load(cfg)
    val props = new PropertiesSettings().load(settings.save())
    assertEquals("win", props.getProperty("type"))
  }
}