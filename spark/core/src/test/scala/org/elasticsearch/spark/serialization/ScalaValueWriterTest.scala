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
package org.elasticsearch.spark.serialization

import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.hadoop.cfg.Settings
import org.elasticsearch.hadoop.serialization.EsHadoopSerializationException
import org.elasticsearch.hadoop.serialization.json.JacksonJsonGenerator
import org.elasticsearch.hadoop.util.TestSettings
import org.elasticsearch.spark.serialization.testbeans.{Contact, ContactBook}
import org.junit.Assert._
import org.junit.Test

import java.io.ByteArrayOutputStream
import java.sql.Timestamp
import java.time.ZoneId
import java.util.Date
import scala.beans.BeanProperty

class ScalaValueWriterTest {

  private def serialize(value: AnyRef): String = serialize(value, null)

  private def serialize(value: AnyRef, settings: Settings): String = {
    val out = new ByteArrayOutputStream()
    val generator = new JacksonJsonGenerator(out)

    val writer = new ScalaValueWriter()
    if (settings != null) {
      writer.setSettings(settings)
    }
    val result = writer.write(value, generator)
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

  class Node(@BeanProperty var info: String) {
    @BeanProperty
    var node: Node = _
  }

  @Test
  def testSimpleMap(): Unit = {
    assertEquals("""{"a":"b"}""", serialize(Map("a" -> "b")))
  }

  @Test
  def testPrimitiveArray(): Unit = {
    assertEquals("""[1,2,3]""", serialize(Array(1,2,3)))
  }

  @Test
  def testPrimitiveSeq(): Unit = {
    assertEquals("""[1,2,3]""", serialize(Seq(1,2,3)))
  }

  @Test
  def testMapInArray(): Unit = {
    assertEquals("""[{"a":"b"}]""", serialize(Array(Map("a" -> "b"))))
  }

  @Test
  def testMapInSeq(): Unit = {
    assertEquals("""[{"a":"b"}]""", serialize(Seq(Map("a" -> "b"))))
  }

  @Test
  def testCaseClass(): Unit = {
    assertEquals("""{"s":"foo"}""", serialize(SimpleCaseClass("foo")))
  }

  @Test
  def testNestedMap(): Unit = {
    assertEquals("""{"p":{"s":"bar"}}""", serialize(Map("p" -> SimpleCaseClass("bar"))))
  }

  @Test
  def testNestedJavaBean(): Unit = {
    val contacts = new java.util.LinkedHashMap[String, Contact]()
    contacts.put("Benny", new Contact("Benny", "Some guy"))
    contacts.put("The Jets", new Contact("The Jets", "Benny's associates"))
    assertEquals("""{"contacts":{"Benny":{"name":"Benny","relation":"Some guy"},"The Jets":{"name":"The Jets","relation":"Benny's associates"}},"owner":"me"}""", serialize(new ContactBook("me", contacts)))
  }

  @Test(expected = classOf[EsHadoopSerializationException])
  def testMapWithInvalidObject(): Unit = {
    val map = new java.util.HashMap[String, Object]()
    map.put("test", new Garbage(42))
    serialize(map)
  }

  @Test(expected = classOf[EsHadoopSerializationException])
  def testNestedUnknownValue(): Unit = {
    val map = Map("itemId" -> "1", "map" -> Map("lat" -> 1.23, "lon" -> -70.12), "list" -> ("A" -> "B" -> "C"), "unknown" -> new Garbage(0))
    serialize(map)
  }

  @Test
  def testIgnoreScalaToJavaToScalaFieldExclusion(): Unit = {
    val someValue = "value"
    val javaMap = new java.util.LinkedHashMap[String, Object]()
    javaMap.put("jkey", someValue)
    javaMap.put("ignoreme", someValue)
    val scalaMap = scala.collection.immutable.Map("skey" -> javaMap)

    val settings = new TestSettings()
    settings.setProperty(ConfigurationOptions.ES_MAPPING_EXCLUDE, "skey.ignoreme")

    val serialized = serialize(scalaMap, settings)
    println(serialized)
    assertEquals("""{"skey":{"jkey":"value"}}""", serialized)
  }

  @Test
  def testReentrantData(): Unit = {
    val node = new Node("value")
    node.node = node
    assertEquals("""{"info":"value"}""", serialize(node))
  }

  @Test(expected = classOf[EsHadoopSerializationException])
  def testRingOfData(): Unit = {
    val node1 = new Node("value1")
    val node2 = new Node("value2")

    node1.node = node2
    node2.node = node1

    println(serialize(node1))
  }

  @Test
  def testCaseClassWithNone(): Unit = {

    case class TestCaseClass(option : Option[String])
    val caseClass = TestCaseClass(None)

    assertEquals("""{}""", serialize(caseClass))
  }

  @Test
  def testCaseClassWithSome(): Unit = {

    case class TestCaseClass(option : Option[String])
    val caseClass = TestCaseClass(Some("value"))

    assertEquals("""{"option":"value"}""", serialize(caseClass))
  }


  @Test
  def testCaseClassWithSomeAndNone(): Unit = {

    case class TestCaseClass(option1 : Option[String], option2 : Option[String])
    val caseClass = TestCaseClass(None, Some("value2"))

    assertEquals("""{"option2":"value2"}""", serialize(caseClass))
  }


  @Test
  def testCaseClassWithInnerObject(): Unit = {

    case class TestCaseClass(option1 : Option[String], option2 : Option[TestCaseClassInner])
    case class TestCaseClassInner(option1 : Option[String], option2 : Option[String])
    val caseClass = TestCaseClass(None, Some(TestCaseClassInner(option1 = Some("value1") , option2 = None)))

    assertEquals("""{"option2":{"option1":"value1"}}""", serialize(caseClass))
  }

  @Test
  def testCaseClassWithInnerObjectAndNullSetting(): Unit = {

    case class TestCaseClass(option1: Option[String], option2: Option[TestCaseClassInner], option3: Any)
    case class TestCaseClassInner(option1: Option[String], option2: Option[String])
    val caseClass = TestCaseClass(None, Some(TestCaseClassInner(option1 = Some("value1"), option2 = None)), ())

    val settings = new TestSettings()
    settings.setProperty(ConfigurationOptions.ES_SPARK_DATAFRAME_WRITE_NULL_VALUES, "true")

    assertEquals("""{"option1":null,"option2":{"option1":"value1","option2":null},"option3":null}""", serialize(caseClass, settings))
  }

  @Test
  def testDate(): Unit = {
    val date = new Date(1420114230123l)
    val actual = serialize(date);
    val expected = "\"" + date.toInstant.atZone(ZoneId.systemDefault).toOffsetDateTime.toString + "\""
    assertEquals(expected, actual)
  }

  @Test
  def testDateWithNanos(): Unit = {
    val timestamp = new Timestamp(1420114230123l)
    timestamp.setNanos(123456789)
    val actual = serialize(timestamp);
    val expected = "\"" + timestamp.toInstant.atZone(ZoneId.systemDefault).toOffsetDateTime.toString + "\""
    assertEquals(expected, actual)
  }

}
