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

import org.elasticsearch.hadoop.serialization.json.JacksonJsonGenerator
import org.junit.Assert._
import org.junit.Test
import org.junit.Assert._
import org.hamcrest.Matchers._
import java.io.ByteArrayOutputStream

class ScalaValueWriterTest {

  private def serialize(value: AnyRef): String = {
    val out = new ByteArrayOutputStream()
    val generator = new JacksonJsonGenerator(out)

    val writer = new ScalaValueWriter()
    writer.write(value, generator)
    generator.flush()

    new String(out.toByteArray)
  }

  case class SimpleCaseClass(s: String)

  @Test
  def testSimpleMap() {
    assertEquals("""{"a":"b"}""", serialize(Map("a" -> "b")))
  }

  @Test
  def testPrimitiveArray() {
    assertEquals("""[1,2,3]""", serialize(Array(1,2,3)))
  }

  @Test
  def testPrimitiveSeq() {
    assertEquals("""[1,2,3]""", serialize(Seq(1,2,3)))
  }

  @Test
  def testMapInArray() {
    assertEquals("""[{"a":"b"}]""", serialize(Array(Map("a" -> "b"))))
  }

  @Test
  def testMapInSeq() {
    assertEquals("""[{"a":"b"}]""", serialize(Seq(Map("a" -> "b"))))
  }

  @Test
  def testCaseClass(){
    assertEquals("""{"s":"foo"}""", serialize(SimpleCaseClass("foo")))
  }

  @Test
  def testNestedMap(){
    assertEquals("""{"p":{"s":"bar"}}""", serialize(Map("p" -> SimpleCaseClass("bar"))))
  }

}
