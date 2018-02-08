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
package org.elasticsearch.spark.sql

import java.util.{Map => JMap}

import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.StructType
import org.codehaus.jackson.map.ObjectMapper
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
import org.elasticsearch.hadoop.cfg.Settings
import org.elasticsearch.hadoop.util.TestSettings
import org.elasticsearch.spark.sql.SchemaUtils._
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import java.util.Collections

import org.elasticsearch.hadoop.serialization.dto.mapping.FieldParser

class SchemaUtilsTest {

  var cfg: Settings = null

  @Before
  def start(): Unit = {
    cfg = new TestSettings
  }

  @Test
  def testConvertToStructSimpleField(): Unit = {
    val mapping = """{
    |  "simple" : {
    |      "properties" : {
    |          "name" : {
    |              "type" : "string"
    |          }
    |      }
    |  }
    |}
    |""".stripMargin
      
    val struct = getStruct(mapping)
    assertTrue(struct.fieldNames.contains("name"))
    assertEquals(StringType, struct("name").dataType)
  }

  @Test
  def testConvertToStructWithObject(): Unit = {
    val mapping = """{ "nested-array": {
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
      
    val struct = getStruct(mapping)
    assertTrue(struct.fieldNames.contains("arr"))
    assertFalse(struct.fieldNames.contains("one"))
    val nested = struct("arr").dataType
    assertEquals("struct", nested.typeName)
    
    val arr = nested.asInstanceOf[StructType]
    assertTrue(arr.fieldNames.contains("one"))
    assertTrue(arr.fieldNames.contains("two"))
    assertEquals(StringType, arr("one").dataType)
    assertEquals(StringType, arr("two").dataType)
  }

  @Test
  def testConvertToStructWithSpecifiedArray(): Unit = {
    val mapping = """{
    |  "simple" : {
    |      "properties" : {
    |          "name" : {
    |              "type" : "string"
    |          }
    |      }
    |  }
    |}
    |""".stripMargin
    
    cfg.setProperty(ES_READ_FIELD_AS_ARRAY_INCLUDE, "name")
    
    val struct = getStruct(mapping)
    assertTrue(struct.fieldNames.contains("name"))
    assertEquals("array", struct("name").dataType.typeName)
    
    val arr = struct("name").dataType.asInstanceOf[ArrayType]
    assertEquals(StringType, arr.elementType)
  }

  
  @Test
  def testConvertToStructWithSpecifiedArrayDepth(): Unit = {
    val mapping = """{
    |  "simple" : {
    |      "properties" : {
    |          "name" : {
    |              "type" : "string"
    |          }
    |      }
    |  }
    |}
    |""".stripMargin
    
    cfg.setProperty(ES_READ_FIELD_AS_ARRAY_INCLUDE, "name:3")
    
    val struct = getStruct(mapping)
    assertTrue(struct.fieldNames.contains("name"))

    // first level
    assertEquals("array", struct("name").dataType.typeName)

    var arr = struct("name").dataType.asInstanceOf[ArrayType]
    // second level
    assertEquals("array", arr.elementType.typeName)

    arr = arr.elementType.asInstanceOf[ArrayType]
    // third type
    assertEquals("array", arr.elementType.typeName)
    
    arr = arr.elementType.asInstanceOf[ArrayType]
    // actual type
    assertEquals(StringType, arr.elementType)
  }

  @Test
  def testConvertToStructWithJoinField(): Unit = {
    val mapping =
      """{
        |  "join": {
        |    "properties": {
        |      "my_join": {
        |        "type": "join",
        |        "relations": {
        |          "my_parent": "my_child"
        |        }
        |      }
        |    }
        |  }
        |}
      """.stripMargin

    val struct = getStruct(mapping)
    assertTrue(struct.fieldNames.contains("my_join"))

    val nested = struct("my_join").dataType
    assertEquals("struct", nested.typeName)

    val arr = nested.asInstanceOf[StructType]
    assertTrue(arr.fieldNames.contains("name"))
    assertTrue(arr.fieldNames.contains("parent"))
    assertEquals(StringType, arr("name").dataType)
    assertEquals(StringType, arr("parent").dataType)
  }

  @Test
  def testDetectRowInfoSimple(): Unit = {
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
    
    val struct = getStruct(mapping)
    val info = detectRowInfo(cfg, struct)
    assertEquals("arr,top-level", info._1.getProperty("_"))
    assertEquals("one,two", info._1.getProperty("arr"))
  }

  @Test
  def testDetectRowInfoWithOneNestedArray(): Unit = {
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
    
    cfg.setProperty(ES_READ_FIELD_AS_ARRAY_INCLUDE, "arr")
    
    val struct = getStruct(mapping)
    val info = detectRowInfo(cfg, struct)
    assertEquals("arr,top-level", info._1.getProperty("_"))
    assertEquals("one,two", info._1.getProperty("arr"))
    assertEquals("1", info._2.getProperty("arr"))
  }

  @Test
  def testDetectRowInfoWithMultiDepthArray(): Unit = {
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
    
    cfg.setProperty(ES_READ_FIELD_AS_ARRAY_INCLUDE, "arr:3")
    
    val struct = getStruct(mapping)
    val info = detectRowInfo(cfg, struct)
    assertEquals("arr,top-level", info._1.getProperty("_"))
    assertEquals("one,two", info._1.getProperty("arr"))
    assertEquals("3", info._2.getProperty("arr"))
  }

  private def wrapMappingAsResponse(mapping: String): String =
    s"""{
       |  "index": {
       |    "mappings": $mapping
       |  }
       |}
       """.stripMargin

  private def fieldFromMapping(mapping: String) = {
    FieldParser.parseMapping(new ObjectMapper().readValue(wrapMappingAsResponse(mapping), classOf[JMap[String, Object]])).getResolvedView
  }
  
  private def getStruct(mapping: String) = {
    convertToStruct(fieldFromMapping(mapping), Collections.emptyMap(), cfg)
  }
}