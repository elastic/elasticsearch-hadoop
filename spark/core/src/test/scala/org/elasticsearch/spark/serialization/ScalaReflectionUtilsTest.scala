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

import org.elasticsearch.spark.serialization.ReflectionUtils._
import org.junit.Test
import org.junit.Assert._
import org.hamcrest.Matchers._

class ScalaReflectionUtilsTest {

  @Test
  def testJavaBean(): Unit = {
    val values = javaBeanAsMap(new Bean("1", Integer.valueOf(1), true))
    assertEquals(Map("id" -> 1, "bool" -> true, "foo" -> "1"), values)
  }

  @Test
  def testCaseClassValues(): Unit = {
    val cc = SimpleCaseClass(1, "simpleClass")
    assertTrue(isCaseClass(cc))
    val values = caseClassValues(cc)

    //println(values)
    assertEquals(Map("i" -> 1, "s" -> "simpleClass"), values)

    val ccv = CaseClassWithValue(2, "caseClassWithVal")
    assertTrue(isCaseClass(ccv))
    val valuesccv = caseClassValues(ccv)

    //println(valuesccv)
    assertEquals(Map("first" -> 2, "second" -> "caseClassWithVal"), valuesccv)
  }
}

case class SimpleCaseClass(i: Int, s: String) {
}

case class CaseClassWithValue(first: Int, second: String) {
  var internal = "internal"
}
