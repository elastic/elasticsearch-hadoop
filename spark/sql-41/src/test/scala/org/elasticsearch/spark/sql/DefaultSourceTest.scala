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

import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

class DefaultSourceTest {

  @Test
  def parameters(): Unit = {
    val settings = new mutable.LinkedHashMap[String, String]()
    settings.put("path", "wrong")
    settings.put("resource", "wrong")
    settings.put("es_resource", "preferred")
    settings.put("unrelated", "unrelated")

    val relation = new DefaultSource().params(settings.toMap)

    assertEquals(Map("es.resource" -> "preferred", "es.unrelated" -> "unrelated"), relation)
  }
}