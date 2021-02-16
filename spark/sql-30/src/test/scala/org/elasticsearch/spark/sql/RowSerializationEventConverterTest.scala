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

import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.elasticsearch.hadoop.serialization.handler.write.SerializationFailure
import org.elasticsearch.hadoop.serialization.handler.write.impl.SerializationEventConverter
import org.elasticsearch.hadoop.util.DateUtils
import org.elasticsearch.hadoop.util.StringUtils
import org.hamcrest.Matchers.equalTo
import org.junit.Assert.assertEquals
import org.junit.Assert.assertThat
import org.junit.Assert.assertTrue
import org.junit.Test

class RowSerializationEventConverterTest {

  @Test
  def generateEvent(): Unit = {
    val schema = StructType(Seq(
      StructField("field1", StringType),
      StructField("field2", StringType),
      StructField("field3", StringType)
    ))
    val row = Row("value1", "value2", "value3")

    val eventConverter = new SerializationEventConverter

    val iaeFailure = new SerializationFailure(new IllegalArgumentException("garbage"), (schema, row),
      new util.ArrayList[String])

    val rawEvent = eventConverter.getRawEvent(iaeFailure)
    assertThat(rawEvent, equalTo("(StructType(StructField(field1,StringType,true), " +
      "StructField(field2,StringType,true), StructField(field3,StringType,true)),[value1,value2,value3])"))
    val timestamp = eventConverter.getTimestamp(iaeFailure)
    assertTrue(StringUtils.hasText(timestamp))
    assertTrue(DateUtils.parseDate(timestamp).getTime.getTime > 1L)
    val exceptionType = eventConverter.renderExceptionType(iaeFailure)
    assertEquals("illegal_argument_exception", exceptionType)
    val exceptionMessage = eventConverter.renderExceptionMessage(iaeFailure)
    assertEquals("garbage", exceptionMessage)
    val eventMessage = eventConverter.renderEventMessage(iaeFailure)
    assertEquals("Could not construct bulk entry from record", eventMessage)
  }

}
