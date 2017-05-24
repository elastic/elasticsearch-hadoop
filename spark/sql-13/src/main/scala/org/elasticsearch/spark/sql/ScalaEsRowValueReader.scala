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

import java.sql.Timestamp
import scala.collection.mutable.LinkedHashMap
import scala.collection.mutable.Map
import org.elasticsearch.hadoop.serialization.FieldType
import org.elasticsearch.hadoop.serialization.Parser
import org.elasticsearch.hadoop.serialization.builder.ValueParsingCallback
import org.elasticsearch.spark.serialization.ScalaValueReader
import org.apache.commons.logging.LogFactory
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.hadoop.util.StringUtils

class ScalaRowValueReader extends ScalaValueReader with RowValueReader with ValueParsingCallback {

  var metadataMap = true
  var rootLevel = true
  var inArray = false
  var currentArrayRowOrder:Seq[String] = null

  override def readValue(parser: Parser, value: String, esType: FieldType) = {
    sparkRowField = currentFieldName

    if (sparkRowField == null) {
      sparkRowField = Utils.ROOT_LEVEL_NAME
    }

    super.readValue(parser, value, esType)
  }

  override def createMap() = {
    if (readMetadata && metadataMap) {
      metadataMap = false
      // metadata has schema [String, String] so convert all values (like score) to String
      new LinkedHashMap[Any, Any] {
        override def put(key: Any, value: Any): Option[Any] = {
          super.put(key, if (value != null) value.toString() else null)
        }
      }
    }
    else {
      val rowOrd = 
      if (inArray) {
        if (rowColumnsMap.contains(sparkRowField)) {
            rowColumns(sparkRowField)
        }
        else {
          currentArrayRowOrder
        }
      }
      else rowColumns(sparkRowField)

      new ScalaEsRow(rowOrd)
    }
  }

  // start array
  override def createArray(typ: FieldType): AnyRef = {

    val previousLevel = (inArray, currentArrayRowOrder)
    if (arrayFields.contains(sparkRowField)) {
      inArray = true
      // array of objects
      if (rowColumnsMap.contains(sparkRowField)) {
        currentArrayRowOrder = rowColumns(sparkRowField)
      }
      // array of values
      else {
        // ignore
      }
    }
    else {
      LogFactory.getLog(getClass).warn(
          s"""Field '$sparkRowField' is backed by an array but the associated Spark Schema does not reflect this;
              (use ${ConfigurationOptions.ES_READ_FIELD_AS_ARRAY_INCLUDE}/exclude) """.stripMargin)
    }
    // since the list is not used actually, return the parent field information usable for nested arrays
    previousLevel
  }

  // end array
  override def addToArray(array: AnyRef, values: java.util.List[Object]): AnyRef = {
    // restore previous state
    array match {
      case (pastInArray: Boolean, pastRowOrder: Seq[String @unchecked]) => {
        inArray = pastInArray
        currentArrayRowOrder = pastRowOrder
      }
      case _ => {
        inArray = false
        currentArrayRowOrder = null
      }
    }
    super.addToArray(array, values)
  }

  override def addToMap(map: AnyRef, key: AnyRef, value: Any) = {
    map match {
      case m: Map[_, _]        => super.addToMap(map, key, value)
      case r: ScalaEsRow       => addToBuffer(r, key, value)
    }
  }

  override def createDate(value: Long) = {
    new Timestamp(value)
  }

  def beginDoc(): Unit = {}

  def beginLeadMetadata(): Unit = { metadataMap = true }

  def endLeadMetadata(): Unit = {}

  def beginSource(): Unit = { rootLevel = true; sparkRowField = Utils.ROOT_LEVEL_NAME }

  def endSource(): Unit = {}

  def excludeSource(): Unit = { rootLevel = true; sparkRowField = Utils.ROOT_LEVEL_NAME }

  def beginTrailMetadata(): Unit = {}

  def endTrailMetadata(): Unit = {}

  def endDoc(): Unit = {}
  
  def beginGeoField(): Unit = {
    currentFieldIsGeo = true
  }
  
  def endGeoField(): Unit = {
    currentFieldIsGeo = false
  }
}