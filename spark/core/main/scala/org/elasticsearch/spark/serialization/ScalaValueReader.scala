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

import java.util.Collections
import java.util.Date
import java.util.{List => JList}

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.Seq
import scala.collection.mutable.LinkedHashMap
import scala.collection.mutable.Map
import org.elasticsearch.hadoop.cfg.Settings
import org.elasticsearch.hadoop.serialization.FieldType
import org.elasticsearch.hadoop.serialization.FieldType.BINARY
import org.elasticsearch.hadoop.serialization.FieldType.BOOLEAN
import org.elasticsearch.hadoop.serialization.FieldType.BYTE
import org.elasticsearch.hadoop.serialization.FieldType.DATE
import org.elasticsearch.hadoop.serialization.FieldType.DOUBLE
import org.elasticsearch.hadoop.serialization.FieldType.HALF_FLOAT
import org.elasticsearch.hadoop.serialization.FieldType.SCALED_FLOAT
import org.elasticsearch.hadoop.serialization.FieldType.FLOAT
import org.elasticsearch.hadoop.serialization.FieldType.INTEGER
import org.elasticsearch.hadoop.serialization.FieldType.JOIN
import org.elasticsearch.hadoop.serialization.FieldType.KEYWORD
import org.elasticsearch.hadoop.serialization.FieldType.GEO_POINT
import org.elasticsearch.hadoop.serialization.FieldType.GEO_SHAPE
import org.elasticsearch.hadoop.serialization.FieldType.LONG
import org.elasticsearch.hadoop.serialization.FieldType.NULL
import org.elasticsearch.hadoop.serialization.FieldType.SHORT
import org.elasticsearch.hadoop.serialization.FieldType.STRING
import org.elasticsearch.hadoop.serialization.FieldType.TEXT
import org.elasticsearch.hadoop.serialization.FieldType.TOKEN_COUNT
import org.elasticsearch.hadoop.serialization.Parser
import org.elasticsearch.hadoop.serialization.Parser.Token.VALUE_BOOLEAN
import org.elasticsearch.hadoop.serialization.Parser.Token.VALUE_NULL
import org.elasticsearch.hadoop.serialization.Parser.Token.VALUE_NUMBER
import org.elasticsearch.hadoop.serialization.SettingsAware
import org.elasticsearch.hadoop.serialization.builder.ValueReader
import org.elasticsearch.hadoop.serialization.field.FieldFilter
import org.elasticsearch.hadoop.serialization.field.FieldFilter.NumberedInclude
import org.elasticsearch.hadoop.util.DateUtils
import org.elasticsearch.hadoop.util.SettingsUtils
import org.elasticsearch.hadoop.util.StringUtils
import org.elasticsearch.hadoop.util.unit.Booleans

import scala.annotation.tailrec

class ScalaValueReader extends ValueReader with SettingsAware {

  var emptyAsNull: Boolean = false
  var richDate: Boolean = false
  var arrayInclude: JList[NumberedInclude] = Collections.emptyList()
  var arrayExclude: JList[String] = Collections.emptyList()

  var nestedArrayLevel: Integer = 0
  var currentFieldName: String = StringUtils.EMPTY

  def readValue(parser: Parser, value: String, esType: FieldType) = {
    if (esType == null || parser.currentToken() == VALUE_NULL) {
      nullValue()

    } else {
      esType match {
        case NULL => nullValue()
        case STRING => textValue(value, parser)
        case TEXT => textValue(value, parser)
        case KEYWORD => textValue(value, parser)
        case BYTE => byteValue(value, parser)
        case SHORT => shortValue(value, parser)
        case INTEGER => intValue(value, parser)
        case TOKEN_COUNT => longValue(value, parser)
        case LONG => longValue(value, parser)
        case HALF_FLOAT => floatValue(value, parser)
        case SCALED_FLOAT => floatValue(value, parser)
        case FLOAT => floatValue(value, parser)
        case DOUBLE => doubleValue(value, parser)
        case BOOLEAN => booleanValue(value, parser)
        case BINARY => binaryValue(Option(parser.binaryValue()).getOrElse(value.getBytes()))
        case DATE => date(value, parser)
        // GEO is ambiguous so use the JSON type instead to differentiate between doubles (a lot in GEO_SHAPE) and strings
        case GEO_POINT | GEO_SHAPE => {
          if (parser.currentToken() == VALUE_NUMBER) doubleValue(value, parser) else textValue(value, parser)
        }
        // JOIN field is special. Only way that we could have reached here is if the join value we're reading is
        // the short-hand format of the join field for parent documents. Make a container and put the value under it.
        case JOIN => {
          val container = createMap()
          addToMap(container, "name", textValue(value, parser))
          container
        }
        // everything else (IP, GEO) gets translated to strings
        case _ => textValue(value, parser)
      }
    }
  }

  def checkNull(converter: (String, Parser) => Any, value: String, parser: Parser) = {
    if (value != null) {
      if (!StringUtils.hasText(value) && emptyAsNull) {
        nullValue()
      }
      else {
        converter(value, parser).asInstanceOf[AnyRef]
      }
    }
    else {
      nullValue()
    }
  }

  def nullValue() = { None }
  def textValue(value: String, parser: Parser) = { checkNull (parseText, value, parser) }
  protected def parseText(value:String, parser: Parser) = { value }

  def byteValue(value: String, parser: Parser) = { checkNull (parseByte, value, parser) }
  protected def parseByte(value: String, parser:Parser) = { if (parser.currentToken()== VALUE_NUMBER) parser.intValue().toByte else value.toByte }

  def shortValue(value: String, parser:Parser) = { checkNull (parseShort, value, parser) }
  protected def parseShort(value: String, parser:Parser) = { if (parser.currentToken()== VALUE_NUMBER) parser.shortValue().toShort else value.toShort }

  def intValue(value: String, parser:Parser) = { checkNull(parseInt, value, parser) }
  protected def parseInt(value: String, parser:Parser) = { if (parser.currentToken()== VALUE_NUMBER) parser.intValue().toInt else value.toInt }

  def longValue(value: String, parser:Parser) = { checkNull(parseLong, value, parser) }
  protected def parseLong(value: String, parser:Parser) = { if (parser.currentToken()== VALUE_NUMBER) parser.longValue().toLong else value.toLong }

  def floatValue(value: String, parser:Parser) = { checkNull(parseFloat, value, parser) }
  protected def parseFloat(value: String, parser:Parser) = { if (parser.currentToken()== VALUE_NUMBER) parser.floatValue().toFloat else value.toFloat }

  def doubleValue(value: String, parser:Parser) = { checkNull(parseDouble, value, parser) }
  protected def parseDouble(value: String, parser:Parser) = { if (parser.currentToken()== VALUE_NUMBER) parser.doubleValue().toDouble else value.toDouble }

  def booleanValue(value: String, parser:Parser) = { checkNull(parseBoolean, value, parser) }
  protected def parseBoolean(value: String, parser:Parser) = {
    if (parser.currentToken()== VALUE_NULL) nullValue()
    else if (parser.currentToken()== VALUE_BOOLEAN) parser.booleanValue()
    else if (parser.currentToken()== VALUE_NUMBER) parser.intValue() != 0
    else Booleans.parseBoolean(value)
  }

  def binaryValue(value: Array[Byte]) = {
    Option(value) collect {
      case value: Array[Byte] if !emptyAsNull || !value.isEmpty =>
        parseBinary(value)
    } getOrElse nullValue()
  }
  protected def parseBinary(value: Array[Byte]) = { value }

  def date(value: String, parser: Parser) = { checkNull(parseDate, value, parser) }

  protected def parseDate(value: String, parser:Parser) = {
    if (parser.currentToken()== VALUE_NUMBER) {
     if (richDate) createDate(parser.longValue()) else parser.longValue()
    }
    else {
     if (richDate) createDate(value) else value
    }
  }

  protected def createDate(value: Long):Any = {
    new Date(value)
  }

  protected def createDate(value: String):Any = {
    createDate(DateUtils.parseDate(value).getTimeInMillis())
  }

  def setSettings(settings: Settings) = {
    emptyAsNull = settings.getReadFieldEmptyAsNull
    richDate = settings.getMappingDateRich
    arrayInclude = SettingsUtils.getFieldArrayFilterInclude(settings);
    arrayExclude = StringUtils.tokenize(settings.getReadFieldAsArrayExclude());
  }

  def createMap(): AnyRef = {
    new LinkedHashMap
  }

  override def addToMap(map: AnyRef, key: AnyRef, value: Any): Unit = {
    map.asInstanceOf[Map[AnyRef, Any]].put(key, value)
  }

  override def wrapString(value: String): AnyRef = {
    value
  }

  def createArray(typ: FieldType): AnyRef = {
    nestedArrayLevel += 1

    List.empty
  }

  override def addToArray(array: AnyRef, values: java.util.List[Object]): AnyRef = {
      nestedArrayLevel -= 1

      var arr: AnyRef = values.asScala
      // outer most array (a multi level array might be defined)
      if (nestedArrayLevel == 0) {
          val result = FieldFilter.filter(currentFieldName, arrayInclude, arrayExclude)
          if (result.matched && result.depth > 1) {
              val extraDepth = result.depth - arrayDepth(arr)
              if (extraDepth > 0) {
                  arr = wrapArray(arr, extraDepth)
              }
          }
      }
      arr
  }

  def arrayDepth(potentialArray: AnyRef): Int = {
    @tailrec
    def _arrayDepth(potentialArray: AnyRef, depth: Int): Int = {
      potentialArray match {
        case Seq(x: AnyRef, _*) => _arrayDepth(x, depth + 1)
        case _ => depth
      }
    }
    _arrayDepth(potentialArray, 0)
  }

  def wrapArray(array: AnyRef, extraDepth: Int): AnyRef = {
      var arr = array
      for (_ <- 0 until extraDepth) {
          arr = List(arr)
      }
      arr
  }

  def beginField(fieldName: String): Unit = {
       currentFieldName = fieldName
  }

  def endField(fieldName: String): Unit = {
       currentFieldName = null
  }
}