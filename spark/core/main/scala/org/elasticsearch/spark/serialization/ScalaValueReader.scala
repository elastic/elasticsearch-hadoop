package org.elasticsearch.spark.serialization

import java.util.Date

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.LinkedHashMap
import scala.collection.mutable.Map

import org.elasticsearch.hadoop.cfg.Settings
import org.elasticsearch.hadoop.serialization.FieldType
import org.elasticsearch.hadoop.serialization.FieldType.BINARY
import org.elasticsearch.hadoop.serialization.FieldType.BOOLEAN
import org.elasticsearch.hadoop.serialization.FieldType.BYTE
import org.elasticsearch.hadoop.serialization.FieldType.DATE
import org.elasticsearch.hadoop.serialization.FieldType.DOUBLE
import org.elasticsearch.hadoop.serialization.FieldType.FLOAT
import org.elasticsearch.hadoop.serialization.FieldType.INTEGER
import org.elasticsearch.hadoop.serialization.FieldType.LONG
import org.elasticsearch.hadoop.serialization.FieldType.NULL
import org.elasticsearch.hadoop.serialization.FieldType.SHORT
import org.elasticsearch.hadoop.serialization.FieldType.STRING
import org.elasticsearch.hadoop.serialization.FieldType.TOKEN_COUNT
import org.elasticsearch.hadoop.serialization.Parser
import org.elasticsearch.hadoop.serialization.Parser.Token.VALUE_BOOLEAN
import org.elasticsearch.hadoop.serialization.Parser.Token.VALUE_NULL
import org.elasticsearch.hadoop.serialization.Parser.Token.VALUE_NUMBER
import org.elasticsearch.hadoop.serialization.SettingsAware
import org.elasticsearch.hadoop.serialization.builder.ValueReader
import org.elasticsearch.hadoop.util.DateUtils
import org.elasticsearch.hadoop.util.StringUtils

class ScalaValueReader extends ValueReader with SettingsAware {

  var emptyAsNull: Boolean = false
  var richDate: Boolean = false

  def readValue(parser: Parser, value: String, esType: FieldType) = {
    if (esType == null) {
      null
    }

    if (parser.currentToken() == VALUE_NULL) {
      nullValue()
    }

    esType match {
      case NULL => nullValue()
      case STRING => textValue(value, parser)
      case BYTE => byteValue(value, parser)
      case SHORT => shortValue(value, parser)
      case INTEGER => intValue(value, parser)
      case TOKEN_COUNT => longValue(value, parser)
      case LONG => longValue(value, parser)
      case FLOAT => floatValue(value, parser)
      case DOUBLE => doubleValue(value, parser)
      case BOOLEAN => booleanValue(value, parser)
      case BINARY => binaryValue(parser.binaryValue())
      case DATE => date(value, parser)
      // everything else (IP, GEO) gets translated to strings
      case _ => textValue(value, parser)
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
  protected def parseBoolean(value: String, parser:Parser) = { if (parser.currentToken()== VALUE_BOOLEAN)  parser.booleanValue() else value.toBoolean }

  def binaryValue(value: Array[Byte]) = {
    if (value != null) {
      if (emptyAsNull) {
        nullValue()
      }
      else {
        parseBinary(value)
      }
    }
    else {
      nullValue()
    }
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
    emptyAsNull = settings.getFieldReadEmptyAsNull
    richDate = settings.getMappingDateRich
  }

  def createMap(): AnyRef = {
    new LinkedHashMap
  }

  override def addToMap(map: AnyRef, key: AnyRef, value: Any) = {
    map.asInstanceOf[Map[AnyRef, Any]].put(key, value)
  }

  override def wrapString(value: String) = {
    value
  }

  def createArray(typ: FieldType): AnyRef = {
    List.empty;
  }

  def addToArray(array: AnyRef, values: java.util.List[Object]): AnyRef = {
    values.asScala
  }
}