package org.elasticsearch.hadoop.spark.serialization

import scala.collection.JavaConverters._
import scala.collection.mutable.LinkedHashMap
import scala.collection.mutable.Map

import org.elasticsearch.hadoop.cfg.Settings
import org.elasticsearch.hadoop.serialization.FieldType
import org.elasticsearch.hadoop.serialization.FieldType._
import org.elasticsearch.hadoop.serialization.Parser
import org.elasticsearch.hadoop.serialization.SettingsAware
import org.elasticsearch.hadoop.serialization.builder.ValueReader

class ScalaValueReader extends ValueReader with SettingsAware {

  var emptyAsNull: Boolean = false
  
  def readValue(parser: Parser, value: String, esType: FieldType) = {
    if (esType == null) {
      null
    }

    esType match {
      case NULL => nullValue()
      case STRING => textValue(value)
      case BYTE => byteValue(value)
      case SHORT => shortValue(value)
      case INTEGER => intValue(value)
      case TOKEN_COUNT => longValue(value)
      case LONG => longValue(value)
      case FLOAT => floatValue(value)
      case DOUBLE => doubleValue(value)
      case BOOLEAN => booleanValue(value)
      case BINARY => binaryValue(parser.binaryValue())
      case DATE => date(value)
      // everything else (IP, GEO) gets translated to strings
      case _ => textValue(value)
    }
  }
  
  def checkNull(converter: String => Any, value: String) = {
    if (value != null) {
      if (value.isEmpty() && emptyAsNull) {
        nullValue()
      }
      converter(value).asInstanceOf[AnyRef]
    }
    else {
      nullValue()
    }
  }
  
  def nullValue() = { None }
  def textValue(value: String) = { checkNull (parseText, value) }
  protected def parseText(value:String) = { value }
  
  def byteValue(value: String) = { checkNull (parseByte, value) }
  protected def parseByte(value: String) = { value.toByte }

  def shortValue(value: String) = { checkNull (parseShort, value) }
  protected def parseShort(value: String) = { value.toShort }
  
  def intValue(value: String) = { checkNull(parseInt, value) }
  protected def parseInt(value: String) = { value.toInt }
  
  def longValue(value: String) = { checkNull(parseLong, value) }
  protected def parseLong(value: String) = { value.toLong }
  
  def floatValue(value: String) = { checkNull(parseFloat, value) }
  protected def parseFloat(value: String) = { value.toFloat }
  
  def doubleValue(value: String) = { checkNull(parseDouble, value) }
  protected def parseDouble(value: String) = { value.toDouble }
  
  def booleanValue(value: String) = { checkNull(parseBoolean, value) }
  protected def parseBoolean(value: String) = { value.toBoolean }
  
  def binaryValue(value: Array[Byte]) = {  
    if (value != null) {
      if (value.length == 0 && emptyAsNull) {
        nullValue()
      }
      parseBinary(value)
    }
    else {
      nullValue()
    }
  }
  protected def parseBinary(value: Array[Byte]) = { value }
  
  def date(value: String) = { checkNull(parseDate, value) }
  protected def parseDate(value: String) = { value }
  
  def setSettings(settings: Settings) = { emptyAsNull = settings.getFieldReadEmptyAsNull() }
  
  def createMap() = {
    new LinkedHashMap
  }
  
  def addToMap(map: AnyRef, key: AnyRef, value: AnyRef) = {
    map.asInstanceOf[Map[AnyRef, AnyRef]].put(key, value)
  }
  
  def createArray(typ: FieldType) = {
    List.empty;
  }

  def addToArray(array: AnyRef, values: java.util.List[Object]): AnyRef = {
    values.asScala
  }
}