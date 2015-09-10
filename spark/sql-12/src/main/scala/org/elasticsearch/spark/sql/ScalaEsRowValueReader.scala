package org.elasticsearch.spark.sql

import java.sql.Timestamp

import scala.collection.mutable.LinkedHashMap
import scala.collection.mutable.Map

import org.elasticsearch.hadoop.serialization.FieldType
import org.elasticsearch.hadoop.serialization.Parser
import org.elasticsearch.hadoop.serialization.builder.ValueParsingCallback
import org.elasticsearch.spark.serialization.ScalaValueReader

class ScalaRowValueReader extends ScalaValueReader with RowValueReader with ValueParsingCallback {

  var metadataMap = true
  var rootLevel = true

  override def readValue(parser: Parser, value: String, esType: FieldType) = {
    currentField = parser.currentName
    if (currentField == null) {
      currentField = Utils.ROOT_LEVEL_NAME
    }
    super.readValue(parser, value, esType)
  }

  override def createMap() = {
    if (readMetadata && metadataMap) {
      metadataMap = false
      new LinkedHashMap
    }
    else {
      new ScalaEsRow(rowOrder(currentField))
    }
  }

  override def addToMap(map: AnyRef, key: AnyRef, value: Any) = {
    map match {
      case m: Map[_, _]        => super.addToMap(map, key, value)
      case r: ScalaEsRow       => addToBuffer(map.asInstanceOf[ScalaEsRow], key, value)
    }
  }

  override def createDate(value: Long) = {
    new Timestamp(value)
  }

  def beginDoc() {}

  def beginLeadMetadata() { metadataMap = true }

  def endLeadMetadata() {}

  def beginSource() { rootLevel = true; currentField = Utils.ROOT_LEVEL_NAME }

  def endSource() {}

  def beginTrailMetadata() {}

  def endTrailMetadata() {}

  def endDoc() {}

}