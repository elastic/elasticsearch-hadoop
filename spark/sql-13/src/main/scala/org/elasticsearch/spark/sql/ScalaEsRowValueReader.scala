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

class ScalaRowValueReader extends ScalaValueReader with RowValueReader with ValueParsingCallback {

  var metadataMap = true
  var rootLevel = true
  var inArray = false
  var currentArrayRowOrder:Seq[String] = null

  override def readValue(parser: Parser, value: String, esType: FieldType) = {
    currentField = stripSource(parser.absoluteName)

    if (currentField == null) {
      currentField = Utils.ROOT_LEVEL_NAME
    }

    super.readValue(parser, value, esType)
  }

  private def stripSource(name: String): String = {
    val root = "hits.hits._source."
    if (name != null && name.startsWith(root)) {
      return name.substring(root.length())
    }
    name
  }

  override def createMap() = {
    if (readMetadata && metadataMap) {
      metadataMap = false
      // metadata has schema [String, String] so convert all values (like score) to String
      new LinkedHashMap[Any, Any] {
        override def put(key: Any, value: Any): Option[Any] = {
          super.put(key, value.toString())
        }
      }
    }
    else {
      val rowOrd = if (inArray) currentArrayRowOrder else rowColumns(currentField)
      new ScalaEsRow(rowOrd)
    }
  }

  // start array
  override def createArray(typ: FieldType): AnyRef = {
    val previousLevel = (inArray, currentArrayRowOrder)
    if (arrayFields.contains(currentField)) {
      inArray = true
      // array of objects
      if (rowColumnsMap.contains(currentField)) {
        currentArrayRowOrder = rowColumns(currentField)
      }
      // array of values
      else {
        // ignore
      }
    }
    else {
      LogFactory.getLog(getClass).warn(
          s"""Field '$currentField' is backed by an array but the associated Spark Schema does not reflect this;
              (use ${ConfigurationOptions.ES_FIELD_READ_AS_ARRAY_INCLUDE}/exclude) """.stripMargin)
    }
    // since the list is not actually, return the parent field information usable for nested arrays
    previousLevel
  }

  // end array
  override def addToArray(array: AnyRef, values: java.util.List[Object]): AnyRef = {
    // restore previous state
    array match {
      case (pastInArray: Boolean, pastRowOrder: Seq[String]) => { inArray = pastInArray; currentArrayRowOrder = pastRowOrder }
      case _                                                 => { inArray = false; currentArrayRowOrder = null}
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

  def beginDoc() {}

  def beginLeadMetadata() { metadataMap = true }

  def endLeadMetadata() {}

  def beginSource() { rootLevel = true; currentField = Utils.ROOT_LEVEL_NAME }

  def endSource() {}

  def beginTrailMetadata() {}

  def endTrailMetadata() {}

  def endDoc() {}

}