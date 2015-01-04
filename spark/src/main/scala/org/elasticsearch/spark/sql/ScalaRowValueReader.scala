package org.elasticsearch.spark.sql

import scala.collection.mutable.LinkedHashMap
import org.elasticsearch.spark.serialization.ScalaValueReader
import org.elasticsearch.hadoop.cfg.Settings
import org.elasticsearch.hadoop.util.StringUtils
import scala.collection.mutable.ArrayBuffer

class ScalaRowValueReader extends ScalaValueReader with RowValueReader {
  
  override def createMap() = {
    new ScalaEsRow(createBuffer)
  }
  
  override def addToMap(map: AnyRef, key: AnyRef, value: AnyRef) = {
    addToBuffer(map.asInstanceOf[ScalaEsRow].values, key, value)
  }
}