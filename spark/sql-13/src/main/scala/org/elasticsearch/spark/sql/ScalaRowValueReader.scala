package org.elasticsearch.spark.sql

import org.elasticsearch.spark.serialization.ScalaValueReader

class ScalaRowValueReader extends ScalaValueReader with RowValueReader {
  
  override def createMap() = {
    new ScalaEsRow(createBuffer)
  }
  
  override def addToMap(map: AnyRef, key: AnyRef, value: Any) = {
    addToBuffer(map.asInstanceOf[ScalaEsRow].values, key, value)
  }
}