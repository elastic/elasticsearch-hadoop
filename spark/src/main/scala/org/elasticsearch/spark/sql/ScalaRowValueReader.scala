package org.elasticsearch.spark.sql

import scala.collection.mutable.LinkedHashMap

import org.elasticsearch.spark.serialization.ScalaValueReader

class ScalaRowValueReader extends ScalaValueReader {

  override def createMap() = {
    new ScalaEsRow(new LinkedHashMap)
  }
  
  override def addToMap(map: AnyRef, key: AnyRef, value: AnyRef) = {
    map.asInstanceOf[ScalaEsRow].map.put(key, value)
  }
}