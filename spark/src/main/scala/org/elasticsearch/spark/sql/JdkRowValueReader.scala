package org.elasticsearch.spark.sql

import scala.collection.mutable.LinkedHashMap

import org.elasticsearch.hadoop.serialization.builder.JdkValueReader

class JdkRowValueReader extends JdkValueReader {

  override def createMap() = {
    new JavaEsRow(new ScalaEsRow(new LinkedHashMap()))
  }
  
  override def addToMap(map: AnyRef, key: AnyRef, value: AnyRef) = {
    map.asInstanceOf[JavaEsRow].esrow.map.put(key, value)
  }
}