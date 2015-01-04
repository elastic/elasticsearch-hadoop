package org.elasticsearch.spark.sql

import scala.collection.mutable.ArrayBuffer
import org.elasticsearch.hadoop.serialization.builder.JdkValueReader
import org.elasticsearch.hadoop.util.StringUtils
import org.elasticsearch.hadoop.cfg.Settings
import scala.collection.mutable.LinkedHashMap

class JdkRowValueReader extends JdkValueReader with RowValueReader {

  override def createMap() = {
    new JavaEsRow(new ScalaEsRow(createBuffer))
  }
  
  override def addToMap(map: AnyRef, key: AnyRef, value: AnyRef) = {
    addToBuffer(map.asInstanceOf[JavaEsRow].esrow.values, key, value)
  }
}