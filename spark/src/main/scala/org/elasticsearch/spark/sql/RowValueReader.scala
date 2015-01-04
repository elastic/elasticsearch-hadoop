package org.elasticsearch.spark.sql

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.LinkedHashMap

import org.elasticsearch.hadoop.cfg.Settings
import org.elasticsearch.hadoop.serialization.SettingsAware
import org.elasticsearch.hadoop.util.StringUtils

private[sql] trait RowValueReader extends SettingsAware {

  private var rowOrder = new LinkedHashMap[String, Int]
  
  abstract override def setSettings(settings: Settings) = {
    super.setSettings(settings)
    val csv = settings.getScrollFields()
    
    if (StringUtils.hasText(csv)) {
      val fields = StringUtils.tokenize(csv).asScala
      for (i <- 0 until fields.length) {
        rowOrder.put(fields(i), i)
      }
    }
  }
  
  def createBuffer: ArrayBuffer[AnyRef] = {
    if (rowOrder.isEmpty) new ArrayBuffer() else ArrayBuffer.fill(rowOrder.size)(null) 
  }
  
  def addToBuffer(buffer: ArrayBuffer[AnyRef], key: AnyRef, value: AnyRef) {
    if (rowOrder.isEmpty) buffer.append(value) else { rowOrder(key.toString); buffer.update(rowOrder(key.toString), value) } 
  }
}