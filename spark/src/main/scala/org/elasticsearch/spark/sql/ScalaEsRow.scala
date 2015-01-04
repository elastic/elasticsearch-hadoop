package org.elasticsearch.spark.sql

import org.apache.spark.sql.catalyst.expressions.Row
import scala.collection.mutable.LinkedHashMap
import org.apache.spark.sql.catalyst.expressions.GenericRow
import scala.collection.mutable.ArrayBuffer

private[spark] class ScalaEsRow(private[spark] val values: ArrayBuffer[AnyRef]) extends Row {
  def iterator = values.iterator
  def length = values.size

  def apply(i: Int) = values(i)  

  def isNullAt(i: Int) = values(i) == null

  def getInt(i: Int): Int = {
    if (values(i) == null) sys.error("Failed to check null bit for primitive int value.")
    values(i).asInstanceOf[Int]
  }

  def getLong(i: Int): Long = {
    if (values(i) == null) sys.error("Failed to check null bit for primitive long value.")
    values(i).asInstanceOf[Long]
  }

  def getDouble(i: Int): Double = {
    if (values(i) == null) sys.error("Failed to check null bit for primitive double value.")
    values(i).asInstanceOf[Double]
  }

  def getFloat(i: Int): Float = {
    if (values(i) == null) sys.error("Failed to check null bit for primitive float value.")
    values(i).asInstanceOf[Float]
  }

  def getBoolean(i: Int): Boolean = {
    if (values(i) == null) sys.error("Failed to check null bit for primitive boolean value.")
    values(i).asInstanceOf[Boolean]
  }

  def getShort(i: Int): Short = {
    if (values(i) == null) sys.error("Failed to check null bit for primitive short value.")
    values(i).asInstanceOf[Short]
  }

  def getByte(i: Int): Byte = {
    if (values(i) == null) sys.error("Failed to check null bit for primitive byte value.")
    values(i).asInstanceOf[Byte]
  }

  def getString(i: Int): String = {
    if (values(i) == null) sys.error("Failed to check null bit for primitive String value.")
    values(i).asInstanceOf[String]
  }
  
  def copy() = this
}