package org.elasticsearch.spark.sql

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.Row

private[spark] class ScalaEsRow(private[spark] val rowOrder: Seq[String]) extends Row {

  lazy private[spark] val values: ArrayBuffer[Any] = ArrayBuffer.fill(rowOrder.size)(null)

  /** No-arg constructor for Kryo serialization. */
  def this() = this(null)

  def iterator = values.iterator

  override def length = values.size

  override def apply(i: Int) = values(i)

  override def get(i: Int): Any = values(i)
  
  override def isNullAt(i: Int) = values(i) == null

  override def getInt(i: Int): Int = getAs[Int](i)

  override def getLong(i: Int): Long = getAs[Long](i)

  override def getDouble(i: Int): Double = getAs[Double](i)

  override def getFloat(i: Int): Float = getAs[Float](i)

  override def getBoolean(i: Int): Boolean = getAs[Boolean](i)

  override def getShort(i: Int): Short = getAs[Short](i)

  override def getByte(i: Int): Byte = getAs[Byte](i)

  override def getString(i: Int): String = get(i).toString()
  
  def copy() = this

  override def toSeq = values.toSeq
}