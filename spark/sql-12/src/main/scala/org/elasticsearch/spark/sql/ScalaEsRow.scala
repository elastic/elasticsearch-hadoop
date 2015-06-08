package org.elasticsearch.spark.sql

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer

import org.apache.spark.sql.catalyst.expressions.Row

private[spark] class ScalaEsRow(private[spark] val rowOrder: Seq[String]) extends Row {

  lazy private[spark] val values: ArrayBuffer[Any] = ArrayBuffer.fill(rowOrder.size)(null)

  /** No-arg constructor for Kryo serialization. */
  def this() = this(null)

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
    values(i).asInstanceOf[String]
  }

  def copy() = this

  // Custom hashCode function that matches the efficient code generated version.
  override def hashCode: Int = {
    var result: Int = 37

    var i = 0
    while (i < values.length) {
      val update: Int =
        if (isNullAt(i)) {
          0
        } else {
          apply(i) match {
            case b: Boolean => if (b) 0 else 1
            case b: Byte => b.toInt
            case s: Short => s.toInt
            case i: Int => i
            case l: Long => (l ^ (l >>> 32)).toInt
            case f: Float => java.lang.Float.floatToIntBits(f)
            case d: Double =>
              val b = java.lang.Double.doubleToLongBits(d)
              (b ^ (b >>> 32)).toInt
            case other => other.hashCode()
          }
        }
      result = 37 * result + update
      i += 1
    }
    result
  }
}