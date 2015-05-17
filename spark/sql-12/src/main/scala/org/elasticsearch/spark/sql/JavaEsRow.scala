package org.elasticsearch.spark.sql

import org.apache.spark.sql.api.java.Row
import org.apache.spark.sql.catalyst.expressions.{Row => ScalaRow}

private[spark] class JavaEsRow(private[spark] val esrow: ScalaEsRow) extends Row(esrow) {
  /** No-arg constructor for Kryo serialization. */
  def this() = this(null)
}