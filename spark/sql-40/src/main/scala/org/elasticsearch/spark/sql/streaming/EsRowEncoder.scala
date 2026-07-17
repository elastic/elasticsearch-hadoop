package org.elasticsearch.spark.sql.streaming

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.StructType

private[streaming] object EsRowEncoder {
  def make(schema: StructType): ExpressionEncoder[Row] =
    ExpressionEncoder(schema, false).resolveAndBind()
}
