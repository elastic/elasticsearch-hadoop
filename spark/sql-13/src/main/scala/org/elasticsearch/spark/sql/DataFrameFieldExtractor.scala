package org.elasticsearch.spark.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.elasticsearch.hadoop.serialization.field.FieldExtractor
import org.elasticsearch.spark.serialization.ScalaMapFieldExtractor

class DataFrameFieldExtractor extends ScalaMapFieldExtractor {

  override protected def extractField(target: AnyRef): AnyRef = {
    var obj = target
    for (in <- 0 until getFieldNames.size()) {
      val field = getFieldNames.get(in)
      obj = obj match {
        case (row: Row, struct: StructType) => {
          val index = struct.fieldNames.indexOf(field)
          if (index < 0) {
            FieldExtractor.NOT_FOUND
          } else {
            row(index).asInstanceOf[AnyRef]
          }
        }
        case _ => super.extractField(target)
      }
    }
    return obj
  }
}