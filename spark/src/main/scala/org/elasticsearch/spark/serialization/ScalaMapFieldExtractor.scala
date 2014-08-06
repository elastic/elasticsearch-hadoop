package org.elasticsearch.spark.serialization

import org.elasticsearch.hadoop.serialization.field.ConstantFieldExtractor
import org.elasticsearch.hadoop.serialization.MapFieldExtractor
import scala.collection.GenMapLike

class ScalaMapFieldExtractor extends MapFieldExtractor {
  
  override protected def extractField(target: AnyRef): AnyRef = {
    target match {
      case m: Map[AnyRef, AnyRef] => return m.getOrElse(getFieldName(), null)
    }
  } 
  
}