package org.elasticsearch.spark.serialization

import org.elasticsearch.hadoop.serialization.field.ConstantFieldExtractor
import org.elasticsearch.hadoop.serialization.MapFieldExtractor
import scala.collection.GenMapLike
import scala.collection.Map
import org.elasticsearch.hadoop.serialization.field.FieldExtractor

class ScalaMapFieldExtractor extends MapFieldExtractor {
  
  override protected def extractField(target: AnyRef): AnyRef = {
    target match {
      case m: Map[AnyRef, AnyRef] => m.getOrElse(getFieldName(), FieldExtractor.NOT_FOUND)
      case _					  => super.extractField(target)
    }
  } 
  
}