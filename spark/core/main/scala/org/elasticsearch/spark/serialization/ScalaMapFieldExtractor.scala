package org.elasticsearch.spark.serialization

import scala.collection.GenMapLike
import scala.collection.Map

import org.elasticsearch.hadoop.serialization.field.ConstantFieldExtractor
import org.elasticsearch.hadoop.serialization.MapFieldExtractor
import org.elasticsearch.hadoop.serialization.field.FieldExtractor._
import org.elasticsearch.spark.serialization.{ ReflectionUtils => RU }

class ScalaMapFieldExtractor extends MapFieldExtractor {

  override protected def extractField(target: AnyRef): AnyRef = {
    var obj = target
    for (index <- 0 until getFieldNames.size()) {
      val field = getFieldNames.get(index)
      obj = obj match {
        case m: Map[_, _]                    => m.asInstanceOf[Map[AnyRef, AnyRef]].getOrElse(field, NOT_FOUND)
        case p: Product if RU.isCaseClass(p) => RU.caseClassValues(p).getOrElse(field, NOT_FOUND).asInstanceOf[AnyRef]
        case _                               => {
          val result = super.extractField(target)

          if (result == NOT_FOUND && RU.isJavaBean(target)) {
            RU.javaBeanAsMap(target).getOrElse(field, NOT_FOUND)
          }
          else {
            result
          }
        }
      }
    }
    return obj
  }
}