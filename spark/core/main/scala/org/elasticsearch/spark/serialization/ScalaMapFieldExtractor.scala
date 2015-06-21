package org.elasticsearch.spark.serialization

import scala.collection.GenMapLike
import scala.collection.Map

import org.elasticsearch.hadoop.serialization.field.ConstantFieldExtractor
import org.elasticsearch.hadoop.serialization.MapFieldExtractor
import org.elasticsearch.hadoop.serialization.field.FieldExtractor._
import org.elasticsearch.spark.serialization.{ ReflectionUtils => RU }

class ScalaMapFieldExtractor extends MapFieldExtractor {

  override protected def extractField(target: AnyRef): AnyRef = {
    target match {
      case m: Map[_, _]                    => m.asInstanceOf[Map[AnyRef, AnyRef]].getOrElse(getFieldName, NOT_FOUND)
      case p: Product if RU.isCaseClass(p) => RU.caseClassValues(p).getOrElse(getFieldName, NOT_FOUND).asInstanceOf[AnyRef]
      case _                               => {
        val result = super.extractField(target)

        if (result == NOT_FOUND && RU.isJavaBean(target)) {
          return RU.javaBeanAsMap(target).getOrElse(getFieldName, NOT_FOUND)
        }

        result
      }
    }
  }
}