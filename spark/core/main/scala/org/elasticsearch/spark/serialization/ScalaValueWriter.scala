package org.elasticsearch.spark.serialization

import scala.collection.Map
import scala.collection.immutable.Nil

import org.elasticsearch.hadoop.serialization.Generator
import org.elasticsearch.hadoop.serialization.builder.JdkValueWriter
import org.elasticsearch.hadoop.serialization.builder.ValueWriter.Result
import org.elasticsearch.spark.serialization.{ ReflectionUtils => RU }

class ScalaValueWriter(writeUnknownTypes: Boolean = false) extends JdkValueWriter(writeUnknownTypes) {

  def this() {
    this(false)
  }

  override def write(value: AnyRef, generator: Generator): Result = {
    doWrite(value, generator, true)
  }

  private def doWrite(value: Any, generator: Generator, acceptsJavaBeans: Boolean): Result = {
    value match {
      case None => generator.writeNull()
      case Unit => generator.writeNull()
      case Nil =>
        generator.writeBeginArray(); generator.writeEndArray()

      case Some(s: AnyRef) => return doWrite(s, generator, false)

      case m: Map[_, _] => {
        generator.writeBeginObject()
        for ((k, v) <- m) {
          if (shouldKeep(generator.getParentPath(), k.toString())) {
            generator.writeFieldName(k.toString())
            val result = doWrite(v, generator, false)
            if (!result.isSuccesful()) {
              return result
            }
          }
        }
        generator.writeEndObject()
      }

      case i: Traversable[_] => {
        generator.writeBeginArray()
        for (v <- i) {
          val result = doWrite(v, generator, false)
          if (!result.isSuccesful()) {
            return result
          }
        }
        generator.writeEndArray()
      }

      case p: Product => {
        // handle case class
        if (RU.isCaseClass(p)) {
          val result = doWrite(RU.caseClassValues(p), generator, false)
          if (!result.isSuccesful()) {
            return result
          }
        } // normal product - treat it as a list/array
        else {
          generator.writeBeginArray()
          for (t <- p.productIterator) {
            val result = doWrite(t.asInstanceOf[AnyRef], generator, false)
            if (!result.isSuccesful()) {
              return result
            }
          }
          generator.writeEndArray()
        }
      }

      case _ => {
        // normal JDK types failed, try the JavaBean last
        val result = super.write(value, generator)
        if (!result.isSuccesful()) {
          if (acceptsJavaBeans && RU.isJavaBean(value)) {
            return doWrite(RU.javaBeanAsMap(value), generator, false)
          } else
            return result
        }
      }
    }

    Result.SUCCESFUL()
  }
}