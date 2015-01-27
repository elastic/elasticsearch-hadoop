package org.elasticsearch.spark.serialization

import java.lang.reflect.Method

import scala.collection.Map
import scala.collection.immutable.Nil
import scala.collection.mutable.HashMap
import scala.ref.WeakReference

import org.elasticsearch.hadoop.serialization.Generator
import org.elasticsearch.hadoop.serialization.builder.JdkValueWriter
import org.elasticsearch.spark.serialization.{ ReflectionUtils => RU }

class ScalaValueWriter(writeUnknownTypes: Boolean = false) extends JdkValueWriter(writeUnknownTypes) {

  val caseClassCache = new HashMap[Class[_], (Boolean, Iterable[String])]
  val javaBeanCache = new HashMap[Class[_], Array[(String, Method)]]

  def this() {
    this(false)
  }

  override def write(value: AnyRef, generator: Generator): Boolean = {
    value match {
      case None => generator.writeNull()
      case Unit => generator.writeNull()
      case Nil =>
        generator.writeBeginArray(); generator.writeEndArray()

      case s: Some[AnyRef] => return write(s.get, generator)

      case m: Map[_, AnyRef] => {
        generator.writeBeginObject()
        for ((k, v) <- m) {
          generator.writeFieldName(k.toString())
          if (!write(v, generator)) {
            return false
          }
        }
        generator.writeEndObject()
      }

      case i: Traversable[AnyRef] => {
        generator.writeBeginArray()
        for (v <- i) {
          if (!write(v, generator)) {
            return false
          }
        }
        generator.writeEndArray()
      }

      case p: Product => {
        // handle case class
        if (isCaseClass(p)) {
          if (!write(caseClassValues(p), generator)) {
            return false
          }
        } // normal product - treat it as a list/array
        else {
          generator.writeBeginArray()
          for (t <- p.productIterator) {
            if (!write(t.asInstanceOf[AnyRef], generator)) {
              return false
            }
          }
          generator.writeEndArray()
        }
      }

      case _ => {
        // normal JDK types failed, try the JavaBean last
        if (!super.write(value, generator)) {
          if (isJavaBean(value)) {
            return write(javaBeanAsMap(value), generator)
          } else
            return false
        }
      }
    }

    true
  }

  def isCaseClass(p: Product) = {
    caseClassCache.getOrElseUpdate(p.getClass, {
      val isCaseClazz = RU.isCaseClass(p.getClass)
      val info = if (isCaseClazz) RU.caseClassInfo(p.getClass) else null
      (isCaseClazz, info)
    })._1
  }

  def caseClassValues(p: Product) = {
    RU.caseClassValues(p.asInstanceOf[AnyRef], caseClassCache.get(p.getClass).get._2)
  }

  def isJavaBean(value: AnyRef) = {
    !javaBeanCache.getOrElseUpdate(value.getClass, {
      RU.javaBeansInfo(value.getClass)
    }).isEmpty
  }

  def javaBeanAsMap(value: AnyRef) = {
    RU.javaBeansValues(value, javaBeanCache.get(value.getClass()).get)
  }
}