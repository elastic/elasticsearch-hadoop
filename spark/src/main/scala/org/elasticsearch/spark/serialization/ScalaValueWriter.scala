package org.elasticsearch.spark.serialization

import java.lang.reflect.Method
import scala.collection.Map
import scala.collection.immutable.Nil
import scala.collection.mutable.HashMap
import scala.ref.WeakReference
import org.elasticsearch.hadoop.serialization.Generator
import org.elasticsearch.hadoop.serialization.builder.JdkValueWriter
import org.elasticsearch.spark.serialization.{ ReflectionUtils => RU }
import org.elasticsearch.hadoop.serialization.builder.ValueWriter.Result
import org.apache.commons.logging.LogFactory

class ScalaValueWriter(writeUnknownTypes: Boolean = false) extends JdkValueWriter(writeUnknownTypes) {

  val caseClassCache = new HashMap[Class[_], (Boolean, Iterable[String])]
  val javaBeanCache = new HashMap[Class[_], Array[(String, Method)]]

  def this() {
    this(false)
  }

  override def write(value: AnyRef, generator: Generator): Result = {
    doWrite(value, generator, true)
  }

  private def doWrite(value: AnyRef, generator: Generator, acceptsJavaBeans: Boolean): Result = {
    value match {
      case None => generator.writeNull()
      case Unit => generator.writeNull()
      case Nil =>
        generator.writeBeginArray(); generator.writeEndArray()

      case s: Some[AnyRef] => return doWrite(s.get, generator, false)

      case m: Map[_, AnyRef] => {
        generator.writeBeginObject()
        for ((k, v) <- m) {
          generator.writeFieldName(k.toString())
          val result = doWrite(v, generator, false)
          if (!result.isSuccesful()) {
            return result
          }
        }
        generator.writeEndObject()
      }

      case i: Traversable[AnyRef] => {
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
        if (isCaseClass(p)) {
          val result = doWrite(caseClassValues(p), generator, false)
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
          if (acceptsJavaBeans && isJavaBean(value)) {
            return doWrite(javaBeanAsMap(value), generator, false)
          } else
            return result
        }
      }
    }

    Result.SUCCESFUL()
  }

  def isCaseClass(p: Product) = {
    caseClassCache.getOrElseUpdate(p.getClass, {
      var isCaseClazz = RU.isCaseClass(p.getClass)
      var info = if (isCaseClazz) RU.caseClassInfo(p.getClass) else null
      if (!isCaseClazz) {
        isCaseClazz = RU.isCaseClassInsideACompanionModule(p.getClass, p.productArity)
        if (isCaseClazz) {
          LogFactory.getLog(classOf[ScalaValueWriter]).warn(
              String.format("[%s] is detected as a case class in Java but not in Scala and thus " +
                  "its properties might be detected incorrectly - make sure the @ScalaSignature is available within the class bytecode " +
                  "and/or consider moving the case class from its companion object/module", p.getClass))
        }
        info = if (isCaseClazz) RU.caseClassInfoInsideACompanionModule(p.getClass(), p.productArity) else null
      } 
      
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