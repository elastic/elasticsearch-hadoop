package org.elasticsearch.spark.serialization

import java.beans.Introspector
import java.lang.reflect.Method
import scala.reflect.runtime.{ universe => ru }
import scala.reflect.runtime.universe._
import scala.reflect.ClassTag

private[spark] object ReflectionUtils {

  //SI-6240 
  protected[spark] object ReflectionLock

  def javaBeansInfo(clazz: Class[_]) = {
    Introspector.getBeanInfo(clazz).getPropertyDescriptors().filterNot(_.getName == "class").map(pd => (pd.getName, pd.getReadMethod)).sortBy(_._1)
  }

  def javaBeansValues(target: AnyRef, info: Array[(String, Method)]) = {
    info.map(in => (in._1, in._2.invoke(target))).toMap
  }

  def isCaseClass(clazz: Class[_]): Boolean = {
    ReflectionLock.synchronized {
      // reliable case class identifier only happens through class symbols...
      runtimeMirror(clazz.getClassLoader()).classSymbol(clazz).isCaseClass
    }
  }

  def caseClassInfo(clazz: Class[_]): Iterable[String] = {
    ReflectionLock.synchronized {
      runtimeMirror(clazz.getClassLoader()).classSymbol(clazz).toType.declarations.collect {
        case m: MethodSymbol if m.isCaseAccessor => m.name.toString()
      }
    }
  }

  def caseClassValues(target: AnyRef, props: Iterable[String]) = {
    val product = target.asInstanceOf[Product].productIterator
    val tuples = for (y <- props) yield (y, product.next)
    tuples.toMap
  }
}