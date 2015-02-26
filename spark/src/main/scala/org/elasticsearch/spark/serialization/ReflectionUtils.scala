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
    Introspector.getBeanInfo(clazz).getPropertyDescriptors().collect {
      case pd if (pd.getName != "class" && pd.getReadMethod() != null) => (pd.getName, pd.getReadMethod)
    }.sortBy(_._1)
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

  def isCaseClassInsideACompanionModule(clazz: Class[_], arity: Int): Boolean = {
    if (!classOf[Serializable].isAssignableFrom(clazz)) {
      false
    }

    // check 'copy' synthetic methods - they are public so go with getMethods
    val copyMethods = clazz.getMethods.collect {
      case m: Method if m.getName.startsWith("copy$default$") => m.getName
    }

    arity == copyMethods.length
  }

  // TODO: this is a hack since we expect the field declaration order to be according to the source but there's no guarantee
  def caseClassInfoInsideACompanionModule(clazz: Class[_], arity: Int): Iterable[String] = {
    // fields are private so use the 'declared' variant
    var counter: Int = 0
    clazz.getDeclaredFields.collect {
      case field if (counter < arity) => counter += 1; field.getName
    }
  }

  def caseClassValues(target: AnyRef, props: Iterable[String]) = {
    val product = target.asInstanceOf[Product].productIterator
    val tuples = for (y <- props) yield (y, product.next)
    tuples.toMap
  }
}