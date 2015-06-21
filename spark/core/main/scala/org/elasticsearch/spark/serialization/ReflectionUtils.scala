package org.elasticsearch.spark.serialization

import java.beans.Introspector
import java.lang.reflect.Method

import scala.collection.mutable.HashMap
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._


import org.apache.commons.logging.LogFactory

private[spark] object ReflectionUtils {

  val caseClassCache = new HashMap[Class[_], (Boolean, Iterable[String])]
  val javaBeanCache = new HashMap[Class[_], Array[(String, Method)]]

  //SI-6240
  protected[spark] object ReflectionLock

  private def checkCaseClass(clazz: Class[_]): Boolean = {
    ReflectionLock.synchronized {
      // reliable case class identifier only happens through class symbols...
      runtimeMirror(clazz.getClassLoader()).classSymbol(clazz).isCaseClass
    }
  }

  private def doGetCaseClassInfo(clazz: Class[_]): Iterable[String] = {
    ReflectionLock.synchronized {
      runtimeMirror(clazz.getClassLoader()).classSymbol(clazz).toType.declarations.collect {
        case m: MethodSymbol if m.isCaseAccessor => m.name.toString()
      }
    }
  }

  private def isCaseClassInsideACompanionModule(clazz: Class[_], arity: Int): Boolean = {
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
  private def caseClassInfoInsideACompanionModule(clazz: Class[_], arity: Int): Iterable[String] = {
    // fields are private so use the 'declared' variant
    var counter: Int = 0
    clazz.getDeclaredFields.collect {
      case field if (counter < arity) => counter += 1; field.getName
    }
  }

  private def doGetCaseClassValues(target: AnyRef, props: Iterable[String]) = {
    val product = target.asInstanceOf[Product].productIterator
    val tuples = for (y <- props) yield (y, product.next)
    tuples.toMap
  }

  private def checkCaseClassCache(p: Product) = {
    caseClassCache.getOrElseUpdate(p.getClass, {
      var isCaseClazz = checkCaseClass(p.getClass)
      var info = if (isCaseClazz) doGetCaseClassInfo(p.getClass) else null
      if (!isCaseClazz) {
        isCaseClazz = isCaseClassInsideACompanionModule(p.getClass, p.productArity)
        if (isCaseClazz) {
          LogFactory.getLog(classOf[ScalaValueWriter]).warn(
              String.format("[%s] is detected as a case class in Java but not in Scala and thus " +
                  "its properties might be detected incorrectly - make sure the @ScalaSignature is available within the class bytecode " +
                  "and/or consider moving the case class from its companion object/module", p.getClass))
        }
        info = if (isCaseClazz) caseClassInfoInsideACompanionModule(p.getClass(), p.productArity) else null
      }

      (isCaseClazz, info)
    })
  }

  def isCaseClass(p: Product) = {
    checkCaseClassCache(p)._1
  }

  def caseClassValues(p: Product) = {
    doGetCaseClassValues(p.asInstanceOf[AnyRef], checkCaseClassCache(p)._2)
  }

  private def checkJavaBeansCache(o: Any) = {
    javaBeanCache.getOrElseUpdate(o.getClass, {
      javaBeansInfo(o.getClass)
    })
  }

  def isJavaBean(value: Any) = {
    !checkJavaBeansCache(value).isEmpty
  }

  def javaBeanAsMap(value: Any) = {
    javaBeansValues(value, checkJavaBeansCache(value))
  }

  private def javaBeansInfo(clazz: Class[_]) = {
    Introspector.getBeanInfo(clazz).getPropertyDescriptors().collect {
      case pd if (pd.getName != "class" && pd.getReadMethod() != null) => (pd.getName, pd.getReadMethod)
    }.sortBy(_._1)
  }

  private def javaBeansValues(target: Any, info: Array[(String, Method)]) = {
    info.map(in => (in._1, in._2.invoke(target))).toMap
  }
}