/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.spark.serialization

import scala.collection.Map
import scala.collection.immutable.Nil
import org.elasticsearch.hadoop.serialization.Generator
import org.elasticsearch.hadoop.serialization.builder.JdkValueWriter
import org.elasticsearch.hadoop.serialization.builder.ValueWriter.Result
import org.elasticsearch.spark.serialization.{ReflectionUtils => RU}
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException

import scala.collection.mutable

class ScalaValueWriter(writeUnknownTypes: Boolean = false) extends JdkValueWriter(writeUnknownTypes) {

  /**
   * Used for tracking the serialization of nested POJOs that are treated
   * as JavaBeans. Alias for a mutable HashSet of type Any
   */
  type BeanTracker = mutable.HashSet[Any]

  def this() {
    this(false)
  }

  private[this] val beanTracker = new BeanTracker

  override def write(value: AnyRef, generator: Generator): Result = {
    doWriteScala(value, generator, null)
  }

  override protected def doWrite(value: Any, generator: Generator, parentField: String): Result = {
    doWriteScala(value, generator, parentField)
  }

  private def doWriteScala(value: Any, generator: Generator, parentField:String): Result = {
    value match {
      case null | None | () => generator.writeNull()
      case Nil =>
        generator.writeBeginArray(); generator.writeEndArray()

      case Some(s: AnyRef) => return doWrite(s, generator, parentField)

      case m: Map[_, _] => {
        generator.writeBeginObject()
        for ((k, v) <- m) {
          if (shouldKeep(parentField, k.toString)) {
            generator.writeFieldName(k.toString)
            val result = doWrite(v, generator, k.toString)
            if (!result.isSuccesful) {
              return result
            }
          }
        }
        generator.writeEndObject()
      }

      case i: Traversable[_] => {
        generator.writeBeginArray()
        for (v <- i) {
          val result = doWrite(v, generator, parentField)
          if (!result.isSuccesful) {
            return result
          }
        }
        generator.writeEndArray()
      }

      case b: Array[Byte] => {
        generator.writeBinary(b)
      }

      case i: Array[_] => {
        generator.writeBeginArray()
        for (v <- i) {
          val result = doWrite(v, generator, parentField)
          if (!result.isSuccesful) {
            return result
          }
        }
        generator.writeEndArray()
      }

      case p: Product => {
        // handle case class
        if (RU.isCaseClass(p)) {
          val result = doWrite(RU.caseClassValues(p), generator, parentField)
          if (!result.isSuccesful) {
            return result
          }
        } // normal product - treat it as a list/array
        else {
          generator.writeBeginArray()
          for (t <- p.productIterator) {
            val result = doWrite(t.asInstanceOf[AnyRef], generator, parentField)
            if (!result.isSuccesful) {
              return result
            }
          }
          generator.writeEndArray()
        }
      }

      case _ => {
        // check if it's called by accident on a DataFrame/SchemaRDD (happens)
        if (value.getClass().getName().startsWith("org.apache.spark.sql.")) {
          throw new EsHadoopIllegalArgumentException("Spark SQL types are not handled through basic RDD saveToEs() calls; typically this is a mistake(as the SQL schema will be ignored). Use 'org.elasticsearch.spark.sql' package instead")
        }

        val result = super.doWrite(value, generator, parentField)

        // Normal JDK types failed, try the JavaBean last. The JavaBean logic accepts just about
        // anything, even if it's not a real java bean. Check to see if the value that failed
        // is the same value we're about to treat as a bean. If the failed value is not the current
        // value, then the last call probably failed on a subfield of the current value that
        // couldn't be serialized; There's a chance that we could treat a container object (map,
        // list) like a java bean, which is improper. In these cases we should skip the javabean
        // handling and just return the result
        if (!result.isSuccesful && result.getUnknownValue == value) {
          if (!beanTracker.contains(value) && RU.isJavaBean(value)) {

            // Recursion warning:
            // There's a chance that when we are handed an object, that object has a getter method
            // that returns itself, or an object that contains itself, or any level of self nesting.
            // This can cause stack overflow errors when serializing. Guard against this:

            // First, keep track of objects we've seen, and don't try to serialize them while we're
            // already serializing them
            beanTracker.add(value)
            try {
              // Second, Try to sense the immediate case of self reference and break out early to avoid
              // stack overflow.
              val asMap = RU.javaBeanAsMap(value).filterNot(e => e._2 == value)
              val beanResult = doWrite(asMap, generator, parentField)
              return beanResult
            } finally {
              // Third, Allow usage of the same bean only if it doesn't recurse into itself.
              // This doubles as clean-up logic to avoid having to clear the set every write call.
              beanTracker.remove(value)
            }
          } else {
            return result
          }
        } else {
          return result
        }
      }
    }

    Result.SUCCESFUL()
  }
}
