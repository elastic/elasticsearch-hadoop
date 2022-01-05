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
    obj
  }
}