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
package org.elasticsearch.spark.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.elasticsearch.hadoop.serialization.field.FieldExtractor
import org.elasticsearch.spark.serialization.ScalaMapFieldExtractor

class DataFrameFieldExtractor extends ScalaMapFieldExtractor {

  override protected def extractField(target: AnyRef): AnyRef = {
    var obj = target
    for (in <- 0 until getFieldNames.size()) {
      val field = getFieldNames.get(in)
      obj = obj match {
        case (row: Row, struct: StructType) => {
          val index = struct.fieldNames.indexOf(field)
          if (index < 0) {
            FieldExtractor.NOT_FOUND
          } else {
            row(index) match {
              case nestedRow: Row => (nestedRow, struct.fields(index).dataType)
              case anythingElse => anythingElse.asInstanceOf[AnyRef]
            }
          }
        }
        case _ => super.extractField(target)
      }
    }

    // Return the value or unpack the value if it's a row-schema tuple
    obj match {
      case (row: Row, _: StructType) => row
      case any => any
    }
  }
}