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

import java.util.EnumMap
import java.util.{Map => JMap}

import scala.collection.{Map => SMap}

import org.elasticsearch.hadoop.serialization.bulk.MetadataExtractor.{Metadata => InternalMetadata}
import org.elasticsearch.hadoop.serialization.bulk.PerEntityPoolingMetadataExtractor
import org.elasticsearch.spark.rdd.{Metadata => SparkMetadata}

private[spark] class ScalaMetadataExtractor extends PerEntityPoolingMetadataExtractor {

  override def getValue(metadata: InternalMetadata): AnyRef = {
    val sparkEnum = ScalaMetadataExtractor.toSparkEnum(metadata)

    if (sparkEnum == null){
      null
    } else {
      entity match {
        case jmap: JMap[_, _] => jmap.asInstanceOf[JMap[SparkMetadata, AnyRef]].get(sparkEnum)
        case smap: SMap[_, _] => smap.asInstanceOf[SMap[SparkMetadata, AnyRef]].getOrElse(sparkEnum, null)
        case _                => if (sparkEnum == SparkMetadata.ID) entity else null
      }
    }
  }
}

object ScalaMetadataExtractor {
  val map = new EnumMap[InternalMetadata, SparkMetadata](classOf[InternalMetadata])

  for (e <- SparkMetadata.values) {
    map.put(InternalMetadata.valueOf(e.name()), e)
  }

  def toSparkEnum(metadata: InternalMetadata) = map.get(metadata)
}