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
package org.elasticsearch.spark

import org.apache.spark.streaming.dstream.DStream
import org.elasticsearch.hadoop.util.ObjectUtils

import scala.reflect.ClassTag
import scala.language.implicitConversions

package object streaming {

  private val init = { ObjectUtils.loadClass("org.elasticsearch.spark.rdd.CompatUtils", classOf[ObjectUtils].getClassLoader)}

  implicit def sparkDStreamFunctions(ds: DStream[_]): SparkDStreamFunctions = new SparkDStreamFunctions(ds)

  class SparkDStreamFunctions(ds: DStream[_]) extends Serializable {
    def saveToEs(resource: String): Unit = { EsSparkStreaming.saveToEs(ds, resource) }
    def saveToEs(resource: String, cfg: Map[String, String]): Unit = { EsSparkStreaming.saveToEs(ds, resource, cfg) }
    def saveToEs(cfg: Map[String, String]): Unit = { EsSparkStreaming.saveToEs(ds, cfg) }
  }

  implicit def sparkStringJsonDStreamFunctions(ds: DStream[String]): SparkJsonDStreamFunctions[String] = new SparkJsonDStreamFunctions[String](ds)
  implicit def sparkByteArrayJsonDStreamFunctions(ds: DStream[Array[Byte]]): SparkJsonDStreamFunctions[Array[Byte]] = new SparkJsonDStreamFunctions[Array[Byte]](ds)

  class SparkJsonDStreamFunctions[T : ClassTag](ds: DStream[T]) extends Serializable {
    def saveJsonToEs(resource: String): Unit = { EsSparkStreaming.saveJsonToEs(ds, resource) }
    def saveJsonToEs(resource: String, cfg: Map[String, String]): Unit = { EsSparkStreaming.saveJsonToEs(ds, resource, cfg) }
    def saveJsonToEs(cfg: Map[String, String]): Unit = { EsSparkStreaming.saveJsonToEs(ds, cfg) }
  }

  implicit def sparkPairDStreamFunctions[K : ClassTag, V : ClassTag](ds: DStream[(K,V)]): SparkPairDStreamFunctions[K,V] = new SparkPairDStreamFunctions[K,V](ds)

  class SparkPairDStreamFunctions[K : ClassTag, V : ClassTag](ds: DStream[(K, V)]) extends Serializable {
    def saveToEsWithMeta(resource: String): Unit = { EsSparkStreaming.saveToEsWithMeta(ds, resource) }
    def saveToEsWithMeta(resource: String, cfg: Map[String, String]): Unit = { EsSparkStreaming.saveToEsWithMeta(ds, resource, cfg) }
    def saveToEsWithMeta(cfg: Map[String, String]): Unit = { EsSparkStreaming.saveToEsWithMeta(ds, cfg) }
  }

}
