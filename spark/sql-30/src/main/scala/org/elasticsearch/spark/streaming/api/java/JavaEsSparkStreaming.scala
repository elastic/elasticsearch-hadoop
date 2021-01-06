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

package org.elasticsearch.spark.streaming.api.java

import java.util.{Map => JMap}

import org.apache.spark.streaming.api.java.{JavaDStream, JavaPairDStream}

import scala.collection.JavaConverters.mapAsScalaMapConverter
import org.elasticsearch.spark.streaming.EsSparkStreaming

object JavaEsSparkStreaming {

  // specify the return types to make sure the bytecode is generated properly (w/o any scala.collections in it)
  def saveToEs(ds: JavaDStream[_], resource: String): Unit = EsSparkStreaming.saveToEs(ds.dstream, resource)
  def saveToEs(ds: JavaDStream[_], resource: String, cfg: JMap[String, String]): Unit = EsSparkStreaming.saveToEs(ds.dstream, resource, cfg.asScala)
  def saveToEs(ds: JavaDStream[_], cfg: JMap[String, String]): Unit = EsSparkStreaming.saveToEs(ds.dstream, cfg.asScala)

  def saveToEsWithMeta[K, V](ds: JavaPairDStream[K, V], resource: String): Unit = EsSparkStreaming.saveToEsWithMeta(ds.dstream, resource)
  def saveToEsWithMeta[K, V](ds: JavaPairDStream[K, V], resource: String, cfg: JMap[String, String]): Unit = EsSparkStreaming.saveToEsWithMeta(ds.dstream, resource, cfg.asScala)
  def saveToEsWithMeta[K, V](ds: JavaPairDStream[K, V], cfg: JMap[String, String]): Unit = EsSparkStreaming.saveToEsWithMeta(ds.dstream, cfg.asScala)

  def saveJsonToEs(ds: JavaDStream[String], resource: String): Unit = EsSparkStreaming.saveJsonToEs(ds.dstream, resource)
  def saveJsonToEs(ds: JavaDStream[String], resource: String, cfg: JMap[String, String]): Unit = EsSparkStreaming.saveJsonToEs(ds.dstream, resource, cfg.asScala)
  def saveJsonToEs(ds: JavaDStream[String], cfg: JMap[String, String]): Unit = EsSparkStreaming.saveJsonToEs(ds.dstream, cfg.asScala)

  def saveJsonByteArrayToEs(ds: JavaDStream[Array[Byte]], resource: String): Unit = EsSparkStreaming.saveJsonToEs(ds.dstream, resource)
  def saveJsonByteArrayToEs(ds: JavaDStream[Array[Byte]], resource: String, cfg: JMap[String, String]): Unit = EsSparkStreaming.saveJsonToEs(ds.dstream, resource, cfg.asScala)
  def saveJsonByteArrayToEs(ds: JavaDStream[Array[Byte]], cfg: JMap[String, String]): Unit = EsSparkStreaming.saveJsonToEs(ds.dstream, cfg.asScala)
}