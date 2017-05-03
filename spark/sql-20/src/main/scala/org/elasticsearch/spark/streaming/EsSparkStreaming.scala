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

package org.elasticsearch.spark.streaming

import java.util.UUID

import org.apache.spark.streaming.dstream.DStream
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
import org.elasticsearch.hadoop.cfg.InternalConfigurationOptions.INTERNAL_TRANSPORT_POOLING_KEY
import org.elasticsearch.spark.rdd.EsSpark

import scala.collection.Map

object EsSparkStreaming {

  // Save methods
  def saveToEs(ds: DStream[_], resource: String): Unit = {
    saveToEs(ds, Map(ES_RESOURCE_WRITE -> resource))
  }
  def saveToEs(ds: DStream[_], resource: String, cfg: Map[String, String]): Unit = {
    saveToEs(ds, collection.mutable.Map(cfg.toSeq: _*) += (ES_RESOURCE_WRITE -> resource))
  }
  def saveToEs(ds: DStream[_], cfg: Map[String, String]): Unit = {
    doSaveToEs(ds, cfg, hasMeta = false)
  }

  // Save with metadata
  def saveToEsWithMeta[K,V](ds: DStream[(K,V)], resource: String): Unit = {
    saveToEsWithMeta(ds, Map(ES_RESOURCE_WRITE -> resource))
  }
  def saveToEsWithMeta[K,V](ds: DStream[(K,V)], resource: String, cfg: Map[String, String]): Unit = {
    saveToEsWithMeta(ds, collection.mutable.Map(cfg.toSeq: _*) += (ES_RESOURCE_WRITE -> resource))
  }
  def saveToEsWithMeta[K,V](ds: DStream[(K,V)], cfg: Map[String, String]): Unit = {
    doSaveToEs(ds, cfg, hasMeta = true)
  }

  // Save as JSON
  def saveJsonToEs(ds: DStream[_], resource: String): Unit = {
    saveToEs(ds, resource, Map(ES_INPUT_JSON -> true.toString))
  }
  def saveJsonToEs(ds: DStream[_], resource: String, cfg: Map[String, String]): Unit = {
    saveToEs(ds, resource, collection.mutable.Map(cfg.toSeq: _*) += (ES_INPUT_JSON -> true.toString))
  }
  def saveJsonToEs(ds: DStream[_], cfg: Map[String, String]): Unit = {
    saveToEs(ds, collection.mutable.Map(cfg.toSeq: _*) += (ES_INPUT_JSON -> true.toString))
  }

  // Implementation
  def doSaveToEs(ds: DStream[_], cfg: Map[String, String], hasMeta: Boolean): Unit = {
    // Set the transport pooling key and delegate to the standard EsSpark save.
    // IMPORTANT: Do not inline this into the lambda expression below
    val config = collection.mutable.Map(cfg.toSeq: _*) += (INTERNAL_TRANSPORT_POOLING_KEY -> UUID.randomUUID().toString)
    ds.foreachRDD(rdd => EsSpark.doSaveToEs(rdd, config, hasMeta))
  }
}
