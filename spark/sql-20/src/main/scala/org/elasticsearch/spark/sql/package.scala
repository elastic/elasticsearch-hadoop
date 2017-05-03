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
package org.elasticsearch.spark;

import scala.language.implicitConversions

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.elasticsearch.spark.sql.EsSparkSQL
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import scala.reflect.ClassTag

package object sql {

  implicit def sqlContextFunctions(sc: SQLContext)= new SQLContextFunctions(sc)

  class SQLContextFunctions(sc: SQLContext) extends Serializable {
    def esDF() = EsSparkSQL.esDF(sc)
    def esDF(resource: String) = EsSparkSQL.esDF(sc, resource)
    def esDF(resource: String, query: String) = EsSparkSQL.esDF(sc, resource, query)
    def esDF(cfg: scala.collection.Map[String, String]) = EsSparkSQL.esDF(sc, cfg)
    def esDF(resource: String, cfg: scala.collection.Map[String, String]) = EsSparkSQL.esDF(sc, resource, cfg)
    def esDF(resource: String, query: String, cfg: scala.collection.Map[String, String]) = EsSparkSQL.esDF(sc, resource, query, cfg)
  }

  // the sparkDatasetFunctions already takes care of this
  // but older clients might still import it hence why it's still here
  implicit def sparkDataFrameFunctions(df: DataFrame) = new SparkDataFrameFunctions(df)

  class SparkDataFrameFunctions(df: DataFrame) extends Serializable {
    def saveToEs(resource: String): Unit = { EsSparkSQL.saveToEs(df, resource) }
    def saveToEs(resource: String, cfg: scala.collection.Map[String, String]): Unit = { EsSparkSQL.saveToEs(df, resource, cfg) }
    def saveToEs(cfg: scala.collection.Map[String, String]): Unit = { EsSparkSQL.saveToEs(df, cfg)    }
  }
  
  implicit def sparkSessionFunctions(ss: SparkSession)= new SparkSessionFunctions(ss)
  
  class SparkSessionFunctions(ss: SparkSession) extends Serializable {
    def esDF() = EsSparkSQL.esDF(ss)
    def esDF(resource: String) = EsSparkSQL.esDF(ss, resource)
    def esDF(resource: String, query: String) = EsSparkSQL.esDF(ss, resource, query)
    def esDF(cfg: scala.collection.Map[String, String]) = EsSparkSQL.esDF(ss, cfg)
    def esDF(resource: String, cfg: scala.collection.Map[String, String]) = EsSparkSQL.esDF(ss, resource, cfg)
    def esDF(resource: String, query: String, cfg: scala.collection.Map[String, String]) = EsSparkSQL.esDF(ss, resource, query, cfg)
  }

  implicit def sparkDatasetFunctions[T : ClassTag](ds: Dataset[T]) = new SparkDatasetFunctions(ds)
  
  class SparkDatasetFunctions[T : ClassTag](ds: Dataset[T]) extends Serializable {
    def saveToEs(resource: String): Unit =  { EsSparkSQL.saveToEs(ds, resource) }
    def saveToEs(resource: String, cfg: scala.collection.Map[String, String]): Unit =  { EsSparkSQL.saveToEs(ds, resource, cfg) }
    def saveToEs(cfg: scala.collection.Map[String, String]): Unit =  { EsSparkSQL.saveToEs(ds, cfg)    }
  }
}