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

package org.elasticsearch.hadoop.qa.kerberos.spark

import java.security.PrivilegedExceptionAction

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.elasticsearch.hadoop.qa.kerberos.security.KeytabLogin
import org.elasticsearch.spark._

class LoadToES(args: Array[String]) {

  val sparkConf: SparkConf = new SparkConf().setAppName("LoadToES")
  val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  def run(): Unit = {
    if (!sparkConf.contains(LoadToES.CONF_FIELD_NAMES)) {
      throw new IllegalArgumentException(LoadToES.CONF_FIELD_NAMES + " is required")
    }
    val resource = sparkConf.get("spark.es.resource")
    val fieldNames = sparkConf.get(LoadToES.CONF_FIELD_NAMES).split(",")

    val df = spark.sqlContext.read.textFile(args(0))

    val parsedData = df.rdd
      .map(line => {
        var record: Map[String, Object] = Map()
        val fields = line.split('\t')
        var fieldNum = 0
        for (field <- fields) {
          if (fieldNum < fieldNames.length) {
            val fieldName = fieldNames(fieldNum)
            record = record + (fieldName -> field)
          }
          fieldNum = fieldNum + 1
        }
        record
      })

    parsedData.saveToEs(resource)
  }
}

object LoadToES {
  val CONF_FIELD_NAMES = "spark.load.field.names"

  def main(args: Array[String]): Unit = {
    KeytabLogin.doAfterLogin(new PrivilegedExceptionAction[Unit] {
      override def run(): Unit = new LoadToES(args).run()
    })
  }
}
