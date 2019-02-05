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

class ReadFromES(args: Array[String]) {

  val sparkConf: SparkConf = new SparkConf().setAppName("ReadFromES")
  val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  def run(): Unit = {
    val resource = sparkConf.get("spark.es.resource")

    spark.sparkContext.esJsonRDD(resource).saveAsTextFile(args(0))
//    spark.sqlContext
//      .read
//      .format("es")
//      .option("es.output.json", "true")
//      .load(resource)
//      .write
//      .text(args(0))
  }
}

object ReadFromES {
  def main(args: Array[String]): Unit = {
    KeytabLogin.doAfterLogin(new PrivilegedExceptionAction[Unit] {
      override def run(): Unit = new ReadFromES(args).run()
    })
  }
}
