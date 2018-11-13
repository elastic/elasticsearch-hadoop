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

package org.elasticsearch.hadoop.qa.kerberos.spark.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class SparkWordCount(args: Array[String]) {

  val sparkConf: SparkConf = new SparkConf().setAppName("WordCount")
  val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  def run(): Unit = {
    val df = spark.sqlContext.read.textFile("/data/pride_and_prejudice.txt")

    val wordCount = df.rdd
      .flatMap(line => line.split(' '))
      .map(word => word.replace(",", ""))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .map(t => t._1 + "," + t._2)

    wordCount.saveAsTextFile("/output/pride2")

    spark.stop()
  }
}

object SparkWordCount {
  def main(args: Array[String]): Unit = {
    new SparkWordCount(args).run()
    System.exit(0)
  }
}
