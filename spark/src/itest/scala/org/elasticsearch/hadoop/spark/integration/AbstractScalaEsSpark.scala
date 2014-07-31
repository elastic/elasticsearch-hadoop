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
package org.elasticsearch.hadoop.spark.integration;

import java.util.concurrent.TimeUnit;

import scala.annotation.migration
import scala.collection.JavaConversions.propertiesAsScalaMap
import scala.runtime.ScalaRunTime.stringOf

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.elasticsearch.hadoop.mr.RestUtils
import org.elasticsearch.hadoop.spark.sparkContextFunctions
import org.elasticsearch.hadoop.spark.sparkRDDFunctions
import org.elasticsearch.hadoop.util.TestSettings
import org.elasticsearch.hadoop.util.TestUtils
import org.junit.After
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test

class AbstractScalaEsScalaSpark extends Serializable {

    @transient val conf = new SparkConf().setAll(TestSettings.TESTING_PROPS).setMaster("local").setAppName("estest");
    @transient var cfg: SparkConf = null
    @transient var sc: SparkContext = null

    @Before def setup() {
      cfg = conf.clone()
    }

    @After def clean() {
      if (sc != null) {
        sc.stop
        Thread.sleep(TimeUnit.SECONDS.toMillis(2))
      }
    }

    @Test
    def testBasicRead() {
        val sc = new SparkContext(conf)
        val input = TestUtils.sampleArtistsDat()
        val data = sc.textFile(input).cache();

        assertTrue(data.count > 300)
    }

    @Test
    def testEsRDDWrite() {
        val doc1 = Map("one" -> 1, "two" -> 2)
        val doc2 = Map("OTP" -> "Otopeni", "SFO" -> "San Fran")

        sc = new SparkContext(cfg)
        sc.makeRDD(Seq(doc1, doc2)).saveToEs("spark-test/basic-write")
        RestUtils.exists("spark-test/scala-basic-write")
        println(RestUtils.get("spark-test/basic-write/_search?"))
    }

    @Test
    def testEsRDDRead() {
        val target = "spark-test/scala-basic-read";
        RestUtils.touch("spark-test");
        RestUtils.putData(target, "{\"message\" : \"Hello World\",\"message_date\" : \"2014-05-25\"}".getBytes());
        RestUtils.putData(target, "{\"message\" : \"Goodbye World\",\"message_date\" : \"2014-05-25\"}".getBytes());
        RestUtils.refresh("spark-test");

        sc = new SparkContext(cfg);
        val esData = sc.esRDD(target);

        val messages = esData.filter(doc => doc.values.find(_.toString.contains("message")).nonEmpty)

        assertTrue(messages.count() ==  2);
        System.out.println(stringOf(messages.take(10)));
        System.out.println(stringOf(messages));
    }
}