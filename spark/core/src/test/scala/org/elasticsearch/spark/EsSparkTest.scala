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

import org.apache.commons.logging.Log
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkConf, SparkContext, TaskContext}
import org.elasticsearch.hadoop.cfg.Settings
import org.elasticsearch.hadoop.rest.{RestRepository, RestService}
import org.elasticsearch.spark.rdd.{EsRDDWriter, EsSpark}
import org.mockito.Mockito
import org.mockito.ArgumentMatchers
import org.junit.{Assert, Test}

import java.util.concurrent.atomic.AtomicReference
import scala.reflect.ClassTag

class EsSparkTest {
  @Test
  def testSaveToEs(): Unit = {
    val rdd: RDD[String] = Mockito.mock(classOf[RDD[String]])
    val partition: Partition = Mockito.mock(classOf[Partition])
    val partitions: Array[Partition] = Array(partition)
    Mockito.when(rdd.partitions).thenReturn(partitions)
    Mockito.when(rdd.toLocalIterator).thenReturn(Iterator("a", "number", "of", "words"))
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.set("spark.jars", "")
    sparkConf.set("spark.master", "local")
    sparkConf.set("spark.app.name", "spark app name")
    sparkConf.set("spark.ui.enabled", "false")
    sparkConf.set("spark.scheduler.listenerbus.eventqueue.capacity", "5")
    val sparkContext: SparkContext = new TestSparkContext(sparkConf)
    Mockito.when(rdd.sparkContext).thenReturn(sparkContext)

    val restRepository = Mockito.mock(classOf[RestRepository])
    val partitionWriter = Mockito.mock(classOf[RestService.PartitionWriter])
    Mockito.when(partitionWriter.getRepository).thenReturn(restRepository)
    val resultSettingsAtomicReference = new AtomicReference[Settings]
    val partitionWriterProvider = (settings: Settings, currentSplit: Long, totalSplits: Int, log: Log) => {
      def getPartitionWriter(settings: Settings, currentSplit: Long, totalSplits: Int, log: Log) = {
        resultSettingsAtomicReference.set(settings)
        partitionWriter
      }

      getPartitionWriter(settings, currentSplit, totalSplits, log)
    }
    val esRDDWriterProvider = (serializedSettings: String, runtimeMetadata: Boolean) => {
      def getEsRDDWriter(serializedSettings: String, runtimeMetadata: Boolean) = {
        new EsRDDWriter[String](serializedSettings, runtimeMetadata, partitionWriterProvider)
      }

      getEsRDDWriter(serializedSettings, runtimeMetadata)
    }
    EsSpark.clusterDiscoverer = (settings: Settings, log: Log) => {}
    EsSpark.saveToEs(rdd, "something", esRDDWriterProvider)
    val resultSettings = resultSettingsAtomicReference.get
    val opaqueId = resultSettings.getProperty("es.net.http.header.X-Opaque-ID")
    Assert.assertTrue(opaqueId.startsWith("spark application local-"))
    Assert.assertTrue(opaqueId.endsWith(", task attempt 0"))
  }

}

class TestSparkContext(config: SparkConf) extends SparkContext(config) {
  override def runJob[T, U: ClassTag](rdd: RDD[T], func: (TaskContext, Iterator[T]) => U): Array[U] = {
    val taskContext: TaskContext = Mockito.mock(classOf[TaskContext])
    func.apply(taskContext, rdd.toLocalIterator)
    null
  }
}