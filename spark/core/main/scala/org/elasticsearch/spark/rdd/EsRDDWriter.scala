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
package org.elasticsearch.spark.rdd

import scala.reflect.ClassTag

import org.apache.commons.logging.LogFactory
import org.apache.spark.TaskContext
import org.elasticsearch.hadoop.cfg.PropertiesSettings
import org.elasticsearch.hadoop.rest.InitializationUtils
import org.elasticsearch.hadoop.rest.RestService
import org.elasticsearch.hadoop.serialization.builder.ValueWriter
import org.elasticsearch.hadoop.serialization.BytesConverter
import org.elasticsearch.hadoop.serialization.field.FieldExtractor
import org.elasticsearch.hadoop.serialization.JdkBytesConverter
import org.elasticsearch.hadoop.util.SettingsUtils
import org.elasticsearch.hadoop.util.ObjectUtils
import org.elasticsearch.spark.serialization.ScalaMapFieldExtractor
import org.elasticsearch.spark.serialization.ScalaMetadataExtractor
import org.elasticsearch.spark.serialization.ScalaValueWriter


private[spark] class EsRDDWriter[T: ClassTag](val serializedSettings: String,
                                              val runtimeMetadata: Boolean = false)
  extends Serializable {

  @transient protected lazy val log = LogFactory.getLog(this.getClass())

  lazy val settings = {
    val settings = new PropertiesSettings().load(serializedSettings);

    InitializationUtils.setValueWriterIfNotSet(settings, valueWriter, log)
    InitializationUtils.setBytesConverterIfNeeded(settings, bytesConverter, log)
    InitializationUtils.setFieldExtractorIfNotSet(settings, fieldExtractor, log)

    settings
  }

  lazy val metaExtractor = new ScalaMetadataExtractor()

  def write(taskContext: TaskContext, data: Iterator[T]) {
    val writer = RestService.createWriter(settings, taskContext.partitionId, -1, log)

    taskContext.addTaskCompletionListener((TaskContext) => writer.close())

    if (runtimeMetadata) {
      writer.repository.addRuntimeFieldExtractor(metaExtractor)
    }

    while (data.hasNext) {
      writer.repository.writeToIndex(processData(data))
    }
  }

  protected def valueWriter: Class[_ <: ValueWriter[_]] = classOf[ScalaValueWriter]
  protected def bytesConverter: Class[_ <: BytesConverter] = classOf[JdkBytesConverter]
  protected def fieldExtractor: Class[_ <: FieldExtractor] = classOf[ScalaMapFieldExtractor]

  protected def processData(data: Iterator[T]): Any = {
    val next = data.next
    if (runtimeMetadata) {
      //TODO: is there a better way to do this cast
      next match {
        case (k, v) =>
          {
            // use the key to extract metadata
            metaExtractor.setObject(k);
            // return the value to be used as the document
            v
          }
      }
    } else {
      next
    }
  }
}