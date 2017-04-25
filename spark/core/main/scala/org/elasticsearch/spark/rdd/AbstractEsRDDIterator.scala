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

import org.apache.commons.logging.Log
import org.apache.spark.TaskContext
import org.apache.spark.TaskKilledException
import org.elasticsearch.hadoop.cfg.Settings
import org.elasticsearch.hadoop.rest.RestService
import org.elasticsearch.hadoop.rest.PartitionDefinition

private[spark] abstract class AbstractEsRDDIterator[T](
    val context: TaskContext,
    partition: PartitionDefinition)
  extends Iterator[T] {

  protected var finished = false
  private var gotNext = false
  private var nextValue: T = _
  private var closed = false

  @transient private lazy val log = getLogger()

  private var initialized = false;

  lazy val reader = {
     initialized = true
     val settings = partition.settings()

     // initialize mapping/ scroll reader
     initReader(settings, log)

     val readr = RestService.createReader(settings, partition, log)
     readr.scrollQuery()
  }

  // Register an on-task-completion callback to close the input stream.
  CompatUtils.addOnCompletition(context, () => closeIfNeeded())

  def hasNext: Boolean = {
    if (CompatUtils.isInterrupted(context)) {
      throw new TaskKilledException
    }

    !finished && reader.hasNext()
  }

  override def next(): T = {
    if (!hasNext) {
      throw new NoSuchElementException("End of stream")
    }
    val value = reader.next();
    createValue(value)
  }

  def closeIfNeeded() {
    if (!closed) {
      close()
      closed = true
    }
  }

  protected def close() = {
    if (initialized) {
      reader.close()
    }
  }

  def getLogger(): Log
  def initReader(settings:Settings, log: Log)
  def createValue(value: Array[Object]): T

}