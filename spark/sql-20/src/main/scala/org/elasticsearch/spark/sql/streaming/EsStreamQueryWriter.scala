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

package org.elasticsearch.spark.sql.streaming

import org.apache.spark.TaskContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException
import org.elasticsearch.hadoop.serialization.BytesConverter
import org.elasticsearch.hadoop.serialization.JdkBytesConverter
import org.elasticsearch.hadoop.serialization.builder.ValueWriter
import org.elasticsearch.hadoop.serialization.field.FieldExtractor
import org.elasticsearch.spark.rdd.EsRDDWriter
import org.elasticsearch.spark.sql.DataFrameFieldExtractor
import org.elasticsearch.spark.sql.DataFrameValueWriter

/**
 * Takes in iterator of InternalRow objects from a partition of data, writes it to Elasticsearch, and manages
 * the streaming commit protocol.
 */
private [sql] class EsStreamQueryWriter(serializedSettings: String,
                                        schema: StructType,
                                        commitProtocol: EsCommitProtocol)
  extends EsRDDWriter[InternalRow](serializedSettings) {

  override protected def valueWriter: Class[_ <: ValueWriter[_]] = classOf[DataFrameValueWriter]
  override protected def bytesConverter: Class[_ <: BytesConverter] = classOf[JdkBytesConverter]
  override protected def fieldExtractor: Class[_ <: FieldExtractor] = classOf[DataFrameFieldExtractor]

  private val encoder: ExpressionEncoder[Row] = RowEncoder(schema).resolveAndBind()

  override def write(taskContext: TaskContext, data: Iterator[InternalRow]): Unit = {
    // Keep clients from using this method, doesn't return task commit information.
    throw new EsHadoopIllegalArgumentException("Use run(taskContext, data) instead to retrieve the commit information")
  }

  def run(taskContext: TaskContext, data: Iterator[InternalRow]): TaskCommit = {
    val taskInfo = TaskState(taskContext.partitionId(), settings.getResourceWrite)
    commitProtocol.initTask(taskInfo)
    try {
      super.write(taskContext, data)
    } catch {
      case t: Throwable =>
        commitProtocol.abortTask(taskInfo)
        throw t
    }
    commitProtocol.commitTask(taskInfo)
  }

  override protected def processData(data: Iterator[InternalRow]): Any = {
    val row = encoder.fromRow(data.next())
    commitProtocol.recordSeen()
    (row, schema)
  }
}
