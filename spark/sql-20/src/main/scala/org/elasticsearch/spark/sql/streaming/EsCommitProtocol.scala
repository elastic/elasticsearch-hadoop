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

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.spark.sql.execution.streaming.MetadataLog
import org.elasticsearch.hadoop.util.Assert

/**
 * Defines job-side and task-side logic for committing batches of writes
 * to Elasticsearch for Spark Structured Streaming
 */
class EsCommitProtocol(@transient val commitLog: MetadataLog[Array[EsSinkStatus]]) extends Serializable {

  @transient lazy val logger: Log = LogFactory.getLog(classOf[EsCommitProtocol])

  /**
   * Prepare the driver side of the commit protocol.
   * @param jobState the current Job information
   */
  def initJob(jobState: JobState): Unit = {
    Assert.notNull(commitLog, "commitLog cannot be null")
  }

  /**
   * Commit the results of each of the tasks to the execution log.
   * @param jobState the current Job information
   * @param taskCommits the results from each of the tasks
   */
  def commitJob(jobState: JobState, taskCommits: Seq[TaskCommit]): Unit = {
    val commits = taskCommits.flatMap(_.statuses).toArray[EsSinkStatus]
    if (commitLog.add(jobState.batchId, commits)) {
      logger.debug(s"Committed batch ${jobState.batchId}")
    } else {
      throw new IllegalStateException(s"Batch Id [${jobState.batchId}] is already committed")
    }
  }

  /**
   * Abort this job execution.
   * @param jobState the current Job information
   */
  def abortJob(jobState: JobState): Unit = {
    // Nothing to do, here for extension if needed
  }

  @transient var recordsWritten: Long = _
  @transient var executionStart: Long = _

  /**
   * Initialize the executor side of the commit protocol.
   * @param taskState the current Task information
   */
  def initTask(taskState: TaskState): Unit = {
    recordsWritten = 0L
    executionStart = System.currentTimeMillis()
  }

  /**
   * Signal that a record has been processed in this transaction.
   */
  def recordSeen(): Unit = {
    recordsWritten = recordsWritten + 1
  }

  /**
   * Commit this task's transaction.
   * @param taskState the current Task information
   * @return a summary of this Task's actions as a commit message to be added to the log
   */
  def commitTask(taskState: TaskState): TaskCommit = {
    if (recordsWritten > 0) {
      TaskCommit(Some(EsSinkStatus(taskState.taskId, executionStart, taskState.resource, recordsWritten)))
    } else {
      TaskCommit(None)
    }
  }

  /**
   * Abort this task execution
   * @param taskState the current Task information
   */
  def abortTask(taskState: TaskState): Unit = {
    // Nothing to do, here for extension if needed
  }
}

// Envelope classes for the commit protocol
case class JobState(jobId: String, batchId: Long)
case class TaskState(taskId: Int, resource: String)
case class TaskCommit(statuses: Option[EsSinkStatus])



