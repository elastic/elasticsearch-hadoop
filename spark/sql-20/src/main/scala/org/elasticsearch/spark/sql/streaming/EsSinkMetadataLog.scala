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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.CompactibleFileStreamLog
import org.elasticsearch.hadoop.cfg.Settings

/**
 * Provides logic for managing batch ID committing as well as cleaning up
 * state between batch executions for the [[EsSparkSqlStreamingSink]]
 */
class EsSinkMetadataLog(settings: Settings, sparkSession: SparkSession, path: String)
  extends CompactibleFileStreamLog[EsSinkStatus](EsSinkMetadataLog.VERSION_NUMBER, sparkSession, path) {

  override protected def fileCleanupDelayMs: Long = SparkSqlStreamingConfigs.getFileCleanupDelayMs(settings)

  override protected def isDeletingExpiredLog: Boolean = SparkSqlStreamingConfigs.getIsDeletingExpiredLog(settings)

  override protected def defaultCompactInterval: Int = SparkSqlStreamingConfigs.getDefaultCompactInterval(settings)

  override def compactLogs(logs: Seq[EsSinkStatus]): Seq[EsSinkStatus] = logs
}

/**
 * Companion object for [[EsSinkMetadataLog]].
 */
object EsSinkMetadataLog {
  private [sql] val VERSION_NUMBER = 1
}
