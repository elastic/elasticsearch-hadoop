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
