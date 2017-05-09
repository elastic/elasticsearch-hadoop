package org.elasticsearch.spark.sql.streaming

import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.internal.SQLConf
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException
import org.elasticsearch.hadoop.cfg.Settings
import org.elasticsearch.hadoop.util.unit.TimeValue

/**
 * Configurations specifically used for Spark Structured Streaming
 */
object SparkSqlStreamingConfigs {

  val ES_SINK_LOG_ENABLE: String = "es.spark.sql.streaming.sink.log.enabled"
  val ES_SINK_LOG_ENABLE_DEFAULT: Boolean = true

  val ES_SINK_LOG_PATH: String = "es.spark.sql.streaming.sink.log.path"

  val ES_INTERNAL_APP_NAME: String = "es.internal.spark.sql.streaming.appName"
  val ES_INTERNAL_APP_ID: String = "es.internal.spark.sql.streaming.appID"
  val ES_INTERNAL_QUERY_NAME: String = "es.internal.spark.sql.streaming.queryName"
  val ES_INTERNAL_USER_CHECKPOINT_LOCATION: String = "es.internal.spark.sql.streaming.userCheckpointLocation"
  val ES_INTERNAL_SESSION_CHECKPOINT_LOCATION: String = "es.internal.spark.sql.streaming.sessionCheckpointLocation"

  val ES_SINK_LOG_CLEANUP_DELAY: String = "es.spark.sql.streaming.sink.log.cleanupDelay"
  val ES_SINK_LOG_CLEANUP_DELAY_DEFAULT: Long = TimeUnit.MINUTES.toMillis(10)

  val ES_SINK_LOG_DELETION: String = "es.spark.sql.streaming.sink.log.deletion"
  val ES_SINK_LOG_DELETION_DEFAULT: Boolean = true

  val ES_SINK_LOG_COMPACT_INTERVAL: String = "es.spark.sql.streaming.sink.log.compactInterval"
  val ES_SINK_LOG_COMPACT_INTERVAL_DEFAULT: Int = 10

  /**
   * Determines if we should use the commit log for writes, or if we should go without one.
   * @param settings connector settings
   * @return true if we should use the commit log, false if we should not
   */
  def getSinkLogEnabled(settings: Settings): Boolean = {
    Option(settings.getProperty(ES_SINK_LOG_ENABLE)).map(_.toBoolean)
      .getOrElse(ES_SINK_LOG_ENABLE_DEFAULT)
  }

  /**
   * Determines the location of the streaming commit log.
   * @param settings connector settings
   * @return the location to use as the commit log
   */
  def constructCommitLogPath(settings: Settings): String = {
    val logPath = getLogPath(settings)
    val userCheckpointLocation = getUserSpecifiedCheckpointLocation(settings)
    val sessionCheckpointLocation = getSessionCheckpointLocation(settings)
    val queryName = getQueryName(settings)

    (logPath, userCheckpointLocation, sessionCheckpointLocation, queryName) match {
      // Case 1) /{log.path}/{batchFiles}
      // If a user gives an explicit log location with ES_SINK_LOG_PATH, then it's fine to store the commit
      // files in the root of that. That directory should be just for us.
      case (Some(explicitPath), _, _, _) => explicitPath
      // Case 2) /{checkpointLocation}/sinks/es
      // Don't store commit files for the log in the root of the checkpoint location; There are other directories
      // inside that root like '/sources', '/metadata', '/state', '/offsets', etc.
      // Instead, nest it under '/sinks/elasticsearch'.
      case (None, Some(userCheckpoint), _, _) => s"$userCheckpoint/sinks/elasticsearch"
      // Case 3) /{sessionCheckpointLocation}/{UUID}
      // Spark lets you define a common location to store checkpoint locations for an entire session. Checkpoint
      // locations are then keyed by their query names. In the event of no query name being present, the query manager
      // creates a random UUID to store the checkpoint in. Not great, since you can't recover easily, but we'll follow
      // suit anyway.
      case (None, None, Some(sessionCheckpoint), None) => s"$sessionCheckpoint/${UUID.randomUUID().toString}/sinks/elasticsearch"
      // Case 4) /{sessionCheckpointLocation}/{queryName}
      // Same as above, but the query name is specified.
      case (None, None, Some(sessionCheckpoint), Some(query)) => s"$sessionCheckpoint/$query/sinks/elasticsearch"
      // Case 5) throw because we don't know where to store things...
      case (None, None, None, _) => throw new EsHadoopIllegalArgumentException(
        "Could not determine path for the Elasticsearch commit log. Specify the commit log location by setting the " +
        "[checkpointLocation] option on your DataStreamWriter. If you do not want to persist the Elasticsearch " +
        "commit log in the regular checkpoint location for your streaming query then you can specify a location to " +
        s"store the log with [$ES_SINK_LOG_PATH], or disable the commit log by setting [$ES_SINK_LOG_ENABLE] to false.")
    }
  }

  /**
   * The log path, if set, is the complete log path to be used for the commit log.
   * @param settings connector settings
   * @return either Some log path or None
   */
  def getLogPath(settings: Settings): Option[String] = {
    Option(settings.getProperty(ES_SINK_LOG_PATH))
  }

  /**
   * The name of the current Spark application, if set
   * @param settings connector settings
   * @return Some name or None
   */
  def getAppName(settings: Settings): Option[String] = {
    Option(settings.getProperty(ES_INTERNAL_APP_NAME))
  }

  /**
   * The ID of the current Spark application, if set
   * @param settings connector settings
   * @return Some id or None
   */
  def getAppId(settings: Settings): Option[String] = {
    Option(settings.getProperty(ES_INTERNAL_APP_ID))
  }

  /**
   * The name of the current Spark Streaming Query, if set
   * @param settings connector settings
   * @return Some query name or None
   */
  def getQueryName(settings: Settings): Option[String] = {
    Option(settings.getProperty(ES_INTERNAL_QUERY_NAME))
  }

  /**
   * The name of the user specified checkpoint location for the current Spark Streaming Query, if set
   * @param settings connector settings
   * @return Some checkpoint location or None
   */
  def getUserSpecifiedCheckpointLocation(settings: Settings): Option[String] = {
    Option(settings.getProperty(ES_INTERNAL_USER_CHECKPOINT_LOCATION))
  }

  /**
   * The name of the default session checkpoint location, if set
   * @param settings connector settings
   * @return Some checkpoint location or None
   */
  def getSessionCheckpointLocation(settings: Settings): Option[String] = {
    Option(settings.getProperty(ES_INTERNAL_SESSION_CHECKPOINT_LOCATION))
  }

  /**
   * The number of milliseconds to wait before cleaning up compacted log files
   * @param settings connector settings
   * @return time in millis if set, or the default delay
   */
  def getFileCleanupDelayMs(settings: Settings): Long =
    Option(settings.getProperty(ES_SINK_LOG_CLEANUP_DELAY)).map(TimeValue.parseTimeValue(_).getMillis)
      .orElse(SQLConf.FILE_SINK_LOG_CLEANUP_DELAY.defaultValue)
      .getOrElse(ES_SINK_LOG_CLEANUP_DELAY_DEFAULT)

  /**
   *
   * @param settings connector settings
   * @return
   */
  def getIsDeletingExpiredLog(settings: Settings): Boolean =
    Option(settings.getProperty(ES_SINK_LOG_DELETION)).map(_.toBoolean)
      .orElse(SQLConf.FILE_SINK_LOG_DELETION.defaultValue)
      .getOrElse(ES_SINK_LOG_DELETION_DEFAULT)

  /**
   *
   * @param settings connector settings
   * @return
   */
  def getDefaultCompactInterval(settings: Settings): Int =
    Option(settings.getProperty(ES_SINK_LOG_COMPACT_INTERVAL)).map(_.toInt)
      .orElse(SQLConf.FILE_SINK_LOG_COMPACT_INTERVAL.defaultValue)
      .getOrElse(ES_SINK_LOG_COMPACT_INTERVAL_DEFAULT)

}
