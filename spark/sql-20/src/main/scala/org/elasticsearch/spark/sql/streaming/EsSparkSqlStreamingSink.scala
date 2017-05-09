package org.elasticsearch.spark.sql.streaming

import java.util.UUID

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.spark.TaskContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.streaming.MetadataLog
import org.apache.spark.sql.execution.streaming.Sink
import org.elasticsearch.hadoop.cfg.Settings

/**
 * Sink for writing Spark Structured Streaming Queries to an Elasticsearch cluster.
 */
class EsSparkSqlStreamingSink(sparkSession: SparkSession, settings: Settings) extends Sink {

  private val logger: Log = LogFactory.getLog(classOf[EsSparkSqlStreamingSink])

  private val writeLog: MetadataLog[Array[EsSinkStatus]] = {
    if (SparkSqlStreamingConfigs.getSinkLogEnabled(settings)) {
      val logPath = SparkSqlStreamingConfigs.constructCommitLogPath(settings)
      logger.info(s"Using log path of [$logPath]")
      new EsSinkMetadataLog(settings, sparkSession, logPath)
    } else {
      logger.warn("EsSparkSqlStreamingSink is continuing without write commit log. " +
        "Be advised that data may be duplicated!")
      new NullMetadataLog[Array[EsSinkStatus]]()
    }
  }

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (batchId <= writeLog.getLatest().map(_._1).getOrElse(-1L)) {
      logger.info(s"Skipping already committed batch [$batchId]")
    } else {
      val commitProtocol = new EsCommitProtocol(writeLog)
      val queryExecution = data.queryExecution
      val schema = data.schema

      SQLExecution.withNewExecutionId(sparkSession, queryExecution) {
        val queryName = SparkSqlStreamingConfigs.getQueryName(settings).getOrElse(UUID.randomUUID().toString)
        val jobState = JobState(queryName, batchId)
        commitProtocol.initJob(jobState)

        try {
          val serializedSettings = settings.save()
          val taskCommits = sparkSession.sparkContext.runJob(queryExecution.toRdd,
            (taskContext: TaskContext, iter: Iterator[InternalRow]) => {
              new EsStreamQueryWriter(serializedSettings, schema, commitProtocol).run(taskContext, iter)
            }
          )
          commitProtocol.commitJob(jobState, taskCommits)
        } catch {
          case t: Throwable =>
            commitProtocol.abortJob(jobState)
            throw t;
        }
      }
    }
  }
}
