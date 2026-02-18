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

import java.io._
import java.nio.charset.StandardCharsets

import scala.collection.mutable
import scala.util.Try

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.elasticsearch.hadoop.cfg.Settings

/**
 * Provides logic for managing batch ID committing as well as cleaning up
 * state between batch executions for the [[EsSparkSqlStreamingSink]].
 *
 * This implementation stores metadata as JSON files in a directory,
 * similar to how Spark's internal CompactibleFileStreamLog worked but
 * simplified for our use case.
 */
class EsSinkMetadataLog(settings: Settings, sparkSession: SparkSession, path: String)
  extends MetadataLog[Array[EsSinkStatus]] with Serializable {

  @transient private lazy val logger = LogFactory.getLog(classOf[EsSinkMetadataLog])

  @transient private lazy val fs: FileSystem = {
    val hadoopConf = sparkSession.sessionState.newHadoopConf()
    new Path(path).getFileSystem(hadoopConf)
  }

  @transient private lazy val metadataPath = new Path(path)

  // Ensure the directory exists
  @transient private lazy val initialized: Boolean = {
    if (!fs.exists(metadataPath)) {
      fs.mkdirs(metadataPath)
    }
    true
  }

  private def batchIdToPath(batchId: Long): Path = {
    new Path(metadataPath, batchId.toString)
  }

  override def add(batchId: Long, metadata: Array[EsSinkStatus]): Boolean = {
    if (!initialized) return false

    val batchPath = batchIdToPath(batchId)
    if (fs.exists(batchPath)) {
      // Batch already exists
      return false
    }

    try {
      val outputStream = fs.create(batchPath, false)
      try {
        // Write a simple format: taskId, execTimeMillis, resource, records per line (tab-separated)
        val writer = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8)
        metadata.foreach { status =>
          writer.write(s"${status.taskId}\t${status.execTimeMillis}\t${status.resource}\t${status.records}\n")
        }
        writer.flush()
        true
      } finally {
        outputStream.close()
      }
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to write metadata for batch $batchId", e)
        false
    }
  }

  override def get(batchId: Long): Option[Array[EsSinkStatus]] = {
    if (!initialized) return None

    val batchPath = batchIdToPath(batchId)
    if (!fs.exists(batchPath)) {
      return None
    }

    try {
      val inputStream = fs.open(batchPath)
      try {
        val reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
        val statuses = mutable.ArrayBuffer[EsSinkStatus]()
        var line = reader.readLine()
        while (line != null) {
          val parts = line.split("\t")
          if (parts.length >= 4) {
            statuses += EsSinkStatus(parts(0).toInt, parts(1).toLong, parts(2), parts(3).toLong)
          }
          line = reader.readLine()
        }
        Some(statuses.toArray)
      } finally {
        inputStream.close()
      }
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to read metadata for batch $batchId", e)
        None
    }
  }

  override def get(startId: Option[Long], endId: Option[Long]): Array[(Long, Array[EsSinkStatus])] = {
    if (!initialized) return Array()

    try {
      val files = fs.listStatus(metadataPath)
      files
        .filter(_.isFile)
        .flatMap { status =>
          Try(status.getPath.getName.toLong).toOption.map(id => (id, status))
        }
        .filter { case (id, _) =>
          startId.forall(_ <= id) && endId.forall(_ >= id)
        }
        .sortBy(_._1)
        .flatMap { case (id, _) =>
          get(id).map(metadata => (id, metadata))
        }
    } catch {
      case e: Exception =>
        logger.warn("Failed to list metadata files", e)
        Array()
    }
  }

  override def getLatest(): Option[(Long, Array[EsSinkStatus])] = {
    if (!initialized) return None

    try {
      val files = fs.listStatus(metadataPath)
      val latestId = files
        .filter(_.isFile)
        .flatMap(status => Try(status.getPath.getName.toLong).toOption)
        .sorted
        .lastOption

      latestId.flatMap(id => get(id).map(metadata => (id, metadata)))
    } catch {
      case e: Exception =>
        logger.warn("Failed to get latest metadata", e)
        None
    }
  }

  override def purge(thresholdBatchId: Long): Unit = {
    if (!initialized) return

    try {
      val files = fs.listStatus(metadataPath)
      files
        .filter(_.isFile)
        .flatMap(status => Try(status.getPath.getName.toLong).toOption.map(id => (id, status.getPath)))
        .filter { case (id, _) => id < thresholdBatchId }
        .foreach { case (_, filePath) =>
          try {
            fs.delete(filePath, false)
          } catch {
            case e: Exception =>
              logger.warn(s"Failed to delete metadata file $filePath", e)
          }
        }
    } catch {
      case e: Exception =>
        logger.warn("Failed to purge metadata files", e)
    }
  }
}

/**
 * Companion object for [[EsSinkMetadataLog]].
 */
object EsSinkMetadataLog {
  private [sql] val VERSION_NUMBER = 1
}
