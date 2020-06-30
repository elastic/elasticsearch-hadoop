package org.elasticsearch.spark.sql.streaming

import org.apache.spark.sql.execution.streaming.MetadataLog

/**
 * A null object style metadata log that discards incoming data, and otherwise
 * acts like an empty log.
 */
class NullMetadataLog[T] extends MetadataLog[T] with Serializable {

  // Always return successful storage
  override def add(batchId: Long, metadata: T): Boolean = true

  override def get(batchId: Long): Option[T] = None

  override def get(startId: Option[Long], endId: Option[Long]): Array[(Long, T)] = Array()

  override def getLatest(): Option[(Long, T)] = None

  override def purge(thresholdBatchId: Long): Unit = ()

}
