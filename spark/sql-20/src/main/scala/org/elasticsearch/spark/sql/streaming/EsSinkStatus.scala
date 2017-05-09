package org.elasticsearch.spark.sql.streaming

/**
 * An envelope of metadata that is stored in the commit log. Mostly contains statistics about
 * what was written in the given batch.
 *
 * @param taskId of the task that wrote this data
 * @param execTimeMillis when the data was written
 * @param resource which resource the data was written to, patterns included
 * @param records number of records written to resource
 */
case class EsSinkStatus(taskId: Int, execTimeMillis: Long, resource: String, records: Long)
