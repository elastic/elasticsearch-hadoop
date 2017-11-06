package org.elasticsearch.spark.sql.streaming

import org.apache.commons.logging.LogFactory
import org.apache.spark.sql.SparkSession
import org.elasticsearch.hadoop.EsHadoopException
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException

/**
 * Spark Structured Streaming just recently left "Experimental" mode in Spark 2.2.0. Its underlying interfaces have not
 * been subject to the same stability rules as other parts of the project since its debut in 2.0.0. Each point release
 * between 2.0 and 2.2 has introduced breaking changes to the api. This means that even though our 2.x artifact supports
 * all 2.0 and up versions of Spark, we need to ensure that users of Spark Structured Streaming are running on
 * Spark 2.2.0 and above and are not confused about why their streaming jobs are failing if they use versions of Spark
 * that do not match what we can reasonably support.
 */
object StructuredStreamingVersionLock {

  private [this] val LOG = LogFactory.getLog(getClass)

  private [this] val supported = """(2.[2-9].[0-9]|[3-9].[0-9].[0-9]).*""".r

  /**
   * Checks the spark session to make sure that it contains a compatible
   * version.
   */
  def checkCompatibility(session: SparkSession): Unit = {
    try {
      session.version match {
        case supported(version) => if (LOG.isDebugEnabled) LOG.debug(s"Running against supported version of Spark [$version]")
        case _ =>
          throw new EsHadoopIllegalArgumentException(s"Spark version mismatch. Expected at least Spark version [2.2.0] " +
            s"but found [${session.version}]. Spark Structured Streaming is a feature that is only supported on Spark " +
            s"[2.2.0] and up for this version of ES-Hadoop/Spark.")
      }
    } catch {
      case e: EsHadoopException =>
        throw e
      case t: Throwable =>
        throw new EsHadoopIllegalArgumentException("Could not determine the version of Spark for compatibility", t)
    }
  }
}
