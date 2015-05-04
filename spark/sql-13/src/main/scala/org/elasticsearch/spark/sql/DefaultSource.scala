package org.elasticsearch.spark.sql

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.mutable.LinkedHashMap

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.PrunedScan
import org.apache.spark.sql.sources.RelationProvider
import org.apache.spark.sql.types.StructType
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.hadoop.util.StringUtils
import org.elasticsearch.spark.cfg.SparkSettingsManager

private[sql] class DefaultSource extends RelationProvider {
  override def createRelation(
    @transient sqlContext: SQLContext,
    parameters: Map[String, String]): BaseRelation = {

    // . seems to be problematic when specifying the options
    val params = parameters.map { case (k, v) => (k.replace('_', '.'), v)}. map { case (k, v) =>
      if (k.startsWith("es.")) (k, v)
      else if (k == "path") ("es.resource", v)
      else ("es." + k, v) }
    params.getOrElse("es.resource", sys.error("resource must be specified for Elasticsearch resources."))
    ElasticsearchRelation(params)(sqlContext)
  }
}

private[sql] case class ElasticsearchRelation(parameters: Map[String, String])(@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedScan //with InsertableRelation
  {

  @transient lazy val cfg = {
    new SparkSettingsManager().load(sqlContext.sparkContext.getConf).merge(parameters.asJava)
  }

  @transient lazy val lazySchema = {
    MappingUtils.discoverMapping(cfg)
  }

  override val schema = lazySchema.struct

  // TableScan
  def buildScan() = new ScalaEsRowRDD(sqlContext.sparkContext, parameters, lazySchema)

  // PrunedScan
  def buildScan(requiredColumns: Array[String]) = {
    val paramWithProjection = LinkedHashMap[String, String]() ++ parameters
    paramWithProjection += (ConfigurationOptions.ES_SCROLL_FIELDS -> StringUtils.concatenate(requiredColumns.asInstanceOf[Array[Object]], StringUtils.DEFAULT_DELIMITER))
    
    if (cfg.getReadMetadata) {
      val metadata = cfg.getReadMetadataField
      // if metadata is not selected, don't ask for it
      if (!requiredColumns.contains(metadata)) {
        paramWithProjection += (ConfigurationOptions.ES_READ_METADATA -> false.toString())
      }
    }
    new ScalaEsRowRDD(sqlContext.sparkContext, paramWithProjection, lazySchema)
  }

  def insert(data: DataFrame, overwrite: Boolean) {
    throw new UnsupportedOperationException()
  }
}
