package org.elasticsearch.spark.sql

import scala.collection.JavaConverters.mapAsJavaMapConverter
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._
import org.elasticsearch.spark.cfg.SparkSettingsManager
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.hadoop.util.StringUtils
import org.apache.spark.rdd.RDD
import scala.collection.mutable.LinkedHashMap


private[sql] class DefaultSource extends RelationProvider {
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {

    val params = parameters map { case (k, v) => if (k.startsWith("es.")) (k, v) else ("es." + k, v)}
    params.getOrElse("es.resource", sys.error("resource must be specified for Elasticsearch resources."))
    ElasticsearchRelation(params)(sqlContext)
  }
}

private [sql] case class ElasticsearchRelation(parameters: Map[String, String])
	(@transient val sqlContext: SQLContext)
  extends PrunedScan {  

  @transient lazy val cfg = {
    new SparkSettingsManager().load(sqlContext.sparkContext.getConf).merge(parameters.asJava)
  }
  
  @transient lazy val lazySchema = {
    MappingUtils.discoverMapping(cfg)
  }
  
  override val schema = lazySchema
  
  // TableScan
  def buildScan() = new ScalaEsRowRDD(sqlContext.sparkContext, parameters)
 
  // PrunedScan
  def buildScan(requiredColumns: Array[String]) = {
    val paramWithProjection = LinkedHashMap[String, String]() ++ parameters
    paramWithProjection += (ConfigurationOptions.ES_SCROLL_FIELDS -> StringUtils.concatenate(requiredColumns.asInstanceOf[Array[Object]], ","))
    new ScalaEsRowRDD(sqlContext.sparkContext, paramWithProjection)
  }
  
  // PrunedFilteredScan
  def buildScan(requiredColumns: Array[String], filters: Array[Filter]) = {
    val paramWithProjection = LinkedHashMap[String, String]() ++ parameters
    paramWithProjection += (ConfigurationOptions.ES_SCROLL_FIELDS -> StringUtils.concatenate(requiredColumns.asInstanceOf[Array[Object]], ","))
    
    // build query based on filters
    new ScalaEsRowRDD(sqlContext.sparkContext, paramWithProjection)
  }
}

