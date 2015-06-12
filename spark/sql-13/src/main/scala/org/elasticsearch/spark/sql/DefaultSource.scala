package org.elasticsearch.spark.sql

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.mutable.LinkedHashMap

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.And
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.EqualTo
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.GreaterThan
import org.apache.spark.sql.sources.GreaterThanOrEqual
import org.apache.spark.sql.sources.In
import org.apache.spark.sql.sources.IsNotNull
import org.apache.spark.sql.sources.IsNull
import org.apache.spark.sql.sources.LessThan
import org.apache.spark.sql.sources.LessThanOrEqual
import org.apache.spark.sql.sources.Not
import org.apache.spark.sql.sources.Or
import org.apache.spark.sql.sources.PrunedFilteredScan
import org.apache.spark.sql.sources.PrunedScan
import org.apache.spark.sql.sources.RelationProvider
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.hadoop.cfg.InternalConfigurationOptions
import org.elasticsearch.hadoop.serialization.json.JacksonJsonGenerator
import org.elasticsearch.hadoop.util.FastByteArrayOutputStream
import org.elasticsearch.hadoop.util.IOUtils
import org.elasticsearch.hadoop.util.StringUtils
import org.elasticsearch.spark.cfg.SparkSettingsManager
import org.elasticsearch.spark.serialization.ScalaValueWriter

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
  extends BaseRelation with PrunedScan with PrunedFilteredScan  //with InsertableRelation
  {

  @transient lazy val cfg = {
    new SparkSettingsManager().load(sqlContext.sparkContext.getConf).merge(parameters.asJava)
  }

  @transient lazy val lazySchema = {
    MappingUtils.discoverMapping(cfg)
  }

  @transient lazy val valueWriter = new ScalaValueWriter

  override val schema = lazySchema.struct

  // TableScan
  def buildScan() = new ScalaEsRowRDD(sqlContext.sparkContext, parameters, lazySchema)

  // PrunedScan
  def buildScan(requiredColumns: Array[String]) = {
    buildScan(requiredColumns, null)
  }

  // PrunedFilteredScan
  def buildScan(requiredColumns: Array[String], filters: Array[Filter]) = {
    val paramWithScan = LinkedHashMap[String, String]() ++ parameters
    paramWithScan += (ConfigurationOptions.ES_SCROLL_FIELDS -> StringUtils.concatenate(requiredColumns.asInstanceOf[Array[Object]], StringUtils.DEFAULT_DELIMITER))

    if (cfg.getReadMetadata) {
      val metadata = cfg.getReadMetadataField
      // if metadata is not selected, don't ask for it
      if (!requiredColumns.contains(metadata)) {
        paramWithScan += (ConfigurationOptions.ES_READ_METADATA -> false.toString())
      }
    }

    if (filters != null && filters.size > 0) {
      val filterString = createDSLFromFilters(filters)
      paramWithScan += (InternalConfigurationOptions.INTERNAL_ES_QUERY_FILTERS -> IOUtils.serializeToBase64(filterString))
    }

    new ScalaEsRowRDD(sqlContext.sparkContext, paramWithScan, lazySchema)
  }

  private def createDSLFromFilters(filters: Array[Filter]) = {
    filters.map(filter => translateFilter(filter)).filter(query => query.trim().length() > 0)
  }

  // string interpolation FTW
  private def translateFilter(filter: Filter):String = {
    filter match {
      // use match (not term) as we have no control over the analyzer (not-analyzer vs analyzed)
      case EqualTo(attribute, value)            => s"""{"query":{"match":{"$attribute":${extract(value)}}}}"""
      case GreaterThan(attribute, value)        => s"""{"range":{"$attribute":{"gt" :${extract(value)}}}}"""
      case GreaterThanOrEqual(attribute, value) => s"""{"range":{"$attribute":{"gte":${extract(value)}}}}"""
      case LessThan(attribute, value)           => s"""{"range":{"$attribute":{"lt" :${extract(value)}}}}"""
      case LessThanOrEqual(attribute, value)    => s"""{"range":{"$attribute":{"lte":${extract(value)}}}}"""
      case In(attribute, values)                => s"""{"query":{"match":{"$attribute":${extract(values)}}}}"""
      case IsNull(attribute)                    => s"""{"missing":{"field":"$attribute"}}"""
      case IsNotNull(attribute)                 => s"""{"exists":{"field":"$attribute"}}"""
      case And(left, right)                     => s"""{"and":{"filters":[${translateFilter(left)}, ${translateFilter(right)}]}}"""
      case Or(left, right)                      => s"""{"or":{"filters":[${translateFilter(left)}, ${translateFilter(right)}]}}"""
      case Not(filterToNeg)                     => s"""{"not":{"filter":${translateFilter(filterToNeg)}}}"""

      // the filter below are available only from Spark 1.3.1 (not 1.3.0)

      //
      // String Filter notes:
      //
      // the DSL will be quite slow (linear to the number of terms in the index) but there's no easy way around them
      // we could use regexp filter however it's a bit overkill and there are plenty of chars to escape
      // s"""{"regexp":{"$attribute":"$value.*"}}"""
      // as an alternative we could use a query string but still, the analyzed / non-analyzed is there as the DSL is slightly more complicated
      // s"""{"query":{"query_string":{"default_field":"$attribute","query":"$value*"}}}"""
      // instead wildcard query is used, with the value lowercased (to match analyzed fields)

      case f:Product if isClass(f, "org.apache.spark.sql.sources.StringStartsWith")
                                                => s"""{"query":{"wildcard":{"${f.productElement(0)}":"${f.productElement(1).toString().toLowerCase()}*"}}}"""

      case f:Product if isClass(f, "org.apache.spark.sql.sources.StringEndsWith")
                                                => s"""{"query":{"wildcard":{"${f.productElement(0)}":"*${f.productElement(1).toString().toLowerCase()}"}}}"""

      case f:Product if isClass(f, "org.apache.spark.sql.sources.StringContains")
                                                => s"""{"query":{"wildcard":{"${f.productElement(0)}":"*${f.productElement(1).toString().toLowerCase()}*"}}}"""

       case _                                   => ""
    }
  }

  private def isClass(filter: Filter, className: String) = {
    className.equals(filter.getClass().getName())
  }

  private def extract(value: Any):String = {
    extract(value, true)
  }

  private def extract(value: Any, inJsonFormat: Boolean):String = {
    // common-case implies primitives and String so try these before using the full-blown ValueWriter
    value match {
      case u: Unit       =>  "null"
      case b: Boolean    =>  b.toString
      case c: Char       =>  if (inJsonFormat) StringUtils.toJsonString(c) else c.toString()
      case by: Byte      =>  by.toString
      case s: Short      =>  s.toString
      case i: Int        =>  i.toString
      case l: Long       =>  l.toString
      case f: Float      =>  f.toString
      case d: Double     =>  d.toString
      case s: String     =>  if (inJsonFormat) StringUtils.toJsonString(s) else s.toString()
      case ar: Array[Any]  => (for (i <- ar) yield extract(i, false)).mkString("\"", " ", "\"")
      case a: AnyRef     =>  {
        val storage = new FastByteArrayOutputStream()
        val generator = new JacksonJsonGenerator(storage)
        valueWriter.write(a, generator)
        generator.flush()
        generator.close()
        storage.toString()
      }
    }
  }

  def insert(data: DataFrame, overwrite: Boolean) {
    throw new UnsupportedOperationException()
  }
}