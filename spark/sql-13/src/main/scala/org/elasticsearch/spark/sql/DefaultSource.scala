package org.elasticsearch.spark.sql

import java.util.Locale
import scala.None
import scala.Null
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.mutable.ArrayOps
import scala.collection.mutable.LinkedHashMap
import scala.collection.mutable.LinkedHashSet
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SaveMode.Append
import org.apache.spark.sql.SaveMode.ErrorIfExists
import org.apache.spark.sql.SaveMode.Ignore
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.sources.And
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.CreatableRelationProvider
import org.apache.spark.sql.sources.EqualTo
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.GreaterThan
import org.apache.spark.sql.sources.GreaterThanOrEqual
import org.apache.spark.sql.sources.In
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.sources.IsNotNull
import org.apache.spark.sql.sources.IsNull
import org.apache.spark.sql.sources.LessThan
import org.apache.spark.sql.sources.LessThanOrEqual
import org.apache.spark.sql.sources.Not
import org.apache.spark.sql.sources.Or
import org.apache.spark.sql.sources.PrunedFilteredScan
import org.apache.spark.sql.sources.RelationProvider
import org.apache.spark.sql.sources.SchemaRelationProvider
import org.apache.spark.sql.types.StructType
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException
import org.elasticsearch.hadoop.EsHadoopIllegalStateException
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.hadoop.cfg.InternalConfigurationOptions
import org.elasticsearch.hadoop.rest.InitializationUtils
import org.elasticsearch.hadoop.rest.RestRepository
import org.elasticsearch.hadoop.serialization.builder.JdkValueWriter
import org.elasticsearch.hadoop.serialization.json.JacksonJsonGenerator
import org.elasticsearch.hadoop.util.FastByteArrayOutputStream
import org.elasticsearch.hadoop.util.IOUtils
import org.elasticsearch.hadoop.util.StringUtils
import org.elasticsearch.spark.cfg.SparkSettingsManager
import org.elasticsearch.spark.serialization.ScalaValueWriter
import org.apache.commons.logging.LogFactory

private[sql] class DefaultSource extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider  {

  override def createRelation(@transient sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    ElasticsearchRelation(params(parameters), sqlContext)
  }

  override def createRelation(@transient sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    ElasticsearchRelation(params(parameters), sqlContext, Some(schema))
  }

  override def createRelation(@transient sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val relation = ElasticsearchRelation(params(parameters), sqlContext, Some(data.schema))
    mode match {
      case Append         => relation.insert(data, false)
      case Overwrite      => relation.insert(data, true)
      case ErrorIfExists  => {
        if (relation.isEmpty()) relation.insert(data, false)
        else throw new EsHadoopIllegalStateException(s"Index ${relation.cfg.getResourceWrite} already exists")
      }
      case Ignore         => if (relation.isEmpty()) { relation.insert(data, false) }
    }
    relation
  }

  private def params(parameters: Map[String, String]) = {
    // . seems to be problematic when specifying the options
    val params = parameters.map { case (k, v) => (k.replace('_', '.'), v)}. map { case (k, v) =>
      if (k.startsWith("es.")) (k, v)
      else if (k == "path") ("es.resource", v)
      else if (k == "pushdown") (Utils.DATA_SOURCE_PUSH_DOWN, v)
      else if (k == "strict") (Utils.DATA_SOURCE_PUSH_DOWN_STRICT, v)
      else ("es." + k, v)
    }
    params.getOrElse(ConfigurationOptions.ES_RESOURCE, throw new EsHadoopIllegalArgumentException("resource must be specified for Elasticsearch resources."))
    params
  }
}

private[sql] case class ElasticsearchRelation(parameters: Map[String, String], @transient val sqlContext: SQLContext, userSchema: Option[StructType] = None)
  extends BaseRelation with PrunedFilteredScan with InsertableRelation
  {

  @transient lazy val cfg = { new SparkSettingsManager().load(sqlContext.sparkContext.getConf).merge(parameters.asJava) }

  @transient lazy val lazySchema = { SchemaUtils.discoverMapping(cfg) }

  @transient lazy val valueWriter = { new ScalaValueWriter }

  override def schema = userSchema.getOrElse(lazySchema.struct)

  // TableScan
  def buildScan(): RDD[Row] = buildScan(Array.empty)

  // PrunedScan
  def buildScan(requiredColumns: Array[String]): RDD[Row] = buildScan(requiredColumns, Array.empty)

  // PrunedFilteredScan
  def buildScan(requiredColumns: Array[String], filters: Array[Filter]) = {
    val paramWithScan = LinkedHashMap[String, String]() ++ parameters
    paramWithScan += (InternalConfigurationOptions.INTERNAL_ES_TARGET_FIELDS ->
                      StringUtils.concatenate(requiredColumns.asInstanceOf[Array[Object]], StringUtils.DEFAULT_DELIMITER))

    // scroll fields only apply to source fields; handle metadata separately
    if (cfg.getReadMetadata) {
      val metadata = cfg.getReadMetadataField
      // if metadata is not selected, don't ask for it
      if (!requiredColumns.contains(metadata)) {
        paramWithScan += (ConfigurationOptions.ES_READ_METADATA -> false.toString())
      }
    }

    if (filters != null && filters.size > 0 && Utils.isPushDown(cfg)) {
      val log = logger
      if (log.isDebugEnabled()) {
        log.debug(s"Pushing down filters ${filters.mkString("[", ",", "]")}")
      }
      val filterString = createDSLFromFilters(filters, Utils.isPushDownStrict(cfg))

      if (log.isTraceEnabled()) {
        log.trace("Transformed filters into DSL $filterString")
      }
      paramWithScan += (InternalConfigurationOptions.INTERNAL_ES_QUERY_FILTERS -> IOUtils.serializeToBase64(filterString))
    }

    new ScalaEsRowRDD(sqlContext.sparkContext, paramWithScan, lazySchema)
  }

  private def createDSLFromFilters(filters: Array[Filter], strictPushDown: Boolean) = {
    filters.map(filter => translateFilter(filter, strictPushDown)).filter(query => query.trim().length() > 0)
  }

  // string interpolation FTW
  private def translateFilter(filter: Filter, strictPushDown: Boolean):String = {
    // the pushdown can be strict - i.e. use only filters and thus match the value exactly (works with non-analyzed)
    // or non-strict meaning queries will be used instead that is the filters will be analyzed as well
    filter match {

      case EqualTo(attribute, value)            => {
        // if we get a null, translate it into a missing query (we're extra careful - Spark should translate the equals into isMissing anyway)
        if (value == null || value == None || value == Unit) {
          return s"""{"missing":{"field":"$attribute"}}"""
        }

        if (strictPushDown) s"""{"term":{"$attribute":${extract(value)}}}"""
        else s"""{"query":{"match":{"$attribute":${extract(value)}}}}"""
      }
      case GreaterThan(attribute, value)        => s"""{"range":{"$attribute":{"gt" :${extract(value)}}}}"""
      case GreaterThanOrEqual(attribute, value) => s"""{"range":{"$attribute":{"gte":${extract(value)}}}}"""
      case LessThan(attribute, value)           => s"""{"range":{"$attribute":{"lt" :${extract(value)}}}}"""
      case LessThanOrEqual(attribute, value)    => s"""{"range":{"$attribute":{"lte":${extract(value)}}}}"""
      case In(attribute, values)                => {
        // when dealing with mixed types (strings and numbers) Spark converts the Strings to null (gets confused by the type field)
        // this leads to incorrect query DSL hence why nulls are filtered
        val filtered = values filter (_ != null)
        if (filtered.isEmpty) {
          return ""
        }
        if (strictPushDown) s"""{"terms":{"$attribute":${extractAsJsonArray(filtered)}}}"""
        else s"""{"or":{"filters":[${extractMatchArray(attribute, filtered)}]}}"""
      }
      case IsNull(attribute)                    => s"""{"missing":{"field":"$attribute"}}"""
      case IsNotNull(attribute)                 => s"""{"exists":{"field":"$attribute"}}"""
      case And(left, right)                     => s"""{"and":{"filters":[${translateFilter(left, strictPushDown)}, ${translateFilter(right, strictPushDown)}]}}"""
      case Or(left, right)                      => s"""{"or":{"filters":[${translateFilter(left, strictPushDown)}, ${translateFilter(right, strictPushDown)}]}}"""
      case Not(filterToNeg)                     => s"""{"not":{"filter":${translateFilter(filterToNeg, strictPushDown)}}}"""

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

      case f:Product if isClass(f, "org.apache.spark.sql.sources.StringStartsWith") => {
        var arg = f.productElement(1).toString()
        if (!strictPushDown) { arg = arg.toLowerCase(Locale.ROOT) }
        s"""{"query":{"wildcard":{"${f.productElement(0)}":"$arg*"}}}"""
      }

      case f:Product if isClass(f, "org.apache.spark.sql.sources.StringEndsWith")   => {
        var arg = f.productElement(1).toString()
        if (!strictPushDown) { arg = arg.toLowerCase(Locale.ROOT) }
        s"""{"query":{"wildcard":{"${f.productElement(0)}":"*$arg"}}}"""
      }

      case f:Product if isClass(f, "org.apache.spark.sql.sources.StringContains")   => {
        var arg = f.productElement(1).toString()
        if (!strictPushDown) { arg = arg.toLowerCase(Locale.ROOT) }
        s"""{"query":{"wildcard":{"${f.productElement(0)}":"*$arg*"}}}"""
      }

      // the filter below are available only from Spark 1.5.0

      //
      // [[EqualNullSafe]] Filter notes:

      case f:Product if isClass(f, "org.apache.spark.sql.sources.EqualNullSafe")   => {
        var arg = extract(f.productElement(1))
        if (strictPushDown) s"""{"term":{"${f.productElement(0)}":$arg}}"""
        else s"""{"query":{"match":{"${f.productElement(0)}":$arg}}}"""
      }

      case _                                                                        => ""
    }
  }

  private def isClass(obj: Any, className: String) = {
    className.equals(obj.getClass().getName())
  }

  private def extract(value: Any):String = {
    extract(value, true, false)
  }

  private def extractAsJsonArray(value: Any):String = {
    extract(value, true, true)
  }

  private def extractMatchArray(attribute: String, ar: Array[Any]):String = {
    // use a set to avoid duplicate values
    // especially since Spark conversion might turn each user param into null
    val numbers = LinkedHashSet.empty[AnyRef]
    val strings = LinkedHashSet.empty[AnyRef]

    // move numbers into a separate list for a terms query combined with a bool
    for (i <- ar) i.asInstanceOf[AnyRef] match {
      case null     => // ignore
      case n:Number => numbers += extract(i, false, false)
      case _        => strings += extract(i, false, false)
    }

    if (numbers.isEmpty) {
     if (strings.isEmpty) {
       return StringUtils.EMPTY
     }
     return s"""{"query":{"match":{"$attribute":${strings.mkString("\"", " ", "\"")}}}}"""
     //s"""{"query":{"$attribute":${strings.mkString("\"", " ", "\"")}}}"""
    }
    else {
      // translate the numbers into a terms query
      val str = s"""{"terms":{"$attribute":${numbers.mkString("[", ",", "]")}}}"""
      if (strings.isEmpty) return str
      // if needed, add the strings as a match query
      else return str + s""",{"query":{"match":{"$attribute":${strings.mkString("\"", " ", "\"")}}}}"""
    }
  }

  private def extract(value: Any, inJsonFormat: Boolean, asJsonArray: Boolean):String = {
    // common-case implies primitives and String so try these before using the full-blown ValueWriter
    value match {
      case null           => "null"
      case u: Unit        => "null"
      case b: Boolean     => b.toString
      case c: Char        => if (inJsonFormat) StringUtils.toJsonString(c) else c.toString()
      case by: Byte       => by.toString
      case s: Short       => s.toString
      case i: Int         => i.toString
      case l: Long        => l.toString
      case f: Float       => f.toString
      case d: Double      => d.toString
      case s: String      => if (inJsonFormat) StringUtils.toJsonString(s) else s
      case ar: Array[Any] =>
        if (asJsonArray) (for (i <- ar) yield extract(i, true, false)).distinct.mkString("[", ",", "]")
        else (for (i <- ar) yield extract(i, false, false)).distinct.mkString("\"", " ", "\"")
      // new in Spark 1.4
      case utf if (isClass(utf, "org.apache.spark.sql.types.UTF8String")
      // new in Spark 1.5
                   || isClass(utf, "org.apache.spark.unsafe.types.UTF8String"))
                          => if (inJsonFormat) StringUtils.toJsonString(utf.toString()) else utf.toString()
      case a: AnyRef      => {
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
    if (overwrite) {
      logger.info(s"Overwriting data for ${cfg.getResourceWrite}")

      // perform a scan-scroll delete
      val cfgCopy = cfg.copy()
      InitializationUtils.setValueWriterIfNotSet(cfgCopy, classOf[JdkValueWriter], null)
      cfgCopy.setProperty(ConfigurationOptions.ES_BATCH_FLUSH_MANUAL, "false")
      cfgCopy.setProperty(ConfigurationOptions.ES_BATCH_SIZE_ENTRIES, "1000")
      cfgCopy.setProperty(ConfigurationOptions.ES_BATCH_SIZE_BYTES, "1mb")
      val rr = new RestRepository(cfgCopy)
      rr.delete()
      rr.close()
    }
    EsSparkSQL.saveToEs(data, parameters)
  }

  def isEmpty(): Boolean = {
      val rr = new RestRepository(cfg)
      val empty = rr.isEmpty(true)
      rr.close()
      empty
  }

  private def logger = LogFactory.getLog("org.elasticsearch.spark.sql.DataSource")
}