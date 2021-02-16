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
package org.elasticsearch.spark.sql

import java.util.Calendar
import java.util.Date
import java.util.Locale
import java.util.UUID

import javax.xml.bind.DatatypeConverter
import org.apache.commons.logging.LogFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode.Append
import org.apache.spark.sql.SaveMode.ErrorIfExists
import org.apache.spark.sql.SaveMode.Ignore
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.execution.streaming.Sink
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
import org.apache.spark.sql.sources.StreamSinkProvider
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException
import org.elasticsearch.hadoop.EsHadoopIllegalStateException
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.hadoop.cfg.InternalConfigurationOptions
import org.elasticsearch.hadoop.cfg.InternalConfigurationOptions.INTERNAL_TRANSPORT_POOLING_KEY
import org.elasticsearch.hadoop.cfg.Settings
import org.elasticsearch.hadoop.mr.security.HadoopUserProvider
import org.elasticsearch.hadoop.rest.InitializationUtils
import org.elasticsearch.hadoop.rest.RestRepository
import org.elasticsearch.hadoop.serialization.builder.JdkValueWriter
import org.elasticsearch.hadoop.serialization.field.ConstantFieldExtractor
import org.elasticsearch.hadoop.serialization.json.JacksonJsonGenerator
import org.elasticsearch.hadoop.util.EsMajorVersion
import org.elasticsearch.hadoop.util.FastByteArrayOutputStream
import org.elasticsearch.hadoop.util.IOUtils
import org.elasticsearch.hadoop.util.SettingsUtils
import org.elasticsearch.hadoop.util.StringUtils
import org.elasticsearch.hadoop.util.Version
import org.elasticsearch.spark.cfg.SparkSettingsManager
import org.elasticsearch.spark.serialization.ScalaValueWriter
import org.elasticsearch.spark.sql.streaming.EsSparkSqlStreamingSink
import org.elasticsearch.spark.sql.streaming.SparkSqlStreamingConfigs
import org.elasticsearch.spark.sql.streaming.StructuredStreamingVersionLock

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.mutable.LinkedHashMap
import scala.collection.mutable.{Map => MutableMap}
import scala.collection.mutable.LinkedHashSet

private[sql] class DefaultSource extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider with StreamSinkProvider {

  Version.logVersion()
  
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
        else throw new EsHadoopIllegalStateException(s"SaveMode is set to ErrorIfExists and " + 
                s"index ${relation.cfg.getResourceWrite} exists and contains data. Consider changing the SaveMode")
      }
      case Ignore         => if (relation.isEmpty()) { relation.insert(data, false) }
    }
    relation
  }

  override def createSink(sqlContext: SQLContext, parameters: Map[String, String], partitionColumns: Seq[String], outputMode: OutputMode): Sink = {
    val sparkSession = sqlContext.sparkSession

    // Verify compatiblity versions for alpha:
    StructuredStreamingVersionLock.checkCompatibility(sparkSession)

    // For now we only support Append style output mode
    if (outputMode != OutputMode.Append()) {
      throw new EsHadoopIllegalArgumentException("Append is only supported OutputMode for Elasticsearch. " +
        s"Cannot continue with [$outputMode].")
    }

    // Should not support partitioning. We already allow people to split data into different
    // indices with the index pattern functionality. Potentially could add this later if a need
    // arises by appending patterns to the provided index, but that's probably feature overload.
    if (partitionColumns != Nil) {
      throw new EsHadoopIllegalArgumentException("Partition columns are not supported for Elasticsearch. " +
        "If you need to partition your data by column values on Elasticsearch, please use an index pattern instead.")
    }

    // Add in the transport pooling key for this job
    val mapConfig = MutableMap(parameters.toSeq: _*) += (INTERNAL_TRANSPORT_POOLING_KEY -> UUID.randomUUID().toString)
    val jobSettings = new SparkSettingsManager()
        .load(sqlContext.sparkContext.getConf)
        .merge(streamParams(mapConfig.toMap, sparkSession).asJava)

    InitializationUtils.discoverClusterInfo(jobSettings, LogFactory.getLog(classOf[DefaultSource]))
    InitializationUtils.checkIdForOperation(jobSettings)
    InitializationUtils.checkIndexExistence(jobSettings)

    new EsSparkSqlStreamingSink(sparkSession, jobSettings)
  }

  private[sql] def params(parameters: Map[String, String]) = {
    // '.' seems to be problematic when specifying the options
    val dottedParams = parameters.map { case (k, v) => (k.replace('_', '.'), v)}

    // You can set the resource by using the "path", "resource", or "es.resource"/"es_resource" config keys.
    // In rare circumstances (cough Hive+Spark integration cough) these settings can all be specified, and be set to different
    // values. Instead of relying on the map's entry order to set the value, we will proactively retrieve the settings
    // in the following order, falling back as needed:
    //
    // 1. "es.resource"
    // 2. "resource"
    // 3. "path"
    //
    // See https://github.com/elastic/elasticsearch-hadoop/issues/1082
    val preferredResource = dottedParams.get(ConfigurationOptions.ES_RESOURCE)
      .orElse(dottedParams.get("resource"))
      .orElse(dottedParams.get("path"))

    // Convert simple parameters into internal properties, and prefix other parameters
    val processedParams = dottedParams.map { case (k, v) =>
      if (k.startsWith("es.")) (k, v)
      else if (k == "path") (ConfigurationOptions.ES_RESOURCE, v) // This may not be the final value for this setting.
      else if (k == "pushdown") (Utils.DATA_SOURCE_PUSH_DOWN, v)
      else if (k == "strict") (Utils.DATA_SOURCE_PUSH_DOWN_STRICT, v)
      else if (k == "double.filtering") (Utils.DATA_SOURCE_KEEP_HANDLED_FILTERS, v)
      else ("es." + k, v)
    }

    // Set the preferred resource if it was specified originally
    val finalParams = preferredResource match {
      case Some(resource) => processedParams + (ConfigurationOptions.ES_RESOURCE -> resource)
      case None => processedParams
    }

    // validate path is available
    finalParams.getOrElse(ConfigurationOptions.ES_RESOURCE_READ,
      finalParams.getOrElse(ConfigurationOptions.ES_RESOURCE, throw new EsHadoopIllegalArgumentException("resource must be specified for Elasticsearch resources.")))

    finalParams
  }

  private def streamParams(parameters: Map[String, String], sparkSession: SparkSession) = {
    // '.' seems to be problematic when specifying the options
    var params = parameters.map { case (k, v) => (k.replace('_', '.'), v)}. map { case (k, v) =>
      if (k.startsWith("es.")) (k, v)
      else if (k == "path") (ConfigurationOptions.ES_RESOURCE, v)
      else if (k == "queryname") (SparkSqlStreamingConfigs.ES_INTERNAL_QUERY_NAME, v)
      else if (k == "checkpointlocation") (SparkSqlStreamingConfigs.ES_INTERNAL_USER_CHECKPOINT_LOCATION, v)
      else ("es." + k, v)
    }

    params = params + (SparkSqlStreamingConfigs.ES_INTERNAL_APP_NAME -> sparkSession.sparkContext.appName)
    params = params + (SparkSqlStreamingConfigs.ES_INTERNAL_APP_ID -> sparkSession.sparkContext.applicationId)

    sparkSession.conf.getOption(SQLConf.CHECKPOINT_LOCATION.key).foreach { loc =>
      params = params + (SparkSqlStreamingConfigs.ES_INTERNAL_SESSION_CHECKPOINT_LOCATION -> loc)
    }

    // validate path
    params.getOrElse(ConfigurationOptions.ES_RESOURCE_WRITE,
      params.getOrElse(ConfigurationOptions.ES_RESOURCE,
        throw new EsHadoopIllegalArgumentException("resource must be specified for Elasticsearch resources.")))

    params
  }
}

private[sql] case class ElasticsearchRelation(parameters: Map[String, String], @transient val sqlContext: SQLContext, userSchema: Option[StructType] = None)
  extends BaseRelation with PrunedFilteredScan with InsertableRelation
  {

  @transient lazy val cfg = {
    val conf = new SparkSettingsManager().load(sqlContext.sparkContext.getConf).merge(parameters.asJava)
    InitializationUtils.discoverClusterInfo(conf, LogFactory.getLog(classOf[ElasticsearchRelation]))
    conf
  }

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

    var filteredColumns = requiredColumns
    
    // scroll fields only apply to source fields; handle metadata separately
    if (cfg.getReadMetadata) {
      val metadata = cfg.getReadMetadataField
      // if metadata is not selected, don't ask for it
      if (!requiredColumns.contains(metadata)) {
        paramWithScan += (ConfigurationOptions.ES_READ_METADATA -> false.toString())
      }
      else {
        filteredColumns = requiredColumns.filter( _ != metadata)
      }
    }

    // Set fields to scroll over (_metadata is excluded, because it isn't a part of _source)
    val sourceCSV = StringUtils.concatenate(filteredColumns.asInstanceOf[Array[Object]], StringUtils.DEFAULT_DELIMITER)
    paramWithScan += (InternalConfigurationOptions.INTERNAL_ES_TARGET_FIELDS -> sourceCSV)

    // Keep the order of fields requested by user (we don't exclude _metadata here)
    val requiredCSV = StringUtils.concatenate(requiredColumns.asInstanceOf[Array[Object]], StringUtils.DEFAULT_DELIMITER)
    paramWithScan += (Utils.DATA_SOURCE_REQUIRED_COLUMNS -> requiredCSV)

    // If the only field requested by user is metadata, we don't want to fetch the whole document source
    if (requiredCSV == cfg.getReadMetadataField()) {
      paramWithScan += (InternalConfigurationOptions.INTERNAL_ES_EXCLUDE_SOURCE -> "true")
    }
    
    if (filters != null && filters.size > 0) {
      if (Utils.isPushDown(cfg)) {
        if (Utils.LOGGER.isDebugEnabled()) {
          Utils.LOGGER.debug(s"Pushing down filters ${filters.mkString("[", ",", "]")}")
        }
        val filterString = createDSLFromFilters(filters, Utils.isPushDownStrict(cfg), isEs50(cfg))

        if (Utils.LOGGER.isTraceEnabled()) {
          Utils.LOGGER.trace(s"Transformed filters into DSL ${filterString.mkString("[", ",", "]")}")
        }
        paramWithScan += (InternalConfigurationOptions.INTERNAL_ES_QUERY_FILTERS -> IOUtils.serializeToBase64(filterString))
      }
      else {
        if (Utils.LOGGER.isTraceEnabled()) {
          Utils.LOGGER.trace("Push-down is disabled; ignoring Spark filters...")
        }
      }
    }

    new ScalaEsRowRDD(sqlContext.sparkContext, paramWithScan, lazySchema)
  }

  // introduced in Spark 1.6
  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    if (Utils.isKeepHandledFilters(cfg) || filters == null || filters.size == 0) {
      filters
    } else {
      // walk the filters (things like And / Or) and see whether we recognize all of them
      // if we do, skip the filter, otherwise let it in there even though we might push some of it
      def unhandled(filter: Filter): Boolean = {
        filter match {
          case EqualTo(_, _)                                                            => false
          case GreaterThan(_, _)                                                        => false
          case GreaterThanOrEqual(_, _)                                                 => false
          case LessThan(_, _)                                                           => false
          case LessThanOrEqual(_, _)                                                    => false
          // In is problematic - see translate, don't filter it
          case In(_, _)                                                                 => true
          case IsNull(_)                                                                => false
          case IsNotNull(_)                                                             => false
          case And(left, right)                                                         => unhandled(left) || unhandled(right)
          case Or(left, right)                                                          => unhandled(left) || unhandled(right)
          case Not(pred)                                                                => unhandled(pred)
          // Spark 1.3.1+
          case f:Product if isClass(f, "org.apache.spark.sql.sources.StringStartsWith") => false
          case f:Product if isClass(f, "org.apache.spark.sql.sources.StringEndsWith")   => false
          case f:Product if isClass(f, "org.apache.spark.sql.sources.StringContains")   => false
          // Spark 1.5+
          case f:Product if isClass(f, "org.apache.spark.sql.sources.EqualNullSafe")    => false

          // unknown
          case _                                                                        => true
        }
      }

      val filtered = filters.filter(unhandled)
      if (Utils.LOGGER.isTraceEnabled()) {
        Utils.LOGGER.trace(s"Unhandled filters from ${filters.mkString("[", ",", "]")} to ${filtered.mkString("[", ",", "]")}")
      }
      filtered
    }
  }

  private def createDSLFromFilters(filters: Array[Filter], strictPushDown: Boolean, isES50: Boolean) = {
    filters.map(filter => translateFilter(filter, strictPushDown, isES50)).filter(query => StringUtils.hasText(query))
  }

  // string interpolation FTW
  private def translateFilter(filter: Filter, strictPushDown: Boolean, isES50: Boolean):String = {
    // the pushdown can be strict - i.e. use only filters and thus match the value exactly (works with non-analyzed)
    // or non-strict meaning queries will be used instead that is the filters will be analyzed as well
    filter match {

      case EqualTo(attribute, value)            => {
        // if we get a null, translate it into a missing query (we're extra careful - Spark should translate the equals into isMissing anyway)
        if (value == null || value == None || value == Unit) {
          if (isES50) {
            s"""{"bool":{"must_not":{"exists":{"field":"$attribute"}}}}"""
          }
          else {
            s"""{"missing":{"field":"$attribute"}}"""  
          }
        }

        if (strictPushDown) s"""{"term":{"$attribute":${extract(value)}}}"""
        else {
          if (isES50) {
            s"""{"match":{"$attribute":${extract(value)}}}"""
          }
          else {
            s"""{"query":{"match":{"$attribute":${extract(value)}}}}"""
          }
        }
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

        // further more, match query only makes sense with String types so for other types apply a terms query (aka strictPushDown)
        val attrType = lazySchema.struct(attribute).dataType
        val isStrictType = attrType match {
          case DateType |
               TimestampType => true
          case _             => false
        }

        if (!strictPushDown && isStrictType) {
          if (Utils.LOGGER.isDebugEnabled()) {
            Utils.LOGGER.debug(s"Attribute $attribute type $attrType not suitable for match query; using terms (strict) instead")
          }
        }

        if (strictPushDown || isStrictType) s"""{"terms":{"$attribute":${extractAsJsonArray(filtered)}}}"""
        else {
          if (isES50) {
            s"""{"bool":{"should":[${extractMatchArray(attribute, filtered)}]}}"""
          }
          else {
            s"""{"or":{"filters":[${extractMatchArray(attribute, filtered)}]}}"""  
          }
        }
      }
      case IsNull(attribute)                    => {
        if (isES50) {
          s"""{"bool":{"must_not":{"exists":{"field":"$attribute"}}}}"""
        }
        else {
          s"""{"missing":{"field":"$attribute"}}"""  
        }
      }
      case IsNotNull(attribute)                 => s"""{"exists":{"field":"$attribute"}}"""
      case And(left, right)                     => {
        if (isES50) {
          s"""{"bool":{"filter":[${translateFilter(left, strictPushDown, isES50)}, ${translateFilter(right, strictPushDown, isES50)}]}}"""
        }
        else {
          s"""{"and":{"filters":[${translateFilter(left, strictPushDown, isES50)}, ${translateFilter(right, strictPushDown, isES50)}]}}"""  
        }
      }
      case Or(left, right)                      => {
        if (isES50) {
          s"""{"bool":{"should":[{"bool":{"filter":${translateFilter(left, strictPushDown, isES50)}}}, {"bool":{"filter":${translateFilter(right, strictPushDown, isES50)}}}]}}"""
        }
        else {
          s"""{"or":{"filters":[${translateFilter(left, strictPushDown, isES50)}, ${translateFilter(right, strictPushDown, isES50)}]}}"""
        }
      }
      case Not(filterToNeg)                     => {
        if (isES50) {
          s"""{"bool":{"must_not":${translateFilter(filterToNeg, strictPushDown, isES50)}}}"""
        }
        else {        
          s"""{"not":{"filter":${translateFilter(filterToNeg, strictPushDown, isES50)}}}"""
        }
      }

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
        val arg = {
          val x = f.productElement(1).toString()
          if (!strictPushDown) x.toLowerCase(Locale.ROOT) else x
        }
        if (isES50) {
          s"""{"wildcard":{"${f.productElement(0)}":"$arg*"}}"""
        }
        else {
          s"""{"query":{"wildcard":{"${f.productElement(0)}":"$arg*"}}}"""  
        }
      }

      case f:Product if isClass(f, "org.apache.spark.sql.sources.StringEndsWith")   => {
        val arg = {
          val x = f.productElement(1).toString()
          if (!strictPushDown) x.toLowerCase(Locale.ROOT) else x
        }
        if (isES50) {
          s"""{"wildcard":{"${f.productElement(0)}":"*$arg"}}"""
        }
        else {
          s"""{"query":{"wildcard":{"${f.productElement(0)}":"*$arg"}}}"""
        }
      }

      case f:Product if isClass(f, "org.apache.spark.sql.sources.StringContains")   => {
        val arg = {
          val x = f.productElement(1).toString()
          if (!strictPushDown) x.toLowerCase(Locale.ROOT) else x
        }
        if (isES50) {
          s"""{"wildcard":{"${f.productElement(0)}":"*$arg*"}}"""
        }
        else {
          s"""{"query":{"wildcard":{"${f.productElement(0)}":"*$arg*"}}}"""
        }
      }

      // the filters below are available only from Spark 1.5.0

      case f:Product if isClass(f, "org.apache.spark.sql.sources.EqualNullSafe")    => {
        val arg = extract(f.productElement(1))
        if (strictPushDown) s"""{"term":{"${f.productElement(0)}":$arg}}"""
        else {
          if (isES50) {
            s"""{"match":{"${f.productElement(0)}":$arg}}"""
          }
          else {
            s"""{"query":{"match":{"${f.productElement(0)}":$arg}}}"""
          }
        }
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
        StringUtils.EMPTY
     } else {
       if (isEs50(cfg)) {
         s"""{"match":{"$attribute":${strings.mkString("\"", " ", "\"")}}}"""
       }
       else {
         s"""{"query":{"match":{"$attribute":${strings.mkString("\"", " ", "\"")}}}}"""  
       }
     }
    } else {
      // translate the numbers into a terms query
      val str = s"""{"terms":{"$attribute":${numbers.mkString("[", ",", "]")}}}"""
      if (strings.isEmpty){
        str
      // if needed, add the strings as a match query
      } else str + {
        if (isEs50(cfg)) {
          s""",{"match":{"$attribute":${strings.mkString("\"", " ", "\"")}}}"""
        }
        else {
          s""",{"query":{"match":{"$attribute":${strings.mkString("\"", " ", "\"")}}}}"""
        }
      }
    }
  }

  private def extract(value: Any, inJsonFormat: Boolean, asJsonArray: Boolean):String = {
    // common-case implies primitives and String so try these before using the full-blown ValueWriter
    value match {
      case null           => "null"
      case u: Unit        => "null"
      case b: Boolean     => b.toString
      case by: Byte       => by.toString
      case s: Short       => s.toString
      case i: Int         => i.toString
      case l: Long        => l.toString
      case f: Float       => f.toString
      case d: Double      => d.toString
      case bd: BigDecimal  => bd.toString
      case _: Char        |
           _: String      |
           _: Array[Byte]  => if (inJsonFormat) StringUtils.toJsonString(value.toString) else value.toString()
      // handle Timestamp also
      case dt: Date        => {
        val cal = Calendar.getInstance()
        cal.setTime(dt)
        val str = DatatypeConverter.printDateTime(cal)
        if (inJsonFormat) StringUtils.toJsonString(str) else str
      }
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

  def insert(data: DataFrame, overwrite: Boolean): Unit = {
    if (overwrite) {
      Utils.LOGGER.info(s"Overwriting data for ${cfg.getResourceWrite}")

      // perform a scan-scroll delete
      val cfgCopy = cfg.copy()
      InitializationUtils.discoverClusterInfo(cfgCopy, Utils.LOGGER)
      InitializationUtils.setValueWriterIfNotSet(cfgCopy, classOf[JdkValueWriter], null)
      InitializationUtils.setFieldExtractorIfNotSet(cfgCopy, classOf[ConstantFieldExtractor], null) //throw away extractor
      InitializationUtils.setUserProviderIfNotSet(cfgCopy, classOf[HadoopUserProvider], null)
      cfgCopy.setProperty(ConfigurationOptions.ES_BATCH_FLUSH_MANUAL, "false")
      cfgCopy.setProperty(ConfigurationOptions.ES_BATCH_SIZE_ENTRIES, "1000")
      cfgCopy.setProperty(ConfigurationOptions.ES_BATCH_SIZE_BYTES, "1mb")
      val rr = new RestRepository(cfgCopy)
      if (rr.resourceExists(false)) {
        rr.delete()
      }
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

  private[this] def isEs50(cfg: Settings): Boolean = {
    // TODO: Problematic. It's possible that the version is not ever discovered and set before this is needed.
    val version = if (cfg.getProperty(InternalConfigurationOptions.INTERNAL_ES_VERSION) == null) {
      EsMajorVersion.LATEST
    } else {
      cfg.getInternalVersionOrThrow
    }
    version.onOrAfter(EsMajorVersion.V_5_X)
  }
}