package org.elasticsearch.spark.sql

import java.util.Properties
import java.util.{ LinkedHashSet => JHashSet }
import java.util.{ List => JList }
import java.util.{ Map => JMap }
import java.util.Properties
import scala.Array.fallbackCanBuildFrom
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.JavaConverters.propertiesAsScalaMapConverter
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.MapType
import org.apache.spark.sql.catalyst.types.ArrayType
import org.apache.spark.sql.catalyst.types.BinaryType
import org.apache.spark.sql.catalyst.types.BooleanType
import org.apache.spark.sql.catalyst.types.ByteType
import org.apache.spark.sql.catalyst.types.DoubleType
import org.apache.spark.sql.catalyst.types.FloatType
import org.apache.spark.sql.catalyst.types.IntegerType
import org.apache.spark.sql.catalyst.types.LongType
import org.apache.spark.sql.catalyst.types.NullType
import org.apache.spark.sql.catalyst.types.ShortType
import org.apache.spark.sql.catalyst.types.StringType
import org.apache.spark.sql.catalyst.types.StructField
import org.apache.spark.sql.catalyst.types.StructType
import org.apache.spark.sql.catalyst.types.TimestampType
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException
import org.elasticsearch.hadoop.cfg.InternalConfigurationOptions
import org.elasticsearch.hadoop.cfg.Settings
import org.elasticsearch.hadoop.rest.RestRepository
import org.elasticsearch.hadoop.serialization.FieldType.BINARY
import org.elasticsearch.hadoop.serialization.FieldType.BOOLEAN
import org.elasticsearch.hadoop.serialization.FieldType.BYTE
import org.elasticsearch.hadoop.serialization.FieldType.DATE
import org.elasticsearch.hadoop.serialization.FieldType.DOUBLE
import org.elasticsearch.hadoop.serialization.FieldType.FLOAT
import org.elasticsearch.hadoop.serialization.FieldType.INTEGER
import org.elasticsearch.hadoop.serialization.FieldType.LONG
import org.elasticsearch.hadoop.serialization.FieldType.NESTED
import org.elasticsearch.hadoop.serialization.FieldType.NULL
import org.elasticsearch.hadoop.serialization.FieldType.OBJECT
import org.elasticsearch.hadoop.serialization.FieldType.SHORT
import org.elasticsearch.hadoop.serialization.FieldType.STRING
import org.elasticsearch.hadoop.serialization.dto.mapping.Field
import org.elasticsearch.hadoop.serialization.dto.mapping.MappingUtils
import org.elasticsearch.hadoop.serialization.field.FieldFilter
import org.elasticsearch.hadoop.serialization.field.FieldFilter.NumberedInclude
import org.elasticsearch.hadoop.util.Assert
import org.elasticsearch.hadoop.util.IOUtils
import org.elasticsearch.hadoop.util.StringUtils
import org.elasticsearch.spark.sql.Utils.ROOT_LEVEL_NAME
import org.elasticsearch.spark.sql.Utils.ROW_INFO_ORDER_PROPERTY
import org.elasticsearch.spark.sql.Utils.ROW_INFO_ARRAY_PROPERTY
import org.elasticsearch.hadoop.util.SettingsUtils
import org.apache.spark.sql.catalyst.types.DataType

private[sql] object SchemaUtils {
  case class Schema(field: Field, struct: StructType)

  val readInclude = "es.read.field.include"
  val readExclude = "es.read.field.exclude"

  def discoverMapping(cfg: Settings): Schema = {
    val field = discoverMappingAsField(cfg)
    val struct = convertToStruct(field, cfg)
    Schema(field, struct)
  }

  def discoverMappingAsField(cfg: Settings): Field = {
    val repo = new RestRepository(cfg)
    try {
      if (repo.indexExists(true)) {
        var field = repo.getMapping

        if (field == null) {
          throw new EsHadoopIllegalArgumentException(s"Cannot find mapping for ${cfg.getResourceRead} - one is required before using Spark SQL")
        }

        field = MappingUtils.filterMapping(field, cfg);
        val geoInfo = repo.sampleGeoFields(field)
        
        if (!geoInfo.isEmpty()) {
          throw new EsHadoopIllegalArgumentException(s"Geo types are supported only in ES-Hadoop for SparkSQL 1.3 (or higher) DataFrames")
        }

        // apply mapping filtering only when present to minimize configuration settings (big when dealing with large mappings)
        if (StringUtils.hasText(cfg.getReadFieldInclude) || StringUtils.hasText(cfg.getReadFieldExclude)) {
          // NB: metadata field is synthetic so it doesn't have to be filtered
          // its presence is controller through the dedicated config setting
          cfg.setProperty(InternalConfigurationOptions.INTERNAL_ES_TARGET_FIELDS, StringUtils.concatenate(Field.toLookupMap(field).keySet()));
        }
        return field
      }
      else {
        throw new EsHadoopIllegalArgumentException(s"Cannot find mapping for ${cfg.getResourceRead} - one is required before using Spark SQL")
      }
    } finally {
        repo.close()
    }
  }

  private def convertToStruct(rootField: Field, cfg: Settings): StructType = {
    val arrayIncludes = SettingsUtils.getFieldArrayFilterInclude(cfg)
    val arrayExcludes = StringUtils.tokenize(cfg.getReadFieldAsArrayExclude)

    var fields = for (fl <- rootField.properties()) yield convertField(fl, null, arrayIncludes, arrayExcludes)
    if (cfg.getReadMetadata) {
      val metadataMap = new StructField(cfg.getReadMetadataField, new MapType(StringType, StringType, true), true)
      fields :+= metadataMap
    }
    new StructType(fields)
  }

  private def convertToStruct(field: Field, parentName: String, arrayIncludes: JList[NumberedInclude], arrayExcludes: JList[String]): StructType = {
    new StructType(for (fl <- field.properties()) yield convertField(fl, parentName, arrayIncludes, arrayExcludes))
  }

  private def convertField(field: Field, parentName: String, arrayIncludes: JList[NumberedInclude], arrayExcludes: JList[String]): StructField = {
    val absoluteName = if (parentName != null) parentName + "." + field.name() else field.name()
    val matched = FieldFilter.filter(absoluteName, arrayIncludes, arrayExcludes, false)
    val createArray = !arrayIncludes.isEmpty() && matched.matched

    var dataType = Utils.extractType(field) match {
      case NULL    => NullType
      case BINARY  => BinaryType
      case BOOLEAN => BooleanType
      case BYTE    => ByteType
      case SHORT   => ShortType
      case INTEGER => IntegerType
      case LONG    => LongType
      case FLOAT   => FloatType
      case DOUBLE  => DoubleType
      case STRING  => StringType
      case DATE    => TimestampType
      case OBJECT  => convertToStruct(field, absoluteName, arrayIncludes, arrayExcludes)
      case NESTED  => new ArrayType(convertToStruct(field, absoluteName, arrayIncludes, arrayExcludes), true)
      // fall back to String
      case _       => StringType //throw new EsHadoopIllegalStateException("Unknown field type " + field);
    }

    if (createArray) {
      var currentDepth = 0;
      for (currentDepth <- 0 until matched.depth) {
        dataType = new ArrayType(dataType, true)
      }
    }
    
    return new StructField(field.name(), dataType, true)
  }

  def setRowInfo(settings: Settings, struct: StructType) = {
    val rowInfo = detectRowInfo(settings, struct)
    // save the field in the settings to pass it to the value reader
    settings.setProperty(ROW_INFO_ORDER_PROPERTY, IOUtils.propsToString(rowInfo._1))
    // also include any array info
    settings.setProperty(ROW_INFO_ARRAY_PROPERTY, IOUtils.propsToString(rowInfo._2))
  }

  def getRowInfo(settings: Settings) = {
    val rowOrderString = settings.getProperty(ROW_INFO_ORDER_PROPERTY)
    Assert.hasText(rowOrderString, "no schema/row order detected...")

    val rowOrderProps = IOUtils.propsFromString(rowOrderString)

    val rowArrayString = settings.getProperty(ROW_INFO_ARRAY_PROPERTY)
    val rowArrayProps = if (StringUtils.hasText(rowArrayString)) IOUtils.propsFromString(rowArrayString) else new Properties()

    val order = new scala.collection.mutable.LinkedHashMap[String, Seq[String]]
    for (prop <- rowOrderProps.asScala) {
      val value = StringUtils.tokenize(prop._2).asScala
      if (!value.isEmpty) {
        order.put(prop._1, new ArrayBuffer() ++= value)
      }
    }

    val needToBeArray = new JHashSet[String]()

    for (prop <- rowArrayProps.asScala) {
      needToBeArray.add(prop._1)
    }

    (order,needToBeArray)

  }

  def detectRowInfo(settings: Settings, struct: StructType): (Properties, Properties) = {
    // tuple - 1 = columns (in simple names) for each row, 2 - what fields (in absolute names) are arrays
    val rowInfo = (new Properties, new Properties)

    doDetectInfo(rowInfo, ROOT_LEVEL_NAME, struct)
    val csv = settings.getScrollFields()
    // if a projection is applied (filtering or projection) use that instead
    if (StringUtils.hasText(csv)) {
      if (settings.getReadMetadata) {
        rowInfo._1.setProperty(ROOT_LEVEL_NAME, csv + StringUtils.DEFAULT_DELIMITER + settings.getReadMetadataField)
      }
      else {
        rowInfo._1.setProperty(ROOT_LEVEL_NAME, csv)
      }
    }
    rowInfo
  }

  private def doDetectInfo(info: (Properties, Properties), level: String, dataType: DataType) {
    dataType match {
      case s: StructType => {
        val fields = new java.util.ArrayList[String]
        for (field <- s.fields) {
          fields.add(field.name)
          doDetectInfo(info, if (level != ROOT_LEVEL_NAME) level + "." + field.name else field.name, field.dataType)
        }
        info._1.setProperty(level, StringUtils.concatenate(fields, StringUtils.DEFAULT_DELIMITER))
      }
      case a: ArrayType => {
        val prop = info._2.getProperty(level)
        var depth = 0
        if (StringUtils.hasText(prop)) {
          depth = Integer.parseInt(prop)
        }
        depth += 1
        info._2.setProperty(level, String.valueOf(depth))
        doDetectInfo(info, level, a.elementType)
      }
      // ignore primitives
      case _ => // ignore
    }
  }
}