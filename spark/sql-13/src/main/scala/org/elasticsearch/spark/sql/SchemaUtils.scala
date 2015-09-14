package org.elasticsearch.spark.sql

import java.util.{List => JList}
import java.util.Properties

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.JavaConverters.propertiesAsScalaMapConverter
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.NullType
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
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
import org.elasticsearch.hadoop.serialization.FieldType.NULL
import org.elasticsearch.hadoop.serialization.FieldType.OBJECT
import org.elasticsearch.hadoop.serialization.FieldType.SHORT
import org.elasticsearch.hadoop.serialization.FieldType.STRING
import org.elasticsearch.hadoop.serialization.dto.mapping.Field
import org.elasticsearch.hadoop.serialization.dto.mapping.MappingUtils
import org.elasticsearch.hadoop.serialization.field.FieldFilter
import org.elasticsearch.hadoop.util.Assert
import org.elasticsearch.hadoop.util.IOUtils
import org.elasticsearch.hadoop.util.StringUtils
import org.elasticsearch.spark.sql.Utils.ROOT_LEVEL_NAME
import org.elasticsearch.spark.sql.Utils.ROW_INFO_ARRAY_PROPERTY
import org.elasticsearch.spark.sql.Utils.ROW_INFO_ORDER_PROPERTY

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

        var field = repo.getMapping.skipHeaders()
        val readIncludeCfg = cfg.getProperty(readInclude)
        val readExcludeCfg = cfg.getProperty(readExclude)

        // apply mapping filtering only when present to minimize configuration settings (big when dealing with large mappings)
        if (StringUtils.hasText(readIncludeCfg) || StringUtils.hasText(readExcludeCfg)) {
          // apply any possible include/exclude that can define restrict the DataFrame to just a number of fields
          val includes = StringUtils.tokenize(readIncludeCfg)
          val excludes = StringUtils.tokenize(readExcludeCfg)
          field = MappingUtils.filter(field, includes, excludes)
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
    val arrayIncludes = StringUtils.tokenize(cfg.getFieldReadAsArrayInclude)
    val arrayExcludes = StringUtils.tokenize(cfg.getFieldReadAsArrayExclude)

    var fields = for (fl <- rootField.properties()) yield convertField(fl, arrayIncludes, arrayExcludes)
    if (cfg.getReadMetadata) {
      // enrich structure
      val metadataMap = DataTypes.createStructField(cfg.getReadMetadataField, DataTypes.createMapType(StringType, StringType, true), true)
      fields :+= metadataMap
    }

    DataTypes.createStructType(fields)
  }

  private def convertToStruct(field: Field, arrayIncludes: JList[String], arrayExcludes: JList[String]): StructType = {
    DataTypes.createStructType(for (fl <- field.properties()) yield convertField(fl, arrayIncludes, arrayExcludes))
  }

  private def convertField(field: Field, arrayIncludes: JList[String], arrayExcludes: JList[String]): StructField = {
    val createArray = if (!arrayIncludes.isEmpty()) FieldFilter.filter(field.name(), arrayIncludes, arrayExcludes) else false

    val dataType = Utils.extractType(field) match {
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
      case OBJECT  => convertToStruct(field, arrayIncludes, arrayExcludes)
      // fall back to String
      case _       => StringType //throw new EsHadoopIllegalStateException("Unknown field type " + field);
    }

    DataTypes.createStructField(field.name(), if (createArray) DataTypes.createArrayType(dataType) else dataType, true)
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
      order.put(prop._1, new ArrayBuffer() ++= (StringUtils.tokenize(prop._2).asScala))
    }

    val needToBeArray = new scala.collection.mutable.LinkedHashMap[String, Seq[String]]
    for (prop <- rowArrayProps.asScala) {
      needToBeArray.put(prop._1, new ArrayBuffer() ++= (StringUtils.tokenize(prop._2).asScala))
    }

    (order,needToBeArray)
  }

  private def detectRowInfo(settings: Settings, struct: StructType): (Properties, Properties) = {
    // tuple - 1 = actual order, 2 - what fields are arrays
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

  private def doDetectInfo(info: (Properties, Properties), level: String, struct: StructType) {
    val fields = new java.util.ArrayList[String]
    val arrays = new java.util.ArrayList[String]

    for (field <- struct) {
      fields.add(field.name)
      field.dataType match {
        case s: StructType => doDetectInfo(info, field.name, s)
        case a: ArrayType  => {
          arrays.add(field.name)
          a.elementType match {
            case s: StructType => doDetectInfo(info, field.name, s)
            case _             => // ignore
          }
        }
        case _             => // ignore
      }
    }

    info._1.setProperty(level, StringUtils.concatenate(fields, StringUtils.DEFAULT_DELIMITER))
    info._2.setProperty(level, StringUtils.concatenate(arrays, StringUtils.DEFAULT_DELIMITER))
  }
}