package org.elasticsearch.spark.sql

import java.util.Properties

import scala.Array.fallbackCanBuildFrom
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.JavaConverters.propertiesAsScalaMapConverter
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.MapType
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
import org.elasticsearch.hadoop.util.Assert
import org.elasticsearch.hadoop.util.IOUtils
import org.elasticsearch.hadoop.util.StringUtils

private[sql] object MappingUtils {
  case class Schema(field: Field, struct: StructType)

  val ROW_ORDER_PROPERTY = "es.internal.spark.sql.row.order"
  val ROOT_LEVEL_NAME = "_"

  def discoverMapping(cfg: Settings): Schema = {
    val field = discoverMappingAsField(cfg)
    val struct = convertToStruct(field, cfg)
    Schema(field, struct)
  }

  def discoverMappingAsField(cfg: Settings): Field = {
    val repo = new RestRepository(cfg)
    try {
      return repo.getMapping().skipHeaders()
    } finally {
      repo.close()
    }
  }

  private def convertToStruct(rootField: Field, cfg: Settings): StructType = {
    var fields = for (fl <- rootField.properties()) yield convertField(fl)
    if (cfg.getReadMetadata) {
      val metadataMap = new StructField(cfg.getReadMetadataField, new MapType(StringType, StringType, true), true)
      fields :+= metadataMap
    }
    new StructType(fields)
  }

  private def convertToStruct(field: Field): StructType = {
    new StructType(for (fl <- field.properties()) yield convertField(fl))
  }

  private def convertField(field: Field): StructField = {
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
      case OBJECT  => convertToStruct(field)
      // fall back to String
      case _       => StringType //throw new EsHadoopIllegalStateException("Unknown field type " + field);
    }

    return new StructField(field.name(), dataType, true)
  }

    def setRowOrder(settings: Settings, struct: StructType) = {
    val rowOrder = detectRowOrder(settings, struct)
    // save the field in the settings to pass it to the value reader
    settings.setProperty(ROW_ORDER_PROPERTY, IOUtils.propsToString(rowOrder))
  }

  def getRowOrder(settings: Settings) = {
    val rowOrderString = settings.getProperty(ROW_ORDER_PROPERTY)
    Assert.hasText(rowOrderString, "no schema/row order detected...")

    val rowOrderProps = IOUtils.propsFromString(rowOrderString)

    val map = new scala.collection.mutable.LinkedHashMap[String, Seq[String]]

    for (prop <- rowOrderProps.asScala) {
      map.put(prop._1, new ArrayBuffer ++= (StringUtils.tokenize(prop._2).asScala))
    }

    map
  }

  private def detectRowOrder(settings: Settings, struct: StructType): Properties = {
    val rowOrder = new Properties

    doDetectOrder(rowOrder, ROOT_LEVEL_NAME, struct)
    val csv = settings.getScrollFields()
    // if a projection is applied, use that instead
    if (StringUtils.hasText(csv)) {
      rowOrder.setProperty(ROOT_LEVEL_NAME, csv)
    }
    rowOrder
  }

  private def doDetectOrder(properties: Properties, level: String, struct: StructType) {
    val list = new java.util.ArrayList[String]

    for (field <- struct.fields) {
      list.add(field.name)
      if (field.dataType.isInstanceOf[StructType]) {
        doDetectOrder(properties, field.name, field.dataType.asInstanceOf[StructType])
      }
    }

    properties.setProperty(level, StringUtils.concatenate(list, StringUtils.DEFAULT_DELIMITER))
  }
}