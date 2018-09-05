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

import java.util.ArrayList
import java.util.{LinkedHashSet => JHashSet}
import java.util.{List => JList}
import java.util.{Map => JMap}
import java.util.Properties

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.JavaConverters.propertiesAsScalaMapConverter
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.DataType
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
import org.elasticsearch.hadoop.rest.InitializationUtils
import org.elasticsearch.hadoop.rest.RestRepository
import org.elasticsearch.hadoop.serialization.FieldType.BINARY
import org.elasticsearch.hadoop.serialization.FieldType.BOOLEAN
import org.elasticsearch.hadoop.serialization.FieldType.BYTE
import org.elasticsearch.hadoop.serialization.FieldType.DATE
import org.elasticsearch.hadoop.serialization.FieldType.DOUBLE
import org.elasticsearch.hadoop.serialization.FieldType.HALF_FLOAT
import org.elasticsearch.hadoop.serialization.FieldType.SCALED_FLOAT
import org.elasticsearch.hadoop.serialization.FieldType.FLOAT
import org.elasticsearch.hadoop.serialization.FieldType.GEO_POINT
import org.elasticsearch.hadoop.serialization.FieldType.GEO_SHAPE
import org.elasticsearch.hadoop.serialization.FieldType.INTEGER
import org.elasticsearch.hadoop.serialization.FieldType.JOIN
import org.elasticsearch.hadoop.serialization.FieldType.KEYWORD
import org.elasticsearch.hadoop.serialization.FieldType.LONG
import org.elasticsearch.hadoop.serialization.FieldType.NESTED
import org.elasticsearch.hadoop.serialization.FieldType.NULL
import org.elasticsearch.hadoop.serialization.FieldType.OBJECT
import org.elasticsearch.hadoop.serialization.FieldType.SHORT
import org.elasticsearch.hadoop.serialization.FieldType.STRING
import org.elasticsearch.hadoop.serialization.FieldType.TEXT
import org.elasticsearch.hadoop.serialization.dto.mapping.Field
import org.elasticsearch.hadoop.serialization.dto.mapping.GeoField
import org.elasticsearch.hadoop.serialization.dto.mapping.GeoPointType
import org.elasticsearch.hadoop.serialization.dto.mapping.GeoShapeType
import org.elasticsearch.hadoop.serialization.dto.mapping.Mapping
import org.elasticsearch.hadoop.serialization.dto.mapping.MappingUtils
import org.elasticsearch.hadoop.serialization.field.FieldFilter
import org.elasticsearch.hadoop.serialization.field.FieldFilter.NumberedInclude
import org.elasticsearch.hadoop.util.Assert
import org.elasticsearch.hadoop.util.IOUtils
import org.elasticsearch.hadoop.util.SettingsUtils
import org.elasticsearch.hadoop.util.StringUtils
import org.elasticsearch.spark.sql.Utils.ROOT_LEVEL_NAME
import org.elasticsearch.spark.sql.Utils.ROW_INFO_ARRAY_PROPERTY
import org.elasticsearch.spark.sql.Utils.ROW_INFO_ORDER_PROPERTY

private[sql] object SchemaUtils {
  case class Schema(mapping: Mapping, struct: StructType)

  def discoverMapping(cfg: Settings): Schema = {
    val (mapping, geoInfo) = discoverMappingAndGeoFields(cfg)
    val struct = convertToStruct(mapping, geoInfo, cfg)
    Schema(mapping, struct)
  }

  def discoverMappingAndGeoFields(cfg: Settings): (Mapping, JMap[String, GeoField]) = {
    InitializationUtils.validateSettings(cfg)
    InitializationUtils.discoverClusterInfo(cfg, Utils.LOGGER)

    val repo = new RestRepository(cfg)
    try {
      if (repo.indexExists(true)) {
        var mappingSet = repo.getMappings
        if (mappingSet == null || mappingSet.isEmpty) {
          throw new EsHadoopIllegalArgumentException(s"Cannot find mapping for ${cfg.getResourceRead} - one is required before using Spark SQL")
        }
        var mapping = mappingSet.getResolvedView
        mapping = MappingUtils.filterMapping(mapping, cfg)
        val geoInfo = repo.sampleGeoFields(mapping)
        
        // apply mapping filtering only when present to minimize configuration settings (big when dealing with large mappings)
        if (StringUtils.hasText(cfg.getReadFieldInclude) || StringUtils.hasText(cfg.getReadFieldExclude)) {
          // NB: metadata field is synthetic so it doesn't have to be filtered
          // its presence is controlled through the dedicated config setting
          cfg.setProperty(InternalConfigurationOptions.INTERNAL_ES_TARGET_FIELDS, StringUtils.concatenate(mapping.flatten.keySet(), StringUtils.DEFAULT_DELIMITER))
        }
        (mapping, geoInfo)
      }
      else {
        throw new EsHadoopIllegalArgumentException(s"Cannot find mapping for ${cfg.getResourceRead} - one is required before using Spark SQL")
      }
    } finally {
      repo.close()
    }
  }

  def convertToStruct(mapping: Mapping, geoInfo: JMap[String, GeoField], cfg: Settings): StructType = {
    val arrayIncludes = SettingsUtils.getFieldArrayFilterInclude(cfg)
    val arrayExcludes = StringUtils.tokenize(cfg.getReadFieldAsArrayExclude)

    var fields = for (fl <- mapping.getFields) yield convertField(fl, geoInfo, null, arrayIncludes, arrayExcludes, cfg)
    if (cfg.getReadMetadata) {
      // enrich structure
      val metadataMap = DataTypes.createStructField(cfg.getReadMetadataField, DataTypes.createMapType(StringType, StringType, true), true)
      fields :+= metadataMap
    }

    DataTypes.createStructType(fields)
  }

  private def convertToStruct(field: Field, geoInfo: JMap[String, GeoField], parentName: String, 
                              arrayIncludes: JList[NumberedInclude], arrayExcludes: JList[String], cfg:Settings): StructType = {
    DataTypes.createStructType(for (fl <- field.properties()) yield convertField(fl, geoInfo, parentName, arrayIncludes, arrayExcludes, cfg))
  }

  private def convertField(field: Field, geoInfo: JMap[String, GeoField], parentName: String, 
                           arrayIncludes: JList[NumberedInclude], arrayExcludes: JList[String], cfg:Settings): StructField = {
    val absoluteName = if (parentName != null) parentName + "." + field.name() else field.name()
    val matched = FieldFilter.filter(absoluteName, arrayIncludes, arrayExcludes, false)
    val createArray = !arrayIncludes.isEmpty() && matched.matched

    var dataType = Utils.extractType(field) match {
      case NULL      => NullType
      case BINARY    => BinaryType
      case BOOLEAN   => BooleanType
      case BYTE      => ByteType
      case SHORT     => ShortType
      case INTEGER   => IntegerType
      case LONG      => LongType
      case FLOAT     => FloatType
      case DOUBLE    => DoubleType
      // String type
      case STRING    => StringType
      case TEXT      => StringType
      case KEYWORD   => StringType
      case HALF_FLOAT => FloatType
      case SCALED_FLOAT => FloatType
      case DATE      => if (cfg.getMappingDateRich) TimestampType else StringType
      case OBJECT    => convertToStruct(field, geoInfo, absoluteName, arrayIncludes, arrayExcludes, cfg)
      case NESTED    => DataTypes.createArrayType(convertToStruct(field, geoInfo, absoluteName, arrayIncludes, arrayExcludes, cfg))
      case JOIN      => convertToStruct(field, geoInfo, absoluteName, arrayIncludes, arrayExcludes, cfg)

      // GEO
      case GEO_POINT => {
        val geoPoint = geoInfo.get(absoluteName) match {
          case GeoPointType.LON_LAT_ARRAY  => DataTypes.createArrayType(DoubleType)
          case GeoPointType.GEOHASH        => StringType
          case GeoPointType.LAT_LON_STRING => StringType
          case GeoPointType.LAT_LON_OBJECT => {
            val lon = DataTypes.createStructField("lat", DoubleType, true)
            val lat = DataTypes.createStructField("lon", DoubleType, true)
            DataTypes.createStructType(Array(lon,lat)) 
          }
        }
        
        if (Utils.LOGGER.isDebugEnabled()) {
          Utils.LOGGER.debug(s"Detected field [${absoluteName}] as a GeoPoint with format ${geoPoint.simpleString}")
        }
        geoPoint
      }
      case GEO_SHAPE => {
        val fields = new ArrayList[StructField]()
        fields.add(DataTypes.createStructField("type", StringType, true))
        val COORD = "coordinates"
        geoInfo.get(absoluteName) match {
          case GeoShapeType.POINT               => fields.add(DataTypes.createStructField(COORD, DataTypes.createArrayType(DoubleType), true))
          case GeoShapeType.LINE_STRING         => fields.add(DataTypes.createStructField(COORD, createNestedArray(DoubleType, 2), true))
          case GeoShapeType.POLYGON             => { 
            fields.add(DataTypes.createStructField(COORD, createNestedArray(DoubleType, 3), true))
            fields.add(DataTypes.createStructField("orientation", StringType, true))
          }
          case GeoShapeType.MULTI_POINT         => fields.add(DataTypes.createStructField(COORD, createNestedArray(DoubleType, 2), true))
          case GeoShapeType.MULTI_LINE_STRING   => fields.add(DataTypes.createStructField(COORD, createNestedArray(DoubleType, 3), true))
          case GeoShapeType.MULTI_POLYGON       => fields.add(DataTypes.createStructField(COORD, createNestedArray(DoubleType, 4), true))
          case GeoShapeType.GEOMETRY_COLLECTION => throw new EsHadoopIllegalArgumentException(s"Geoshape $geoInfo not supported")
          case GeoShapeType.ENVELOPE            => fields.add(DataTypes.createStructField(COORD, createNestedArray(DoubleType, 2), true))
          case GeoShapeType.CIRCLE              => {
            fields.add(DataTypes.createStructField(COORD, DataTypes.createArrayType(DoubleType), true))
            fields.add(DataTypes.createStructField("radius", StringType, true))
          }
        }
        val geoShape = DataTypes.createStructType(fields)
        
        if (Utils.LOGGER.isDebugEnabled()) {
          Utils.LOGGER.debug(s"Detected field [${absoluteName}] as a GeoShape with format ${geoShape.simpleString}")
        }
        geoShape
      }
      // fall back to String
      case _         => StringType //throw new EsHadoopIllegalStateException("Unknown field type " + field);
    }

    if (createArray) {
      // can't call createNestedArray for some reason...
      for (_ <- 0 until matched.depth) {
        dataType = DataTypes.createArrayType(dataType)
      }
    }
    DataTypes.createStructField(field.name(), dataType, true)
  }
  
  private def createNestedArray(elementType: DataType, depth: Int): DataType = {
      var array = elementType
      for (_ <- 0 until depth) {
        array = DataTypes.createArrayType(array)
      }
      array
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

    val requiredFields = settings.getProperty(Utils.DATA_SOURCE_REQUIRED_COLUMNS)
    if (StringUtils.hasText(requiredFields)) {
      rowInfo._1.setProperty(ROOT_LEVEL_NAME, requiredFields)
    }

    rowInfo
  }

  private def doDetectInfo(info: (Properties, Properties), level: String, dataType: DataType): Unit = {
    dataType match {
      case s: StructType => {
        val fields = new java.util.ArrayList[String]
        for (field <- s) {
          fields.add(field.name)
          doDetectInfo(info, if (level != ROOT_LEVEL_NAME) level + "." + field.name else field.name, field.dataType)
        }
        info._1.setProperty(level, StringUtils.concatenate(fields, StringUtils.DEFAULT_DELIMITER))
      }
      case a: ArrayType => {
        val prop = info._2.getProperty(level)
        val depth = (if(StringUtils.hasText(prop)) Integer.parseInt(prop) else 0) + 1
        info._2.setProperty(level, String.valueOf(depth))
        doDetectInfo(info, level, a.elementType)
      }
      // ignore primitives
      case _ => // ignore
    }
  }
}