package org.elasticsearch.spark.sql

import scala.Array.fallbackCanBuildFrom
import scala.collection.JavaConverters.seqAsJavaListConverter

import org.apache.spark.sql.ArrayType
import org.apache.spark.sql.DataType
import org.apache.spark.sql.DecimalType
import org.apache.spark.sql.MapType
import org.apache.spark.sql.api.java.{ DataType => JDataType }
import org.apache.spark.sql.api.java.{ StructField => JStructField }
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
import org.elasticsearch.hadoop.EsHadoopIllegalStateException
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
import org.elasticsearch.hadoop.serialization.FieldType.SHORT
import org.elasticsearch.hadoop.serialization.FieldType.STRING
import org.elasticsearch.hadoop.serialization.dto.mapping.Field

private[sql] object MappingUtils {

  def discoverMapping(cfg: Settings): StructType = {
    val repo = new RestRepository(cfg)
    try {
      return convertToStruct(repo.getMapping().skipHeaders());
    } finally {
      repo.close()
    }
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
      case _       => throw new EsHadoopIllegalStateException("Unknown field type " + field);
    }

    return new StructField(field.name(), dataType, true)
  }
}

protected[sql] object DataTypeConversions {

  /**
   * Returns the equivalent StructField in Scala for the given StructField in Java.
   */
  def asJavaStructField(scalaStructField: StructField): JStructField = {
    JDataType.createStructField(
      scalaStructField.name,
      asJavaDataType(scalaStructField.dataType),
      scalaStructField.nullable)
  }

  /**
   * Returns the equivalent DataType in Java for the given DataType in Scala.
   */
  def asJavaDataType(scalaDataType: DataType): JDataType = scalaDataType match {
    case StringType    => JDataType.StringType
    case BinaryType    => JDataType.BinaryType
    case BooleanType   => JDataType.BooleanType
    case TimestampType => JDataType.TimestampType
    case DecimalType   => JDataType.DecimalType
    case DoubleType    => JDataType.DoubleType
    case FloatType     => JDataType.FloatType
    case ByteType      => JDataType.ByteType
    case IntegerType   => JDataType.IntegerType
    case LongType      => JDataType.LongType
    case ShortType     => JDataType.ShortType

    case arrayType: ArrayType => JDataType.createArrayType(
      asJavaDataType(arrayType.elementType), arrayType.containsNull)
    case mapType: MapType => JDataType.createMapType(
      asJavaDataType(mapType.keyType),
      asJavaDataType(mapType.valueType),
      mapType.valueContainsNull)
    case structType: StructType => JDataType.createStructType(
      structType.fields.map(asJavaStructField).asJava)
  }

  /**
   * Returns the equivalent StructField in Scala for the given StructField in Java.
   */
  def asScalaStructField(javaStructField: JStructField): StructField = {
    StructField(
      javaStructField.getName,
      asScalaDataType(javaStructField.getDataType),
      javaStructField.isNullable)
  }

  /**
   * Returns the equivalent DataType in Scala for the given DataType in Java.
   */
  def asScalaDataType(javaDataType: JDataType): DataType = javaDataType match {
    case stringType: org.apache.spark.sql.api.java.StringType =>
      StringType
    case binaryType: org.apache.spark.sql.api.java.BinaryType =>
      BinaryType
    case booleanType: org.apache.spark.sql.api.java.BooleanType =>
      BooleanType
    case timestampType: org.apache.spark.sql.api.java.TimestampType =>
      TimestampType
    case decimalType: org.apache.spark.sql.api.java.DecimalType =>
      DecimalType
    case doubleType: org.apache.spark.sql.api.java.DoubleType =>
      DoubleType
    case floatType: org.apache.spark.sql.api.java.FloatType =>
      FloatType
    case byteType: org.apache.spark.sql.api.java.ByteType =>
      ByteType
    case integerType: org.apache.spark.sql.api.java.IntegerType =>
      IntegerType
    case longType: org.apache.spark.sql.api.java.LongType =>
      LongType
    case shortType: org.apache.spark.sql.api.java.ShortType =>
      ShortType

    case arrayType: org.apache.spark.sql.api.java.ArrayType =>
      ArrayType(asScalaDataType(arrayType.getElementType), arrayType.isContainsNull)
    case mapType: org.apache.spark.sql.api.java.MapType =>
      MapType(
        asScalaDataType(mapType.getKeyType),
        asScalaDataType(mapType.getValueType),
        mapType.isValueContainsNull)
    case structType: org.apache.spark.sql.api.java.StructType =>
      StructType(structType.getFields.map(asScalaStructField))
  }
}