package org.elasticsearch.spark.sql

import scala.Array.fallbackCanBuildFrom
import scala.collection.JavaConverters.seqAsJavaListConverter

import org.apache.spark.sql.ArrayType
import org.apache.spark.sql.DataType
import org.apache.spark.sql.DecimalType
import org.apache.spark.sql.MapType
import org.apache.spark.sql.api.java.{ DataType => JDataType }
import org.apache.spark.sql.api.java.{ DecimalType => JDecimalType }
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
import org.elasticsearch.hadoop.serialization.FieldType.OBJECT
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
      case OBJECT  => convertToStruct(field)
      // fall back to String
      case _       => StringType //throw new EsHadoopIllegalStateException("Unknown field type " + field);
    }

    return new StructField(field.name(), dataType, true)
  }
}
