package org.elasticsearch.spark.sql

import java.sql.Timestamp
import java.util.{Map => JMap}

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.{Map => SMap}
import scala.collection.Seq

import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.types.ArrayType
import org.apache.spark.sql.catalyst.types.BinaryType
import org.apache.spark.sql.catalyst.types.BooleanType
import org.apache.spark.sql.catalyst.types.ByteType
import org.apache.spark.sql.catalyst.types.DataType
import org.apache.spark.sql.catalyst.types.DecimalType
import org.apache.spark.sql.catalyst.types.DoubleType
import org.apache.spark.sql.catalyst.types.FloatType
import org.apache.spark.sql.catalyst.types.IntegerType
import org.apache.spark.sql.catalyst.types.LongType
import org.apache.spark.sql.catalyst.types.MapType
import org.apache.spark.sql.catalyst.types.ShortType
import org.apache.spark.sql.catalyst.types.StringType
import org.apache.spark.sql.catalyst.types.StructType
import org.apache.spark.sql.catalyst.types.TimestampType
import org.elasticsearch.hadoop.serialization.EsHadoopSerializationException
import org.elasticsearch.hadoop.serialization.Generator
import org.elasticsearch.hadoop.serialization.builder.FilteringValueWriter
import org.elasticsearch.hadoop.serialization.builder.ValueWriter.Result
import org.elasticsearch.spark.serialization.ScalaValueWriter

class SchemaRDDValueWriter(writeUnknownTypes: Boolean = false) extends FilteringValueWriter[(Row, StructType)] {

  def this() {
    this(false)
  }

  private val scalaValueWriter = new ScalaValueWriter(writeUnknownTypes)

  override def write(value: (Row, StructType), generator: Generator): Result = {
    val row = value._1
    val schema = value._2

    return writeStruct(schema, row, generator)
  }

  private[spark] def write(schema: DataType, value: Any, generator: Generator): Result = {
    schema match {
      case s @ StructType(_)    => writeStruct(s, value, generator)
      case a @ ArrayType(_, _)  => writeArray(a, value, generator)
      case m @ MapType(_, _, _) => writeMap(m, value, generator)
      case _                    => writePrimitive(value, schema, generator)
    }
  }

  private[spark] def writeStruct(schema: StructType, value: Any, generator: Generator): Result = {
    value match {
      case r: Row =>
        generator.writeBeginObject()

        schema.fields.view.zipWithIndex foreach {
          case (field, index) =>
            if (shouldKeep(generator.getParentPath(),field.name)) {
              generator.writeFieldName(field.name)
              if (r.isNullAt(index)) {
                generator.writeNull()
              } else {
                val result = write(field.dataType, r(index), generator)
                if (!result.isSuccesful()) {
                  return handleUnknown(value, generator)
                }
              }
            }
        }
        generator.writeEndObject()

        Result.SUCCESFUL()
    }
  }

  private[spark] def writeArray(schema: ArrayType, value: Any, generator: Generator): Result = {
    value match {
      case a: Array[_] => return doWriteSeq(schema.elementType, a, generator)
      case s: Seq[_]   => return doWriteSeq(schema.elementType, s, generator)
      // unknown array type
      case _           => return handleUnknown(value, generator)
    }
    Result.SUCCESFUL()
  }

  private def doWriteSeq(schema: DataType, value: Seq[_], generator: Generator): Result = {
    generator.writeBeginArray()
    if (value != null) {
      value.foreach { v =>
        val result = write(schema, v, generator)
        if (!result.isSuccesful()) {
          return handleUnknown(value, generator)
        }
      }
    }
    generator.writeEndArray()
    Result.SUCCESFUL()
  }

  private[spark] def writeMap(schema: MapType, value: Any, generator: Generator): Result = {
    value match {
      case sm: SMap[_, _] => doWriteMap(schema, sm, generator)
      case jm: JMap[_, _] => doWriteMap(schema, jm.asScala, generator)
      // unknown map type
      case _              => return handleUnknown(value, generator)
    }
    Result.SUCCESFUL()
  }

  private def doWriteMap(schema: MapType, value: SMap[_, _], generator: Generator): Result = {
    generator.writeBeginObject()

    for ((k, v) <- value) {
      if (shouldKeep(generator.getParentPath(), k.toString())) {
        generator.writeFieldName(k.toString)
        if (value != null) {
          val result = write(schema.valueType, v, generator)
          if (!result.isSuccesful()) {
            return handleUnknown(value, generator)
          }
        }
      }
    }

    generator.writeEndObject()
    Result.SUCCESFUL()
  }

  private[spark] def writePrimitive(value: Any, schema: DataType, generator: Generator): Result = {
    schema match {
      case BinaryType    => generator.writeBinary(value.asInstanceOf[Array[Byte]])
      case BooleanType   => generator.writeBoolean(value.asInstanceOf[Boolean])
      case ByteType      => generator.writeNumber(value.asInstanceOf[Byte])
      case ShortType     => generator.writeNumber(value.asInstanceOf[Short])
      case IntegerType   => generator.writeNumber(value.asInstanceOf[Int])
      case LongType      => generator.writeNumber(value.asInstanceOf[Long])
      case DoubleType    => generator.writeNumber(value.asInstanceOf[Double])
      case FloatType     => generator.writeNumber(value.asInstanceOf[Float])
      case TimestampType => generator.writeNumber(value.asInstanceOf[Timestamp].getTime())
      case StringType    => generator.writeString(value.toString)
      case _             => {
        val className = schema.getClass().getName()
        if ("org.apache.spark.sql.types.DecimalType".equals(className) || "org.apache.spark.sql.catalyst.types.DecimalType".equals(className)) {
          throw new EsHadoopSerializationException("Decimal types are not supported by Elasticsearch - consider using a different type (such as string)")
        }
        return handleUnknown(value, generator)
      }
    }
    Result.SUCCESFUL()
  }

  protected def handleUnknown(value: Any, generator: Generator): Result = {
    if (!writeUnknownTypes) {
      return Result.FAILED(value)
    }

    generator.writeString(value.toString())
    Result.SUCCESFUL()
  }
}