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
import org.elasticsearch.hadoop.serialization.builder.ValueWriter
import org.elasticsearch.spark.serialization.ScalaValueWriter

class SchemaRDDValueWriter(writeUnknownTypes: Boolean = false) extends ValueWriter[(Row, StructType)] {

  def this() {
    this(false)
  }
    
  private val scalaValueWriter = new ScalaValueWriter(writeUnknownTypes)

  override def write(value: (Row, StructType), generator: Generator): Boolean = {
    val row = value._1
    val schema = value._2
    
    return writeStruct(schema, row, generator)
  }

  private[spark] def write(schema: DataType, value: Any, generator: Generator): Boolean = {
    schema match {
      case s @ StructType(_)    => writeStruct(s, value, generator)
      case a @ ArrayType(_, _)  => writeArray(a, value, generator)
      case m @ MapType(_, _, _) => writeMap(m, value, generator)
      case _                    => writePrimitive(value, schema, generator)
    }
  }

  private[spark] def writeStruct(schema: StructType, value: Any, generator: Generator): Boolean = {
    value match {
      case r: Row =>
        generator.writeBeginObject()

        schema.fields.view.zipWithIndex foreach {
          case (field, index) =>
            generator.writeFieldName(field.name)
            if (r.isNullAt(index)) {
              generator.writeNull()
            } else {
              if (!write(field.dataType, r(index), generator)) {
                return handleUnknown(value, generator)
              }
            }
        }
        generator.writeEndObject()
        
        true
    }
  }

  private[spark] def writeArray(schema: ArrayType, value: Any, generator: Generator): Boolean = {
    value match {
      case a: Array[_] => return doWriteSeq(schema.elementType, a, generator)
      case s: Seq[_]   => return doWriteSeq(schema.elementType, s, generator)
      // unknown array type
      case _ 		   => return handleUnknown(value, generator) 
    }
    true
  }

  private def doWriteSeq(schema: DataType, value: Seq[_], generator: Generator): Boolean = {
    generator.writeBeginArray()
    if (value != null) {
      value.foreach { v =>
        if (!write(schema, v, generator)) {
          return handleUnknown(value, generator)
        }
      }
    }
    generator.writeEndArray()
    true
  }
  
  private[spark] def writeMap(schema: MapType, value: Any, generator: Generator): Boolean = {
    value match {
      case sm: SMap[_,_] => doWriteMap(schema, sm, generator)
      case jm: JMap[_,_] => doWriteMap(schema, jm.asScala, generator)
      // unknown map type
      case _             => return handleUnknown(value, generator) 
    }
    true
  }

  private def doWriteMap(schema: MapType, value: SMap[_, _], generator: Generator): Boolean = {
    generator.writeBeginObject()

    for ((k, v) <- value) {
      generator.writeFieldName(k.toString)
      if (value != null) {
        if (!write(schema.valueType, v, generator)) {
          return handleUnknown(value, generator)
        }
      }
    }

    generator.writeEndObject()
    true
  }

  private[spark] def writePrimitive(value: Any, schema: DataType, generator: Generator): Boolean = {
    schema match {
      case BinaryType    => generator.writeBinary(value.asInstanceOf[Array[Byte]])
      case BooleanType   => generator.writeBoolean(value.asInstanceOf[Boolean])
      case ByteType      => generator.writeNumber(value.asInstanceOf[Byte])
      case ShortType     => generator.writeNumber(value.asInstanceOf[Short])
      case IntegerType   => generator.writeNumber(value.asInstanceOf[Int])
      case LongType      => generator.writeNumber(value.asInstanceOf[Long])
      case DoubleType    => generator.writeNumber(value.asInstanceOf[Double])
      case FloatType     => generator.writeNumber(value.asInstanceOf[Float])
      case DecimalType   => throw new EsHadoopSerializationException("Decimal types are not supported by Elasticsearch - consider using a different type (such as string)")
      case TimestampType => generator.writeNumber(value.asInstanceOf[Timestamp].getTime())
      case StringType    => generator.writeString(value.toString)
      case _             => return handleUnknown(value, generator) 
    }
    true
  }

  protected def handleUnknown(value: Any, generator: Generator): Boolean = {
    if (!writeUnknownTypes) {
      return false
    }

    generator.writeString(value.toString())
   	true
  }
}