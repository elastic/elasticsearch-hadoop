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

import java.sql.Date
import java.sql.Timestamp
import java.util.{Map => JMap}

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.{Map => SMap}
import scala.collection.Seq
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes.BinaryType
import org.apache.spark.sql.types.DataTypes.BooleanType
import org.apache.spark.sql.types.DataTypes.ByteType
import org.apache.spark.sql.types.DataTypes.DateType
import org.apache.spark.sql.types.DataTypes.DoubleType
import org.apache.spark.sql.types.DataTypes.FloatType
import org.apache.spark.sql.types.DataTypes.IntegerType
import org.apache.spark.sql.types.DataTypes.LongType
import org.apache.spark.sql.types.DataTypes.ShortType
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types.DataTypes.TimestampType
import org.apache.spark.sql.types.MapType
import org.apache.spark.sql.types.StructType
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_SPARK_DATAFRAME_WRITE_NULL_VALUES_DEFAULT
import org.elasticsearch.hadoop.cfg.Settings
import org.elasticsearch.hadoop.serialization.EsHadoopSerializationException
import org.elasticsearch.hadoop.serialization.Generator
import org.elasticsearch.hadoop.serialization.builder.FilteringValueWriter
import org.elasticsearch.hadoop.serialization.builder.ValueWriter.Result
import org.elasticsearch.hadoop.util.unit.Booleans


class DataFrameValueWriter(writeUnknownTypes: Boolean = false) extends FilteringValueWriter[(Row, StructType)] {

  def this() {
    this(false)
  }

  private var writeNullValues = Booleans.parseBoolean(ES_SPARK_DATAFRAME_WRITE_NULL_VALUES_DEFAULT)

  override def setSettings(settings: Settings): Unit = {
    super.setSettings(settings)
    writeNullValues = settings.getDataFrameWriteNullValues
  }

  override def write(value: (Row, StructType), generator: Generator): Result = {
    val row = value._1
    val schema = value._2

    writeStruct(schema, row, generator)
  }

  private[spark] def writeStruct(schema: StructType, value: Any, generator: Generator): Result = {
    value match {
      case r: Row =>
        generator.writeBeginObject()

        schema.fields.view.zipWithIndex foreach {
          case (field, index) =>
            if (shouldKeep(generator.getParentPath,field.name)) {
              if (!r.isNullAt(index)) {
                generator.writeFieldName(field.name)
                val result = write(field.dataType, r(index), generator)
                if (!result.isSuccesful) {
                  return handleUnknown(value, generator)
                }
              } else if (writeNullValues) {
                generator.writeFieldName(field.name)
                generator.writeNull()
              }
            }
        }
        generator.writeEndObject()

        Result.SUCCESFUL()
    }
  }

  private[spark] def write(schema: DataType, value: Any, generator: Generator): Result = {
    schema match {
      case s @ StructType(_)    => writeStruct(s, value, generator)
      case a @ ArrayType(_, _)  => writeArray(a, value, generator)
      case m @ MapType(_, _, _) => writeMap(m, value, generator)
      case _                    => writePrimitive(schema, value, generator)
    }
  }

  private[spark] def writeArray(schema: ArrayType, value: Any, generator: Generator): Result = {
    value match {
      case a: Array[_] => doWriteSeq(schema.elementType, a, generator)
      case s: Seq[_]   => doWriteSeq(schema.elementType, s, generator)
      // unknown array type
      case _           => handleUnknown(value, generator)
    }
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
      case _              => handleUnknown(value, generator)
    }
  }

  private def doWriteMap(schema: MapType, value: SMap[_, _], generator: Generator): Result = {
    generator.writeBeginObject()

    if (value != null) {
      for ((k, v) <- value) {
        if (shouldKeep(generator.getParentPath(), k.toString())) {
          generator.writeFieldName(k.toString)
          val result = write(schema.valueType, v, generator)
          if (!result.isSuccesful()) {
            return handleUnknown(v, generator)
          }
        }
      }
    }

    generator.writeEndObject()
    Result.SUCCESFUL()
  }

  private[spark] def writePrimitive(schema: DataType, value: Any, generator: Generator): Result = {
    if (value == null) {
      generator.writeNull()
    }
    else schema match {
      case BinaryType    => generator.writeBinary(value.asInstanceOf[Array[Byte]])
      case BooleanType   => generator.writeBoolean(value.asInstanceOf[Boolean])
      case ByteType      => generator.writeNumber(value.asInstanceOf[Byte])
      case ShortType     => generator.writeNumber(value.asInstanceOf[Short])
      case IntegerType   => generator.writeNumber(value.asInstanceOf[Int])
      case LongType      => generator.writeNumber(value.asInstanceOf[Long])
      case DoubleType    => generator.writeNumber(value.asInstanceOf[Double])
      case FloatType     => generator.writeNumber(value.asInstanceOf[Float])
      case TimestampType => generator.writeNumber(value.asInstanceOf[Timestamp].getTime())
      case DateType      => generator.writeNumber(value.asInstanceOf[Date].getTime())
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
      println("can't handle type " + value);
      Result.FAILED(value)
    } else {
      generator.writeString(value.toString())
      Result.SUCCESFUL()
    }
  }
}