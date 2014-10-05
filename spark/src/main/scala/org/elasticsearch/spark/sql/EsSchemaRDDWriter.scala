package org.elasticsearch.spark.sql

import org.apache.spark.sql.catalyst.expressions.Row
import org.elasticsearch.spark.rdd.EsRDDWriter
import org.elasticsearch.spark.serialization.ScalaValueWriter
import org.elasticsearch.hadoop.serialization.JdkBytesConverter
import org.elasticsearch.hadoop.serialization.builder.ValueWriter
import org.elasticsearch.hadoop.serialization.field.FieldExtractor
import org.elasticsearch.hadoop.serialization.BytesConverter
import org.apache.spark.sql.catalyst.types.StructType

private[spark] class EsSchemaRDDWriter
	(schema: StructType, override val serializedSettings: String) 
	extends EsRDDWriter[Row](serializedSettings:String) {
  
  override protected def valueWriter: Class[_ <: ValueWriter[_]] = classOf[SchemaRDDValueWriter]
  override protected def bytesConverter: Class[_ <: BytesConverter] = classOf[JdkBytesConverter]
  override protected def fieldExtractor: Class[_ <: FieldExtractor] = classOf[SchemaRDDFieldExtractor]

  override protected def processData(data: Iterator[Row]): Any = { (data.next, schema) }
}