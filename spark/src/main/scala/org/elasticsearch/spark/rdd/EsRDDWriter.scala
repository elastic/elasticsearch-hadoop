package org.elasticsearch.spark.rdd

import scala.reflect.ClassTag
import org.apache.commons.logging.LogFactory
import org.apache.spark.TaskContext
import org.elasticsearch.hadoop.cfg.PropertiesSettings
import org.elasticsearch.hadoop.rest.InitializationUtils
import org.elasticsearch.hadoop.serialization.builder.ValueWriter
import org.elasticsearch.spark.serialization.ScalaValueWriter
import org.elasticsearch.hadoop.serialization.BytesConverter
import org.elasticsearch.spark.serialization.ScalaMapFieldExtractor
import org.elasticsearch.hadoop.serialization.field.FieldExtractor
import org.elasticsearch.hadoop.serialization.JdkBytesConverter
import org.elasticsearch.hadoop.rest.RestService

private[spark] class EsRDDWriter[T: ClassTag]
	(val serializedSettings: String) 
	extends Serializable {
  
  protected val log = LogFactory.getLog(this.getClass())
  
  lazy val settings = {
    new PropertiesSettings().load(serializedSettings);
  }
  
  def write(taskContext: TaskContext, data: Iterator[T]) {
    InitializationUtils.setValueWriterIfNotSet(settings, valueWriter, log)
    InitializationUtils.setBytesConverterIfNeeded(settings, bytesConverter, log)
    InitializationUtils.setFieldExtractorIfNotSet(settings, fieldExtractor, log)

    val writer = RestService.createWriter(settings, taskContext.partitionId, -1, log)

    taskContext.addOnCompleteCallback(() => writer.close())

    while (data.hasNext)
      writer.repository.writeToIndex(processData(data))
  }
  
  protected def valueWriter: Class[_ <: ValueWriter[_]] = classOf[ScalaValueWriter]
  protected def bytesConverter: Class[_ <: BytesConverter] = classOf[JdkBytesConverter]
  protected def fieldExtractor: Class[_ <: FieldExtractor] = classOf[ScalaMapFieldExtractor]
  
  protected def processData(data: Iterator[T]): Any = { data.next }
}