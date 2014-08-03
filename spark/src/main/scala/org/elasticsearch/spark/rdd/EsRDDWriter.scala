package org.elasticsearch.spark.rdd

import scala.reflect.ClassTag
import org.apache.commons.logging.LogFactory
import org.apache.spark.TaskContext
import org.elasticsearch.hadoop.cfg.PropertiesSettings
import org.elasticsearch.hadoop.rest.InitializationUtils
import org.elasticsearch.hadoop.rest.RestService
import org.elasticsearch.hadoop.serialization.JdkBytesConverter
import org.elasticsearch.hadoop.serialization.MapFieldExtractor
import org.elasticsearch.spark.serialization.ScalaValueWriter

private[spark] class EsRDDWriter[T: ClassTag]
	(val serializedSettings: String) 
	extends Serializable {
  
  private var log = LogFactory.getLog(classOf[EsRDDWriter[_]])
  
  lazy val settings = {
    new PropertiesSettings().load(serializedSettings);
  }
  
  def write(taskContext: TaskContext, data: Iterator[T]) {
      InitializationUtils.setValueWriterIfNotSet(settings, classOf[ScalaValueWriter], log);
      InitializationUtils.setBytesConverterIfNeeded(settings, classOf[JdkBytesConverter], log);
      InitializationUtils.setFieldExtractorIfNotSet(settings, classOf[MapFieldExtractor], log);
      
      val writer = RestService.createWriter(settings, taskContext.partitionId, -1, log)
      
      taskContext.addOnCompleteCallback(() => writer.close())
      
      while (data.hasNext)
    	  writer.repository.writeToIndex(data.next);
  }
}