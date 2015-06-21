package org.elasticsearch.spark.serialization

import java.util.EnumMap
import java.util.{Map => JMap}

import scala.collection.{Map => SMap}

import org.elasticsearch.hadoop.serialization.bulk.MetadataExtractor.{Metadata => InternalMetadata}
import org.elasticsearch.hadoop.serialization.bulk.PerEntityPoolingMetadataExtractor
import org.elasticsearch.spark.rdd.{Metadata => SparkMetadata}

private[spark] class ScalaMetadataExtractor extends PerEntityPoolingMetadataExtractor {

  override def getValue(metadata: InternalMetadata): AnyRef = {
    val sparkEnum = ScalaMetadataExtractor.toSparkEnum(metadata)

    if (sparkEnum == null) return null

    entity match {
      case jmap: JMap[_, _] => jmap.asInstanceOf[JMap[SparkMetadata, AnyRef]].get(sparkEnum)
      case smap: SMap[_, _] => smap.asInstanceOf[SMap[SparkMetadata, AnyRef]].getOrElse(sparkEnum, null)
      case _                => if (sparkEnum == SparkMetadata.ID) entity else null;
    }
  }
}

object ScalaMetadataExtractor {
  val map = new EnumMap[InternalMetadata, SparkMetadata](classOf[InternalMetadata])

  for (e <- SparkMetadata.values) {
    map.put(InternalMetadata.valueOf(e.name()), e)
  }

  def toSparkEnum(metadata: InternalMetadata) = map.get(metadata)
}