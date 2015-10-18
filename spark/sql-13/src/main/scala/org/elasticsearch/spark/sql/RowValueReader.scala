package org.elasticsearch.spark.sql

import java.util.Collections
import java.util.{Set => JSet}

import org.elasticsearch.hadoop.EsHadoopIllegalStateException
import org.elasticsearch.hadoop.cfg.Settings
import org.elasticsearch.hadoop.serialization.SettingsAware

private[sql] trait RowValueReader extends SettingsAware {

  protected var readMetadata = false
  var metadataField = ""
  // columns for each row (loaded on each new row)
  protected var rowColumnsMap: scala.collection.Map[String, Seq[String]] = Map.empty
  // fields that need to be handled as arrays (in absolute name format)
  protected var arrayFields: JSet[String] = Collections.emptySet()
  protected var currentField = Utils.ROOT_LEVEL_NAME

  abstract override def setSettings(settings: Settings) = {
    super.setSettings(settings)

    val csv = settings.getScrollFields
    readMetadata = settings.getReadMetadata
    val rowInfo = SchemaUtils.getRowInfo(settings)
    rowColumnsMap = rowInfo._1
    arrayFields = rowInfo._2
  }

  def rowColumns(currentField: String): Seq[String] = {
    rowColumnsMap.get(currentField) match {
      case Some(v) => v
      case None => throw new EsHadoopIllegalStateException(s"Field '$currentField' not found; typically this occurs with arrays which are not mapped as single value")
    }
  }

  def addToBuffer(esRow: ScalaEsRow, key: AnyRef, value: Any) {
    val pos = esRow.rowOrder.indexOf(key.toString())
    esRow.values.update(pos, value)
  }
}