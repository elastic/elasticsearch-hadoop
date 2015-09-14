package org.elasticsearch.spark.sql

import scala.collection.mutable.Buffer
import org.elasticsearch.hadoop.cfg.Settings
import org.elasticsearch.hadoop.serialization.SettingsAware
import org.elasticsearch.hadoop.EsHadoopIllegalStateException

private[sql] trait RowValueReader extends SettingsAware {

  protected var readMetadata = false
  var metadataField = ""
  protected var rowMap: scala.collection.Map[String, Seq[String]] = Map.empty
  protected var arrayMap: scala.collection.Map[String, Seq[String]] = Map.empty
  protected var currentField = Utils.ROOT_LEVEL_NAME

  abstract override def setSettings(settings: Settings) = {
    super.setSettings(settings)

    val csv = settings.getScrollFields
    readMetadata = settings.getReadMetadata
    val rowInfo = SchemaUtils.getRowInfo(settings)
    rowMap = rowInfo._1
    arrayMap = rowInfo._2
  }

  def rowInfo(currentField: String): Seq[String] = {
    rowMap.get(currentField) match {
      case Some(v) => v
      case None => throw new EsHadoopIllegalStateException(s"Field '$currentField' not found; typically this occurs with arrays which are not mapped as single value")
    }
  }

  def addToBuffer(esRow: ScalaEsRow, key: AnyRef, value: Any) {
    val pos = esRow.rowOrder.indexOf(key.toString())
    esRow.values.update(pos, value)
  }
}