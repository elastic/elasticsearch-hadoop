package org.elasticsearch.spark.sql

import org.elasticsearch.hadoop.cfg.Settings

object SchemaUtilsTestable {

  def discoverMapping(cfg: Settings) = SchemaUtils.discoverMapping(cfg)

  def rowInfo(cfg: Settings) = {
    val schema = SchemaUtils.discoverMapping(cfg)
    SchemaUtils.setRowInfo(cfg, schema.struct)
    SchemaUtils.getRowInfo(cfg)
  }
}