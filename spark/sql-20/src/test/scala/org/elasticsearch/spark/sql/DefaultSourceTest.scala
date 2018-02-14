package org.elasticsearch.spark.sql

import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

class DefaultSourceTest {

  @Test
  def parameters(): Unit = {
    val settings = new mutable.LinkedHashMap[String, String]()
    settings.put("path", "wrong")
    settings.put("resource", "wrong")
    settings.put("es_resource", "preferred")
    settings.put("unrelated", "unrelated")

    val relation = new DefaultSource().params(settings.toMap)

    assertEquals(Map("es.resource" -> "preferred", "es.unrelated" -> "unrelated"), relation)
  }
}