package org.elasticsearch.spark.serialization

import org.elasticsearch.spark.serialization.ReflectionUtils._
import org.junit.Test
import org.junit.Assert._
import org.hamcrest.Matchers._

class ScalaReflectionUtilsTest {

  @Test
  def testJavaBean() {
    val values = javaBeanAsMap(new Bean("1", Integer.valueOf(1), true))
    assertEquals(Map("id" -> 1, "bool" -> true, "foo" -> "1"), values)
  }

  @Test
  def testCaseClassValues() {
    val cc = SimpleCaseClass(1, "simpleClass")
    assertTrue(isCaseClass(cc))
    val values = caseClassValues(cc)

    //println(values)
    assertEquals(Map("i" -> 1, "s" -> "simpleClass"), values)

    val ccv = CaseClassWithValue(2, "caseClassWithVal")
    assertTrue(isCaseClass(ccv))
    val valuesccv = caseClassValues(ccv)

    //println(valuesccv)
    assertEquals(Map("first" -> 2, "second" -> "caseClassWithVal"), valuesccv)
  }
}

case class SimpleCaseClass(i: Int, s: String) {
}

case class CaseClassWithValue(first: Int, second: String) {
  var internal = "internal"
}
