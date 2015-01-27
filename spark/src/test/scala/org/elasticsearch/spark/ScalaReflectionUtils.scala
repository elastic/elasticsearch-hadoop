package org.elasticsearch.spark

import org.elasticsearch.spark.serialization.ReflectionUtils._
import org.junit.BeforeClass
import org.junit.Test
import org.junit.Assert._
import org.hamcrest.Matchers._

class ScalaReflectionUtils {

  @Test
  def testJavaBean() {
    val info = javaBeansInfo(classOf[Bean])
    val values = javaBeansValues(new Bean("1", Integer.valueOf(1), true), info)
    assertEquals(Map("bar" -> 1, "bool" -> true, "foo" -> "1"), values)
  }

  @Test
  def testCaseClassIdentify() {
    assertFalse(isCaseClass(classOf[Bean]))
    assertTrue(isCaseClass(classOf[SimpleCaseClass]))
    assertTrue(isCaseClass(classOf[CaseClassWithValue]))
  }

  @Test
  def testCaseClassValues() {
    val cc = SimpleCaseClass(1, "simpleClass")
    val info = caseClassInfo(cc.getClass())
    println(caseClassValues(cc, info))

    val ccv = CaseClassWithValue(2, "caseClassWithVal")
    val infoccv = caseClassInfo(ccv.getClass())
    println(caseClassValues(ccv, infoccv))

  }
}

case class SimpleCaseClass(i: Int, s: String) {
}

case class CaseClassWithValue(first: Int, second: String) {
  var internal = "internal"
}
