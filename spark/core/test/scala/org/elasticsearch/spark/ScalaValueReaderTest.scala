package org.elasticsearch.spark

import org.elasticsearch.hadoop.serialization.AbstractValueReaderTest
import org.elasticsearch.spark.serialization.ScalaValueReader
import org.junit.Assert._
import org.elasticsearch.hadoop.serialization.JdkValueReaderTest
import org.elasticsearch.spark.serialization.ScalaValueReader
import org.elasticsearch.hadoop.serialization.builder.ValueReader

class ScalaValueReaderTest extends JdkValueReaderTest {

    override def createValueReader() = new ScalaValueReader()

    override def checkNull(result: Object) { assertEquals(None, result)}
    override def checkEmptyString(result: Object) { assertEquals(None, result)}
    override def checkInteger(result: Object) { assertEquals(Int.MaxValue, result)}
    override def checkLong(result: Object) { assertEquals(Long.MaxValue, result)}
    override def checkDouble(result: Object) { assertEquals(Double.MaxValue, result)}
    override def checkFloat(result: Object) { assertEquals(Float.MaxValue.toString, result.toString())}
    override def checkBoolean(result: Object) { assertEquals(Boolean.box(true), result)}
  
}
