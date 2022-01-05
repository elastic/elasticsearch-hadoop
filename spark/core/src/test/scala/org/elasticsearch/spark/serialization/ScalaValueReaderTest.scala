package org.elasticsearch.spark.serialization

import org.elasticsearch.hadoop.serialization.FieldType.DATE_NANOS
import org.elasticsearch.hadoop.serialization.Parser
import org.junit.Assert._
import org.junit.Test
import org.mockito.Mockito

import java.sql.Timestamp
import java.util.Date

class ScalaValueReaderTest {
  @Test
  def testCreateDateNanos(): Unit = {
    val reader = new ScalaValueReader()
    val nanoDate = reader.createDateNanos("2015-01-01T12:10:30.123456789Z")
    assertEquals(1420114230123l, nanoDate.getTime)
    assertEquals(123456789, nanoDate.getNanos)
  }

  @Test
  def testReadValue(): Unit = {
    val reader = new ScalaValueReader()
    val parser = Mockito.mock(classOf[Parser])

    Mockito.when(parser.currentToken()).thenReturn(Parser.Token.VALUE_STRING)
    val stringValue: String = reader.readValue(parser, "2015-01-01T12:10:30.123456789Z", DATE_NANOS).asInstanceOf[String]
    assertEquals("2015-01-01T12:10:30.123456789Z", stringValue)

    Mockito.when(parser.currentToken()).thenReturn(Parser.Token.VALUE_NUMBER)
    Mockito.when(parser.longValue()).thenReturn(1420114230123l)
    val dateLong = reader.readValue(parser, "1420114230123", DATE_NANOS).asInstanceOf[Long]
    assertEquals(1420114230123l, dateLong)

    reader.richDate = true
    Mockito.when(parser.currentToken()).thenReturn(Parser.Token.VALUE_STRING)
    val timestamp = reader.readValue(parser, "2015-01-01T12:10:30.123456789Z", DATE_NANOS).asInstanceOf[Timestamp]
    assertEquals(1420114230123l, timestamp.getTime)
    assertEquals(123456789, timestamp.getNanos)

    Mockito.when(parser.currentToken()).thenReturn(Parser.Token.VALUE_NUMBER)
    Mockito.when(parser.longValue()).thenReturn(1420114230123l)
    val date = reader.readValue(parser, "1420114230123", DATE_NANOS).asInstanceOf[Date]
    assertEquals(1420114230123l, date.getTime)
  }
}
