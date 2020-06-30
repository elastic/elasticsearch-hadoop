package org.elasticsearch.spark.sql.streaming

import java.lang.Boolean.{FALSE, TRUE}
import java.{lang => jl}
import java.{util => ju}

import org.apache.spark.sql.SparkSession
import org.junit.Assert
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters
import org.mockito.Mockito

object StructuredStreamingVersionLockTest {

  @Parameters
  def testParams(): ju.Collection[Array[jl.Object]] = {
    val list = new ju.ArrayList[Array[jl.Object]]()

    list.add(Array("1.6.3", FALSE))
    list.add(Array("2.0.0", FALSE))
    list.add(Array("2.0.3", FALSE))
    list.add(Array("2.1.0", FALSE))
    list.add(Array("2.1.3", FALSE))
    list.add(Array("2.2.0", TRUE))
    list.add(Array("2.2.1", TRUE))
    list.add(Array("2.2.3", TRUE))
    list.add(Array("2.3.0", TRUE))
    list.add(Array("2.3.1", TRUE))
    list.add(Array("2.3.3", TRUE))
    list.add(Array("2.5.0", TRUE))
    list.add(Array("2.5.1", TRUE))
    list.add(Array("3.0.0", TRUE))

    list.add(Array("2.1.3.extra", FALSE))
    list.add(Array("2.2.0.extra", TRUE))
    list.add(Array("2.3.1.extra", TRUE))
    list.add(Array("3.0.0.extra", TRUE))

    list.add(Array("2.1.3-extra", FALSE))
    list.add(Array("2.2.0-extra", TRUE))
    list.add(Array("2.3.1-extra", TRUE))
    list.add(Array("3.0.0-extra", TRUE))

    list
  }

}

@RunWith(classOf[Parameterized])
class StructuredStreamingVersionLockTest(version: String, expectsPass: Boolean) {

  @Test
  @throws[Exception]
  def checkCompatibility(): Unit = {
    val mockSession = Mockito.mock(classOf[SparkSession])
    Mockito.when(mockSession.version).thenReturn(version)

    var exception: Option[Exception] = None

    try {
      StructuredStreamingVersionLock.checkCompatibility(mockSession)
    } catch {
      case e: Exception => exception = Some(e)
    }

    (expectsPass, exception) match {
      case (true, Some(e)) => throw e
      case (false, None) => Assert.fail(s"Expected failure but didn't fail [$version]")
      case _ => // We good
    }
  }

}