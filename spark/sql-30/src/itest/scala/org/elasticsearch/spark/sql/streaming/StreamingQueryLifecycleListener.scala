package org.elasticsearch.spark.sql.streaming

import org.apache.spark.sql.streaming.StreamingQueryListener
import org.elasticsearch.hadoop.util.unit.TimeValue
import org.junit.Assert

import java.util.UUID
import java.util.concurrent.{CountDownLatch, TimeUnit}

// Listener to 1) ensure no more than a single stream is running at a time, 2) know when we're done processing inputs
// and 3) to capture any Exceptions encountered during the execution of the stream.
class StreamingQueryLifecycleListener extends StreamingQueryListener {

  private var uuid: Option[UUID] = None

  private var inputsRequired = 0L
  private var inputsSeen = 0L

  private var expectingToThrow: Option[Class[_]] = None
  private var foundExpectedException: Boolean = false
  private var encounteredException: Option[String] = None

  private var latch = new CountDownLatch(1) // expects just a single batch

  def incrementExpected(): Unit = inputsRequired = inputsRequired + 1

  def setExpectedException(clazz: Class[_]): Unit = {
    expectingToThrow match {
      case Some(cls) => throw new IllegalArgumentException(s"Already expecting exception of type [$cls]!")
      case None => expectingToThrow = Some(clazz)
    }
  }

  // Make sure we only ever watch one query at a time.
  private def captureQueryID(eventId: UUID): Unit = {
    uuid match {
      case Some(id) if eventId != id => throw new IllegalStateException("Multiple queries are not supported")
      case None => uuid = Some(eventId)
      case _ => // No problem for now
    }
  }

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    captureQueryID(event.id)
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    captureQueryID(event.progress.id)

    // keep track of the number of input rows seen. When we reach the number of expected inputs,
    // wait for two 0 values to pass before signaling to close

    val rows = event.progress.numInputRows
    inputsSeen = inputsSeen + rows

    if (inputsSeen == inputsRequired) {
      if (rows == 0) {
        // Don't close after meeting the first input level. Wait to make sure we get
        // one last pass with no new rows processed before signalling.
        latch.countDown()
      }
    } else if (inputsSeen > inputsRequired) {
      throw new IllegalStateException("Too many inputs encountered. Expected [" + inputsRequired +
        "] but found [" + inputsSeen + "]")
    }
  }

  protected def onQueryIdle(eventId: UUID): Unit = {
    captureQueryID(eventId)
    if (inputsSeen == inputsRequired) {
      latch.countDown()
    }
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    try {
      captureQueryID(event.id)

      encounteredException = event.exception match {
        case Some(value) =>
          // This is a whole trace, get everything after the enclosing SparkException (i.e. the original exception + trace)
          val messageParts = value.split("\\): ")
          if (messageParts.size > 1) {
            val nonSparkMessage = messageParts(1)
            // Ditch the full trace after the first newline
            val removedNewLine = nonSparkMessage.substring(0, nonSparkMessage.indexOf("\n"))
            // Split the class name from the exception message and take the class name
            Some(removedNewLine.substring(0, removedNewLine.indexOf(":")))
          } else {
            // Return the original framework error
            Some(value.substring(0, value.indexOf(":")))
          }
        case None => None
      }

      val expectedExceptionName = expectingToThrow.map(_.getCanonicalName).getOrElse("None")

      foundExpectedException = encounteredException.exists(_.equals(expectedExceptionName))
    } finally {
      // signal no matter what to avoid deadlock
      latch.countDown()
    }
  }

  def waitOnComplete(timeValue: TimeValue): Boolean = latch.await(timeValue.millis, TimeUnit.MILLISECONDS)

  def expectAnotherBatch(): Unit = {
    latch = new CountDownLatch(1)
  }

  def assertExpectedExceptions(message: Option[String]): Unit = {
    expectingToThrow match {
      case Some(exceptionClass) =>
        if (!foundExpectedException) {
          encounteredException match {
            case Some(s) => Assert.fail(s"Expected ${exceptionClass.getCanonicalName} but got $s")
            case None => Assert.fail(message.getOrElse(s"Expected ${exceptionClass.getCanonicalName} but no Exceptions were thrown"))
          }
        }
      case None =>
        encounteredException match {
          case Some(exception) => Assert.fail(s"Expected no exceptions but got $exception")
          case None => ()
        }
    }
  }
}