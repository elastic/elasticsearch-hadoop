/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.spark.sql.streaming

import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.StreamingQueryException
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.elasticsearch.hadoop.util.TestUtils
import org.elasticsearch.hadoop.util.unit.TimeValue
import org.junit.Assert

/**
 * Manages the creation and graceful teardown of a Spark Streaming Query
 * for purposes of integration testing.
 *
 * Concrete subclasses are version-specific: they override [[newListener]] to return a
 * [[LifecycleListener]] that adds Spark-version-specific callbacks (e.g. onQueryIdle for
 * Spark 3.5+).
 */
abstract class StreamingQueryTestHarnessBase[S <: java.io.Serializable : Encoder](val sparkSession: SparkSession) extends Serializable {

  import sparkSession.implicits._

  private[this] sealed trait State extends Serializable
  private[this] object Init extends State
  private[this] object Starting extends State
  private[this] object Running extends State
  private[this] object Closed extends State

  /**
   * Base listener that handles the common Spark 3.x streaming lifecycle.
   * Subclasses (created via [[newListener]]) may override additional callbacks
   * such as onQueryIdle (Spark 3.5+).
   */
  protected class LifecycleListener extends StreamingQueryListener {

    private var uuid: Option[UUID] = None

    protected var inputsRequired = 0L
    protected var inputsSeen = 0L

    private var expectingToThrow: Option[Class[_]] = None
    private var foundExpectedException: Boolean = false
    private var encounteredException: Option[String] = None

    protected var latch = new CountDownLatch(1)

    def incrementExpected(): Unit = inputsRequired = inputsRequired + 1

    def setExpectedException(clazz: Class[_]): Unit = {
      expectingToThrow match {
        case Some(cls) => throw new IllegalArgumentException(s"Already expecting exception of type [$cls]!")
        case None => expectingToThrow = Some(clazz)
      }
    }

    protected def captureQueryID(eventId: UUID): Unit = {
      uuid match {
        case Some(id) if eventId != id => throw new IllegalStateException("Multiple queries are not supported")
        case None => uuid = Some(eventId)
        case _ =>
      }
    }

    override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
      captureQueryID(event.id)
    }

    override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
      captureQueryID(event.progress.id)

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

    override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
      try {
        captureQueryID(event.id)

        encounteredException = event.exception match {
          case Some(value) =>
            val messageParts = value.split("\\): ")
            if (messageParts.size > 1) {
              val nonSparkMessage = messageParts(1)
              val removedNewLine = nonSparkMessage.substring(0, nonSparkMessage.indexOf("\n"))
              Some(removedNewLine.substring(0, removedNewLine.indexOf(":")))
            } else {
              Some(value.substring(0, value.indexOf(":")))
            }
          case None => None
        }

        val expectedExceptionName = expectingToThrow.map(_.getCanonicalName).getOrElse("None")
        foundExpectedException = encounteredException.exists(_.equals(expectedExceptionName))
      } finally {
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

  /** Override to return a listener with additional version-specific callbacks (e.g. onQueryIdle). */
  protected def newListener(): LifecycleListener = new LifecycleListener

  @transient private val listener = newListener()

  @transient private lazy val port = 9999

  @transient private var currentState: State = Init
  @transient private var currentQuery: Option[StreamingQuery] = None
  @transient private var exceptionMessage: Option[String] = None

  @transient private lazy val testingServer: NetcatTestServer = {
    val server = new NetcatTestServer(port)
    server.start()
    server
  }

  @transient private var testTimeout = new TimeValue(1, TimeUnit.MINUTES)

  def stream: Dataset[S] = {
    ensureState(Init, Starting) {
      sparkSession.readStream
                  .format("socket")
                  .option("host", "localhost")
                  .option("port", port)
                  .load()
                  .as[String]
                  .map(TestingSerde.deserialize[S])
    }
  }

  def withInput(data: S): StreamingQueryTestHarnessBase[S] = {
    ensureState(Init, Running) {
      testingServer.sendData(TestingSerde.serialize(data))
      listener.incrementExpected()
    }
    this
  }

  def setTestTimeout(timeout: Long, unit: TimeUnit): StreamingQueryTestHarnessBase[S] = {
    ensureState(Init) {
      testTimeout = new TimeValue(timeout, unit)
    }
    this
  }

  def expectingToThrow[T <: Throwable](clazz: Class[T]): StreamingQueryTestHarnessBase[S] = {
    ensureState(Init) {
      listener.setExpectedException(clazz)
      exceptionMessage = None
    }
    this
  }

  def expectingToThrow[T <: Throwable](clazz: Class[T], message: String): StreamingQueryTestHarnessBase[S] = {
    ensureState(Init) {
      listener.setExpectedException(clazz)
      exceptionMessage = Some(message)
    }
    this
  }

  def startTest(query: => StreamingQuery): StreamingQuery = {
    ensureState(Init) {
      currentState = Starting
      try {
        sparkSession.streams.addListener(listener)
        val q = query
        currentQuery = Some(q)
        currentState = Running
        q
      } catch {
        case t: Throwable =>
          scrubState()
          throw t
      }
    }
  }

  def waitForCompletion(): Unit = {
    ensureState(Running, Closed) {
      currentState match {
        case Running =>
          try {
            if(!testingServer.gracefulShutdown(testTimeout)) {
              throw new TimeoutException("Timed out on waiting for socket server to shutdown.")
            }
            if(!listener.waitOnComplete(testTimeout)) {
              throw new TimeoutException("Timed out on waiting for stream to complete.")
            }
          } catch {
            case e: Throwable =>
              scrubState()
              throw e
          }

          val queryEnded = currentQuery.exists((query: StreamingQuery) => {
            query.stop()
            try {
              query.awaitTermination(testTimeout.millis)
            } catch {
              case _: StreamingQueryException => true
            }
          })

          if (!queryEnded) {
            scrubState()
            throw new TimeoutException("Timed out on waiting for stream to terminate")
          }

          sparkSession.streams.removeListener(listener)
          listener.assertExpectedExceptions(exceptionMessage)
          currentQuery = None
          currentState = Closed
        case _ =>
          sparkSession.streams.removeListener(listener)
          testingServer.shutdownNow()
          currentQuery.foreach(_.stop())
          currentState = Closed
      }
    }
  }

  def waitForPartialCompletion(): Unit = {
    ensureState(Running) {
      currentState match {
        case Running =>
          try {
            if (!listener.waitOnComplete(testTimeout)) {
              throw new TimeoutException("Timed out on waiting for stream to complete.")
            }
            listener.expectAnotherBatch()
          } catch {
            case e: Throwable =>
              scrubState()
              throw e
          }
      }
    }
  }

  private[this] def scrubState(): Unit = {
    sparkSession.streams.removeListener(listener)
    testingServer.shutdownNow()
    currentQuery.foreach(_.stop())
    currentQuery = None
    currentState = Closed
  }

  def runTest(query: => StreamingQuery): Unit = {
    startTest {
      query
    }
    waitForCompletion()
  }

  private def ensureState[T](states: State*)(block: => T): T = {
    currentState match {
      case state: State if states.contains(state) => block
      case Init => throw new IllegalStateException("Test harness hasn't been started yet.")
      case Starting => throw new IllegalStateException("Test harness is starting, and cannot be configured any longer.")
      case Running => throw new IllegalStateException("Test harness already started")
      case Closed => throw new IllegalStateException("Test harness is closed")
    }
  }
}

object TestingSerde extends Serializable {
  def serialize(any: java.io.Serializable): String = {
    TestUtils.serializeToBase64(any)
  }

  def deserialize[T](line: String): T = {
    val data: T = TestUtils.deserializeFromBase64(line)
    data
  }
}
