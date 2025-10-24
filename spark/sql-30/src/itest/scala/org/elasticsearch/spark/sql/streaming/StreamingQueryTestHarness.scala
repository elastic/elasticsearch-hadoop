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

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryException, StreamingQueryListener}
import org.elasticsearch.hadoop.util.TestUtils
import org.elasticsearch.hadoop.util.unit.TimeValue

import java.lang.reflect.Method
import java.util.concurrent.{TimeUnit, TimeoutException}

/**
 * Manages the creation and graceful teardown of a Spark Streaming Query
 * for purposes of integration testing.
 *
 * Duties:
 * 1) Select a viable port to use for socket transmission of data
 * 2) Set up the local netcat server
 * 3) Configure a streaming query to run over that server data using the socket source for testing
 * 4) Accept testing inputs from the test case, and serialize them to base64 for transmission as strings
 * 5) Keep track of the total inputs expected to be seen in the sink
 * 6) Register a streaming listener that signals when an exception is caught, or when all inputs are processed
 * 7) When asked, tear down the streaming query:
 *    - Gracefully stop the netcat server
 *    - Wait for the signal from the streaming listener that the stream is done processing
 *    - Unregister the listener and check for exceptions
 */
class StreamingQueryTestHarness[S <: java.io.Serializable : Encoder](val sparkSession: SparkSession) extends Serializable {
  // Use the more restrictive java.io.Serializable type bounding for java interoperability.

  // Import the implicits here to let us transform the Dataset objects
  import sparkSession.implicits._

  // For tracking internal state
  private[this] sealed trait State extends Serializable
  private[this] object Init extends State
  private[this] object Starting extends State
  private[this] object Running extends State
  private[this] object Closed extends State


  @transient private val listener: StreamingQueryLifecycleListener = {
    val supportsIdleEvent = classOf[StreamingQueryListener].getMethods.exists((method: Method) => method.getName.equals("onQueryIdle"))
    if (supportsIdleEvent) {
      Class.forName("org.elasticsearch.spark.sql.streaming.StreamingQueryIdleLifecycleListener").getConstructor().newInstance().asInstanceOf[StreamingQueryLifecycleListener]
    } else {
      new StreamingQueryLifecycleListener()
    }
  }

  // Port number to use for the socket source
  @transient private lazy val port = {
    // Todo: Generate this port?
    9999
  }

  @transient private var currentState: State = Init
  @transient private var currentQuery: Option[StreamingQuery] = None
  @transient private var exceptionMessage: Option[String] = None

  // Server for test data to the socket source
  @transient private lazy val testingServer: NetcatTestServer = {
    val server = new NetcatTestServer(port)
    server.start()
    server
  }

  @transient private var testTimeout = new TimeValue(1, TimeUnit.MINUTES)

  /**
   * Convenience method for getting the reading stream, sourced from this lifecycle's testing data
   */
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

  /**
   * Add input to test server. Updates listener's bookkeeping to know when it's safe to shut down the stream
   */
  def withInput(data: S): StreamingQueryTestHarness[S] = {
    ensureState(Init, Running) {
      testingServer.sendData(TestingSerde.serialize(data))
      listener.incrementExpected()
    }
    this
  }

  def setTestTimeout(timeout: Long, unit: TimeUnit): StreamingQueryTestHarness[S] = {
    ensureState(Init) {
      testTimeout = new TimeValue(timeout, unit)
    }
    this
  }

  def expectingToThrow[T <: Throwable](clazz: Class[T]): StreamingQueryTestHarness[S] = {
    ensureState(Init) {
      listener.setExpectedException(clazz)
      exceptionMessage = None
    }
    this
  }

  def expectingToThrow[T <: Throwable](clazz: Class[T], message: String): StreamingQueryTestHarness[S] = {
    ensureState(Init) {
      listener.setExpectedException(clazz)
      exceptionMessage = Some(message)
    }
    this
  }

  /**
   * Initiate the test by passing in the running query
   */
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
          // Cleanup in case of exception
          scrubState()
          throw t
      }
    }
  }

  /**
   * Waits until all inputs are processed on the streaming query and then stops it.
   */
  def waitForCompletion(): Unit = {
    ensureState(Running, Closed) {
      currentState match {
        case Running =>
          try {
            // Wait for testing server to close down
            if(!testingServer.gracefulShutdown(testTimeout)) {
              throw new TimeoutException("Timed out on waiting for socket server to shutdown.")
            }

            // Wait for query to complete consuming records
            if(!listener.waitOnComplete(testTimeout)) {
              throw new TimeoutException("Timed out on waiting for stream to complete.")
            }
          } catch {
            case e: Throwable =>
              // Best effort to shutdown queries before throwing
              scrubState()
              throw e
          }

          // Wait for the queries to complete their teardown
          val queryEnded = currentQuery.exists((query: StreamingQuery) => {
            query.stop()
            try {
              query.awaitTermination(testTimeout.millis)
            } catch {
              // May not be totally fatal. Lets see what the expected exceptions were.
              case _: StreamingQueryException => true
            }
          })

          if (!queryEnded) {
            scrubState()
            throw new TimeoutException("Timed out on waiting for stream to terminate")
          }

          // Cleanup the listener first to ensure thread visibility
          sparkSession.streams.removeListener(listener)

          // Check for any error cases
          listener.assertExpectedExceptions(exceptionMessage)

          // Clear state
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

  /**
   * Waits until all inputs are processed on the streaming query, but leaves the query open with the listener still in place, expecting
   * another batch of inputs.
   */
  def waitForPartialCompletion(): Unit = {
    ensureState(Running) {
      currentState match {
        case Running =>
          try {
            // Wait for query to complete consuming records
            if (!listener.waitOnComplete(testTimeout)) {
              throw new TimeoutException("Timed out on waiting for stream to complete.")
            }
            listener.expectAnotherBatch()
          } catch {
            case e: Throwable =>
              // Best effort to shutdown queries before throwing
              scrubState()
              throw e
          }
      }
    }
  }

  // tears down literally everything indiscriminately, mostly for cleanup after a failure
  private[this] def scrubState(): Unit = {
    sparkSession.streams.removeListener(listener)
    testingServer.shutdownNow()
    currentQuery.foreach(_.stop())
    currentQuery = None
    currentState = Closed
  }

  /**
   * Runs the test and waits for completion. Identical to calling
   * startTest with the block and then immediately following with
   * waitForCompletion
   */
  def runTest(query: => StreamingQuery): Unit = {
    try {
      startTest {
        query
      }
      waitForCompletion()
    } finally {

    }
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