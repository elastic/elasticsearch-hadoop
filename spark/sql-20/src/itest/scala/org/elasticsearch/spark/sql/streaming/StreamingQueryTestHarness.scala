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
import org.elasticsearch.hadoop.util.IOUtils
import org.elasticsearch.hadoop.util.unit.TimeValue
import org.junit.Assert

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

  // Listener to 1) ensure no more than a single stream is running at a time, 2) know when we're done processing inputs
  // and 3) to capture any Exceptions encountered during the execution of the stream.
  private [this] class StreamingQueryLifecycleListener extends StreamingQueryListener {

    private var uuid: Option[UUID] = None

    private var inputsRequired = 0L
    private var inputsSeen = 0L

    private var expectingToThrow: Option[Class[_]] = None
    private var foundExpectedException: Boolean = false
    private var encounteredException: Option[String] = None

    private val latch = new CountDownLatch(1)

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

  @transient private val listener = new StreamingQueryLifecycleListener

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
    ensureState(Init) {
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
    IOUtils.serializeToBase64(any)
  }

  def deserialize[T](line: String): T = {
    val data: T = IOUtils.deserializeFromBase64(line)
    data
  }
}