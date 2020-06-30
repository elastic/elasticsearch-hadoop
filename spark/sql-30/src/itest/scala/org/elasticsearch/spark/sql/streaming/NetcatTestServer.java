package org.elasticsearch.spark.sql.streaming;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.util.unit.TimeValue;

/**
 * Buffers up inputs into an internal queue, and sends them to the first client
 * that connects. Sends all inputs to the connected as they become available.
 */
public class NetcatTestServer {

    private static final Log LOG = LogFactory.getLog(NetcatTestServer.class);
    private static final Object KILL_TOKEN = new Object();

    private enum State {
        INIT, RUNNING, CLOSED
    }

    private final int port;

    private State currentState;

    private ServerSocket data;
    private Thread serverThread;
    private BlockingQueue<Object> outputQueue;
    private CountDownLatch closeWaiter = new CountDownLatch(1);

    public NetcatTestServer(int port) {
        this.port = port;
        this.currentState = State.INIT;
    }

    public void start() throws IOException {
        if (!currentState.equals(State.INIT)) {
            throw new EsHadoopIllegalStateException("Expected state INIT but was "+currentState);
        }

        // Initiate resources
        data = new ServerSocket(port);
        outputQueue = new ArrayBlockingQueue<>(1024);
        serverThread = new Thread(new ConnectionHandler(data, outputQueue, closeWaiter));
        serverThread.setDaemon(true);
        serverThread.start();

        currentState = State.RUNNING;
    }

    public void sendData(String data) {
        if (!currentState.equals(State.RUNNING)) {
            throw new EsHadoopIllegalStateException("Expected state RUNNING but was "+currentState);
        }
        outputQueue.add(data);
    }

    /**
     * Closes the streaming server by signaling the server thread to close with a kill token.
     * Will wait up to the specified timeout for the server to close. This is safe to call multiple
     * times, as it simply enqueues another kill token and waits, but there is no guarantee that this
     * will complete with a true output. In those cases, terminate the server immediatly with
     * "closeImmediately()".
     *
     * @return true if server is closed in provided timeout period, false if the server has not closed
     * before the timeout expires.
     * @throws InterruptedException If interrupted while waiting for server to close.
     * @throws IOException From closing internal server resources.
     */
    public boolean gracefulShutdown(TimeValue timeValue) throws InterruptedException, IOException {
        if (currentState == State.RUNNING) {
            outputQueue.add(KILL_TOKEN);
            boolean closed = closeWaiter.await(timeValue.millis(), TimeUnit.MILLISECONDS);
            if (closed) {
                data.close();
                currentState = State.CLOSED;
            }
            return closed;
        } else if (currentState == State.INIT) {
            currentState = State.CLOSED;
            return true;
        } else if (currentState == State.CLOSED) {
            return true;
        } else {
            throw new IllegalStateException("Illegal state of " + currentState + " encountered during shutdown.");
        }
    }

    /**
     * Attempts to close the server immediately. Does not attempt waiting for server to complete the
     * sending of data. Instead it closes the internal socket resources, and interrupts the handler,
     * and waits for the handler to exit.
     *
     * @throws InterruptedException If interrupted while waiting for server to close.
     * @throws IOException From closing internal server resources.
     */
    public void shutdownNow() throws IOException, InterruptedException {
        if (currentState == State.RUNNING) {
            // Close resources
            serverThread.interrupt();
            data.close();
            currentState = State.CLOSED;
        } else if (currentState == State.INIT) {
            currentState = State.CLOSED;
        }
    }

    // Worker thread for sending data to sockets in a netcat style.
    private static class ConnectionHandler implements Runnable {

        private final ServerSocket serverSocket;
        private final BlockingQueue<Object> datastream;
        private final CountDownLatch closeSignal;

        ConnectionHandler(ServerSocket serverSocket, BlockingQueue<Object> datastream, CountDownLatch closeSignal) {
            this.serverSocket = serverSocket;
            this.datastream = datastream;
            this.closeSignal = closeSignal;
        }

        @Override
        public void run() {
            try {
                boolean interrupted = Thread.currentThread().isInterrupted();
                while (!interrupted) {
                    try (Socket socket = serverSocket.accept()) {
                        handleConnection(socket);
                    } catch (InterruptedException ie) {
                        LOG.info("Server Closing... Reason: " + ie.getMessage());
                        interrupted = true;
                    } catch (IOException ioe) {
                        // Only reachable if thrown from accept call.
                        LOG.info("Server closed. Closing.", ioe);
                        interrupted = true;
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("Encountered error on test stream server.", e);
            } finally {
                closeSignal.countDown();
            }
        }

        // Given a socket, taps its output stream and sends data continuously.
        // Returns on socket error/close. Throws IE on interrupt.
        private void handleConnection(Socket socket) throws InterruptedException {
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("Thread interrupted before handle call");
            }

            try {
                PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                LOG.debug("Connected. Sending data as it arrives.");
                sendContinuously(writer);
            } catch (IOException ioe) {
                LOG.debug("Socket closed. Waiting for new connection.", ioe);
            }
        }

        // Constantly invokes the single poll and send method. Throws IOE on close, IE on interrupt
        private void sendContinuously(PrintWriter writer) throws InterruptedException, IOException {
            boolean interrupted = Thread.currentThread().isInterrupted();
            while (!interrupted) {
                sendData(writer);
            }
        }

        // Polls once for data to send to connection. Throws IOE on socket close, and IE on interrupt.
        private void sendData(PrintWriter writer) throws InterruptedException, IOException {
            Object message = datastream.poll(5, TimeUnit.SECONDS);
            if (message != null) {
                if (message instanceof String) {
                    String data = (String) message;
                    writer.println(data);
                    if (writer.checkError()) {
                        throw new IOException("Socket closed!");
                    }
                } else if (message == KILL_TOKEN) {
                    throw new InterruptedException("Kill Token received. Triggering shutdown.");
                } else {
                    throw new IllegalArgumentException("Illegal data type found in server stream: " + message);
                }
            }
        }
    }
}
