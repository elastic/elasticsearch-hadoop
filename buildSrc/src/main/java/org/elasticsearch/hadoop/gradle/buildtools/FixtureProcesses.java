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

package org.elasticsearch.hadoop.gradle.buildtools;

import org.gradle.api.GradleException;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

/**
 * Utilities for managing long running background processes used as test fixtures.
 * <p>
 * Gradle's {@code ExecOperations} only supports running processes to completion, so background fixture
 * processes are spawned with a plain {@link ProcessBuilder}. Their output is redirected to a log file so
 * the process never blocks on a full pipe and the output is available when diagnosing failures.
 */
public final class FixtureProcesses {

    private FixtureProcesses() {}

    /**
     * Starts the given command in the background.
     *
     * @param command     the executable followed by its arguments
     * @param workingDir  the working directory of the process
     * @param environment additional environment variables, added on top of the inherited environment
     * @param logFile     file that receives both stdout and stderr of the process
     */
    public static Process start(List<String> command, File workingDir, Map<String, String> environment, File logFile) {
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.directory(workingDir);
        builder.environment().putAll(environment);
        builder.redirectErrorStream(true);
        builder.redirectOutput(ProcessBuilder.Redirect.appendTo(logFile));
        try {
            Process process = builder.start();
            // fixtures never read from stdin
            process.getOutputStream().close();
            return process;
        } catch (IOException e) {
            throw new GradleException("Failed to start process " + String.join(" ", command), e);
        }
    }

    /** Writes the pid of the given process to the given file. */
    public static void writePidFile(Process process, File pidFile) {
        try {
            Files.writeString(pidFile.toPath(), Long.toString(process.pid()), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write pid file " + pidFile, e);
        }
    }

    /** Reads a pid previously written to the given file. */
    public static long readPidFile(File pidFile) {
        try {
            return Long.parseLong(Files.readString(pidFile.toPath(), StandardCharsets.UTF_8).trim());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read pid file " + pidFile, e);
        }
    }

    /** Returns true if a process with the given pid is currently running. */
    public static boolean isAlive(long pid) {
        return ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false);
    }

    /** Returns true if a TCP connection to the given address can be established. */
    public static boolean isListening(InetSocketAddress address) {
        try (Socket socket = new Socket()) {
            socket.connect(address, 500);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Polls the given condition until it is met or the timeout elapses.
     *
     * @return true if the condition was met, false if timed out
     */
    public static boolean waitFor(Duration timeout, Duration checkInterval, BooleanSupplier condition) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            if (condition.getAsBoolean()) {
                return true;
            }
            if (System.nanoTime() >= deadline) {
                return false;
            }
            try {
                Thread.sleep(checkInterval.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    }

    /**
     * Forcibly terminates the process with the given pid and all of its descendants,
     * waiting up to the given timeout for them to exit.
     */
    public static void stop(long pid, Duration timeout) {
        Optional<ProcessHandle> handle = ProcessHandle.of(pid);
        if (handle.isEmpty()) {
            return;
        }
        // collect descendants before killing the parent, as they get re-parented afterwards
        List<ProcessHandle> processes = new ArrayList<>(handle.get().descendants().collect(Collectors.toList()));
        processes.add(handle.get());
        processes.forEach(ProcessHandle::destroyForcibly);
        for (ProcessHandle process : processes) {
            try {
                process.onExit().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new GradleException("Interrupted while waiting for process " + process.pid() + " to exit", e);
            } catch (ExecutionException | TimeoutException e) {
                throw new GradleException("Process " + process.pid() + " did not exit within " + timeout, e);
            }
        }
    }
}
