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

import org.elasticsearch.hadoop.gradle.util.WaitForURL;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.provider.Property;
import org.gradle.api.specs.Spec;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.TaskProvider;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Starts a fixture process in the background and waits for it to become ready.
 * <p>
 * The process is expected to write a {@code pid} and a {@code ports} file into {@link #getBaseDir()} once it
 * is up. Its stdout and stderr are written to {@code run.log} in {@link #getCwd()}. A companion
 * {@code <name>#stop} task is registered which terminates the process; it finalizes this task and should also
 * finalize every task using the fixture.
 */
public abstract class ProcessFixture extends DefaultTask implements Fixture {

    private final List<Object> arguments = new ArrayList<>();
    private final Map<String, Object> environment = new LinkedHashMap<>();
    private final TaskProvider<StopFixtureProcess> stopTask;
    private Spec<? super ProcessFixture> waitCondition = ProcessFixture::httpWaitCondition;

    @Inject
    protected abstract FileSystemOperations getFileSystemOperations();

    @Inject
    protected abstract ProjectLayout getProjectLayout();

    public ProcessFixture() {
        getMaxWaitInSeconds().convention(30);
        stopTask = getProject().getTasks().register(getName() + "#stop", StopFixtureProcess.class, stop -> {
            stop.getPidFile().set(getProjectLayout().getBuildDirectory().file("fixtures/" + getName() + "/pid"));
        });
        finalizedBy(stopTask);
    }

    /** The executable that starts the fixture. */
    @Internal
    public abstract Property<String> getExecutable();

    /** How long to wait for the pid and ports file to show up. */
    @Internal
    public abstract Property<Integer> getMaxWaitInSeconds();

    /** Adds arguments. Values are converted with {@code toString()} at execution time. */
    public void args(Object... args) {
        arguments.addAll(List.of(args));
    }

    /** Adds an environment variable. The value is converted with {@code toString()} at execution time. */
    public void env(String key, Object value) {
        environment.put(key, value);
    }

    /**
     * Condition evaluated once the process has written its pid and ports file. Defaults to waiting for
     * an HTTP response on the first address in the ports file.
     */
    @Internal
    public Spec<? super ProcessFixture> getWaitCondition() {
        return waitCondition;
    }

    public void setWaitCondition(Spec<? super ProcessFixture> waitCondition) {
        this.waitCondition = waitCondition;
    }

    @Override
    @Internal
    public TaskProvider<StopFixtureProcess> getStopTask() {
        return stopTask;
    }

    @TaskAction
    public void startFixture() {
        getFileSystemOperations().delete(spec -> spec.delete(getBaseDir()));
        getCwd().mkdirs();

        List<String> command = new ArrayList<>();
        command.add(getExecutable().get());
        arguments.forEach(arg -> command.add(arg.toString()));
        Map<String, String> env = new LinkedHashMap<>();
        environment.forEach((key, value) -> env.put(key, value.toString()));

        getCommandString(command, env).lines().forEach(getLogger()::info);

        Process process = FixtureProcesses.start(command, getCwd(), env, getRunLog());
        boolean started = FixtureProcesses.waitFor(
            Duration.ofSeconds(getMaxWaitInSeconds().get()),
            Duration.ofMillis(500),
            () -> process.isAlive() == false || (getPidFile().exists() && getPortsFile().exists())
        );
        if (process.isAlive() == false) {
            fail(command, env, "Failed to start " + getName() + ": process exited with code " + process.exitValue());
        }
        if (started == false) {
            fail(command, env, "Failed to start " + getName() + ": timed out after " + getMaxWaitInSeconds().get() + " seconds");
        }

        boolean success = false;
        try {
            success = waitCondition.isSatisfiedBy(this);
        } catch (Exception e) {
            fail(command, env, "Wait condition caught exception for " + getName(), e);
        }
        if (success == false) {
            fail(command, env, "Wait condition failed for " + getName());
        }
    }

    /** Default wait condition: an HTTP GET against the first address in the ports file succeeds. */
    private static boolean httpWaitCondition(ProcessFixture fixture) {
        return new WaitForURL().setUrl("http://" + fixture.getAddressAndPort())
            .setTotalWaitTime(30, TimeUnit.SECONDS)
            .setCheckEvery(500, TimeUnit.MILLISECONDS)
            .checkAvailability(fixture.getLogger());
    }

    private String getCommandString(List<String> command, Map<String, String> env) {
        StringBuilder sb = new StringBuilder();
        sb.append('\n').append(getName()).append(" configuration:\n");
        sb.append("-----------------------------------------\n");
        sb.append("  cwd: ").append(getCwd()).append('\n');
        sb.append("  command: ").append(String.join(" ", command)).append('\n');
        sb.append("  environment:\n");
        env.forEach((k, v) -> sb.append("    ").append(k).append(": ").append(v).append('\n'));
        return sb.toString();
    }

    /** Fail the build with the given message, logging relevant info. */
    private void fail(List<String> command, Map<String, String> env, String msg, Exception... suppressed) {
        if (getLogger().isInfoEnabled() == false) {
            // We already log the command at info level. No need to do it twice.
            getCommandString(command, env).lines().forEach(getLogger()::error);
        }
        getLogger().error("{} output:", getName());
        getLogger().error("-----------------------------------------");
        getLogger().error("  pid file exists: {}", getPidFile().exists());
        getLogger().error("  ports file exists: {}", getPortsFile().exists());
        if (getRunLog().exists()) {
            getLogger().error("\n  [log]");
            try {
                Files.readAllLines(getRunLog().toPath(), StandardCharsets.UTF_8).forEach(line -> getLogger().error("    {}", line));
            } catch (IOException e) {
                getLogger().error("    <failed to read log: {}>", e.getMessage());
            }
        }
        getLogger().error("-----------------------------------------");
        GradleException toThrow = new GradleException(msg);
        for (Exception e : suppressed) {
            toThrow.addSuppressed(e);
        }
        throw toThrow;
    }

    /** The directory all configuration and runtime files of this fixture live in. */
    @Internal
    public File getBaseDir() {
        return getProjectLayout().getBuildDirectory().dir("fixtures/" + getName()).get().getAsFile();
    }

    /** The working directory of the process. */
    @Internal
    public File getCwd() {
        return new File(getBaseDir(), "cwd");
    }

    /** The file the process writes its pid to. */
    @Internal
    public File getPidFile() {
        return new File(getBaseDir(), "pid");
    }

    /** The file the process writes its bound addresses to. */
    @Internal
    public File getPortsFile() {
        return new File(getBaseDir(), "ports");
    }

    /** The log file that receives the output of the process. */
    @Internal
    public File getRunLog() {
        return new File(getCwd(), "run.log");
    }

    /** Returns an address and port suitable for a uri to connect to this fixture over http. */
    @Internal
    public String getAddressAndPort() {
        try {
            return Files.readAllLines(getPortsFile().toPath(), StandardCharsets.UTF_8).get(0);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
