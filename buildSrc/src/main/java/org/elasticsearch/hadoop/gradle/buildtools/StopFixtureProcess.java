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

import org.gradle.api.DefaultTask;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;

/**
 * Stops a background fixture process (and its child processes) identified by a pid file.
 * The task is skipped if the pid file does not exist.
 */
public abstract class StopFixtureProcess extends DefaultTask {

    /** File containing the pid of the process to stop. Deleted once the process is stopped. */
    @Internal
    public abstract RegularFileProperty getPidFile();

    public StopFixtureProcess() {
        onlyIf("pid file exists", task -> getPidFile().get().getAsFile().exists());
    }

    @TaskAction
    public void stopProcess() throws IOException {
        File pidFile = getPidFile().get().getAsFile();
        long pid = FixtureProcesses.readPidFile(pidFile);
        getLogger().info("Shutting down process with pid {}", pid);
        FixtureProcesses.stop(pid, Duration.ofSeconds(30));
        Files.deleteIfExists(pidFile.toPath());
    }
}
