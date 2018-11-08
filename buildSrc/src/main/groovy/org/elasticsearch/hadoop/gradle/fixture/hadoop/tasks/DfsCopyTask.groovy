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

package org.elasticsearch.hadoop.gradle.fixture.hadoop.tasks

import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.InstanceConfiguration
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.file.CopySpec
import org.gradle.api.tasks.TaskAction
import org.gradle.process.ExecSpec

import java.nio.file.Path

class DfsCopyTask extends DefaultTask {

    InstanceConfiguration hadoopGateway
    Path dfsSource
    Path localSource
    Path dfsDestination
    Path localDestination
    Map<String, String> env = [:]
    Map<String, String> systemProperties = [:]

    Path getDfsSource() {
        return dfsSource
    }

    void setDfsSource(Path dfsSource) {
        if (localSource != null) {
            throw new GradleException("Cannot set dfs source if local source is set")
        }
        this.dfsSource = dfsSource
    }

    void dfsSource(Path path) {
        setDfsSource(path)
    }

    Path getLocalSource() {
        return localSource
    }

    void setLocalSource(Path localSource) {
        if (dfsSource != null) {
            throw new GradleException("Cannot set local source if dfs source is set")
        }
        this.localSource = localSource
    }

    void localSource(Path path) {
        setLocalSource(path)
    }

    Path getDfsDestination() {
        return dfsDestination
    }

    void setDfsDestination(Path dfsDestination) {
        if (localDestination != null) {
            throw new GradleException("Cannot set dfs destination if local destination is set")
        }
        this.dfsDestination = dfsDestination
    }

    void dfsDestination(Path path) {
        setDfsDestination(path)
    }

    Path getLocalDestination() {
        return localDestination
    }

    void setLocalDestination(Path localDestination) {
        if (dfsDestination != null) {
            throw new GradleException("Cannot set local destination if dfs destination is set")
        }
        this.localDestination = localDestination
    }

    void localDestination(Path path) {
        setLocalDestination(path)
    }

    Map<String, String> getEnv() {
        return env
    }

    void setEnv(Map<String, String> env) {
        this.env = env
    }

    void env(String key, String val) {
        env.put(key, val)
    }

    void env(Map<String, String> values) {
        env.putAll(values)
    }

    Map<String, String> getSystemProperties() {
        return systemProperties
    }

    void setSystemProperties(Map<String, String> systemProperties) {
        this.systemProperties = systemProperties
    }

    void systemProperty(String key, String value) {
        systemProperties.put(key, value)
    }

    void systemProperties(Map<String, String> properties) {
        systemProperties.putAll(properties)
    }

    @TaskAction
    void doCopy() {
        // Verification
        if (localSource != null || dfsSource != null) {
            throw new GradleException("No source given")
        }
        if (localDestination != null || dfsDestination != null) {
            throw new GradleException("No destination given")
        }

        // Determine command
        File binDir = new File('')
        String commandName = 'hdfs' // or hdfs.cmd
        File command = new File(binDir, commandName)
        List<String> commandLine = [command.toString()]
        if (localSource != null && localDestination != null) {
            project.copy { CopySpec spec ->
                spec.from(localSource.toString())
                spec.into(localDestination.toString())
            }
            return // Exit early
        } else if (localSource != null && dfsDestination != null) {
            commandLine.addAll(['dfs', '-copyFromLocal', localSource.toString(), dfsDestination.toString()])
        } else if (dfsSource != null && localDestination != null) {
            commandLine.addAll(['dfs', '-copyToLocal', dfsSource.toString(), localDestination.toString()])
        } else if (dfsSource != null && dfsDestination != null) {
            commandLine.addAll(['dfs', '-cp', dfsSource.toString(), dfsDestination.toString()])
        }

        // Combine env and sysprops
        Map<String, String> finalEnv = hadoopGateway.getEnvironmentVariables()
        hadoopGateway.getServiceDescriptor().finalizeEnv(finalEnv, hadoopGateway)
        finalEnv.putAll(env)

//        String finalJvmArgs = hadoopGateway.getJvmArgs()
//        finalJvmArgs = finalJvmArgs + ' ' + systemProperties.collect {k, v -> "-D${k}=${v}"}.join(' ')
//        finalEnv.put()

        // Do command
        project.exec { ExecSpec spec ->
            spec.commandLine(commandLine)
            spec.environment(finalEnv)
        }
    }
}
