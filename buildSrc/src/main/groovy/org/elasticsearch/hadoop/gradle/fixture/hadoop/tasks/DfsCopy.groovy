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

import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.HadoopClusterConfiguration
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.InstanceConfiguration
import org.elasticsearch.hadoop.gradle.fixture.hadoop.services.HadoopServiceDescriptor
import org.gradle.api.GradleException
import org.gradle.api.file.CopySpec
import org.gradle.api.tasks.TaskAction
import org.gradle.process.ExecSpec

class DfsCopy extends AbstractClusterTask {

    File dfsSource
    File localSource
    File dfsDestination
    File localDestination

    File getDfsSource() {
        return dfsSource
    }

    void setDfsSource(File dfsSource) {
        if (localSource != null) {
            throw new GradleException("Cannot set dfs source if local source is set")
        }
        this.dfsSource = dfsSource
    }

    void from(File path) {
        setDfsSource(path)
    }

    File getLocalSource() {
        return localSource
    }

    void setLocalSource(File localSource) {
        if (dfsSource != null) {
            throw new GradleException("Cannot set local source if dfs source is set")
        }
        this.localSource = localSource
    }

    void fromLocal(File path) {
        setLocalSource(path)
    }

    File getDfsDestination() {
        return dfsDestination
    }

    void setDfsDestination(File dfsDestination) {
        if (localDestination != null) {
            throw new GradleException("Cannot set dfs destination if local destination is set")
        }
        this.dfsDestination = dfsDestination
    }

    void to(File path) {
        setDfsDestination(path)
    }

    File getLocalDestination() {
        return localDestination
    }

    void setLocalDestination(File localDestination) {
        if (dfsDestination != null) {
            throw new GradleException("Cannot set local destination if dfs destination is set")
        }
        this.localDestination = localDestination
    }

    void toLocal(File path) {
        setLocalDestination(path)
    }

    @Override
    InstanceConfiguration defaultInstance(HadoopClusterConfiguration clusterConfiguration) {
        return clusterConfiguration
                .service(HadoopClusterConfiguration.HADOOP)
                .role(HadoopServiceDescriptor.GATEWAY)
                .instance(0)
    }

    @Override
    Map<String, String> taskEnvironmentVariables() {
        return [:]
    }

    @TaskAction
    void runHdfsDfsCp() {
        // Verification
        if (clusterConfiguration == null) {
            throw new GradleException("No cluster configuration found")
        }
        if (localSource == null && dfsSource == null) {
            throw new GradleException("No source given")
        }
        if (localDestination == null && dfsDestination == null) {
            throw new GradleException("No destination given")
        }

        // Gateway conf
        InstanceConfiguration hadoopGateway = getInstance()

        // Determine command
        File baseDir = hadoopGateway.getBaseDir()
        File homeDir = new File(baseDir, hadoopGateway.getServiceDescriptor().homeDirName(hadoopGateway))
        File binDir = new File(homeDir, hadoopGateway.serviceDescriptor.scriptDir(hadoopGateway))
        String commandName = 'hdfs' // TODO: or hdfs.cmd for Windows
        File command = new File(binDir, commandName)

        List<String> copyCommandLine = [command.toString()]
        if (localSource != null && localDestination != null) {
            project.copy { CopySpec spec ->
                spec.from(localSource.toString())
                spec.into(localDestination.toString())
            }
            return // Exit early
        } else if (localSource != null && dfsDestination != null) {
            copyCommandLine.addAll(['dfs', '-copyFromLocal', localSource.toString(), dfsDestination.toString()])
        } else if (dfsSource != null && localDestination != null) {
            copyCommandLine.addAll(['dfs', '-copyToLocal', dfsSource.toString(), localDestination.toString()])
        } else if (dfsSource != null && dfsDestination != null) {
            copyCommandLine.addAll(['dfs', '-cp', dfsSource.toString(), dfsDestination.toString()])
        }

        // Combine env and sysprops
        Map<String, String> finalEnv = collectEnvVars()

        // First ensure destination directories exist
        if (dfsDestination != null) {
            List<String> mkdirCommandLine = [command.toString(), 'dfs', '-mkdir', '-p', dfsDestination.parentFile.toString()]
            project.exec { ExecSpec spec ->
                spec.commandLine(mkdirCommandLine)
                spec.environment(finalEnv)
            }
        }

        // Do command
        project.exec { ExecSpec spec ->
            spec.commandLine(copyCommandLine)
            spec.environment(finalEnv)
        }
    }
}
