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
import org.elasticsearch.hadoop.gradle.fixture.hadoop.services.PigServiceDescriptor
import org.gradle.api.GradleException
import org.gradle.api.tasks.TaskAction
import org.gradle.process.ExecSpec

class PigScript extends AbstractClusterTask {

    File script
    List<File> libJars = []

    void libJars(File... files) {
        libJars.addAll(files)
    }

    @Override
    InstanceConfiguration defaultInstance(HadoopClusterConfiguration clusterConfiguration) {
        return clusterConfiguration
                .service(HadoopClusterConfiguration.PIG)
                .role(PigServiceDescriptor.GATEWAY)
                .instance(0)
    }

    @Override
    Map<String, String> taskEnvironmentVariables() {
        return [:]
    }

    @TaskAction
    void runPig() {
        // Verification
        if (clusterConfiguration == null) {
            throw new GradleException("No cluster configuration found")
        }
        if (script == null) {
            throw new GradleException("No script given")
        }

        // Gateway conf
        InstanceConfiguration pigGateway = getInstance()

        File baseDir = pigGateway.getBaseDir()
        File homeDir = new File(baseDir, pigGateway.getServiceDescriptor().homeDirName(pigGateway))
        File binDir = new File(homeDir, pigGateway.serviceDescriptor.scriptDir(pigGateway))
        String commandName = 'pig' // Todo: Or 'pig.cmd' for Windows
        File command = new File(binDir, commandName)

        // bin/pig [-Dpig.additional.jars=<libjars>] [-f <scriptFile>]
        List<String> commandLine = [command.toString()]

        if (!libJars.isEmpty()) {
            String jars = libJars.collect { file -> file.getAbsolutePath() }.join(':')
            commandLine.add("-Dpig.additional.jars=${jars}")
        }

        if (script != null) {
            commandLine.addAll(['-f', script.toString()])
        }

        // Use the service descriptor to pick up HADOOP_HOME=<hadoopHome>
        Map<String, String> environment = collectEnvVars()

        // Additional env's
        // PIG_HEAPSIZE - In MB
        // JAVA_HOME - For the JVM's yo
        // PIG_CLASSPATH - Additional java classpath entries
        // PIG_USER_CLASSPATH_FIRST - Prepend instead append libs. Might be good to randomize
        // PIG_OPTS - JVM options to set
        // HADOOP_CONF_DIR - Seems that setting HADOOP_HOME handles this ok.
        // SPARK_HOME - If using spark mode (non-local, local spark uses embedded jars in pig)
        // SPARK_JAR - HDFS URI for spark-assembly*.jar. Needs to exist on HDFS before hand.
        // HIVE_HOME - If HCatalog is wanted
        // HCAT_HOME - If HCatalog is wanted, usually $HIVE_HOME/hcatalog

        // Possible args:
        // -x/-exectype spark/spark_local (If running on Spark is wanted)
        // -useHCatalog (If HCataclog is wanted)

        project.logger.info("Environment: ${environment}")
        project.exec { ExecSpec spec ->
            spec.setCommandLine(commandLine)
            spec.environment(environment)
        }
    }
}
