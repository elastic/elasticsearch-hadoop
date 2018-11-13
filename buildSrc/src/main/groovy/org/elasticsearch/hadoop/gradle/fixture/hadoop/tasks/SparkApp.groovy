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
import org.elasticsearch.hadoop.gradle.fixture.hadoop.services.SparkYarnServiceDescriptor
import org.gradle.api.GradleException
import org.gradle.api.tasks.TaskAction
import org.gradle.process.ExecSpec

class SparkApp extends AbstractClusterTask {

    enum Master {
        LOCAL, YARN, STANDALONE
    }

    enum DeployMode {
        CLIENT, CLUSTER
    }

    String jobClass
    File jobJar
    Master master = Master.YARN
    DeployMode deployMode = DeployMode.CLIENT
    List<File> libJars = []
    List<String> args = []

    void deployMode(DeployMode mode) {
        deployMode = mode
    }

    void deployModeClient() {
        deployMode = DeployMode.CLIENT
    }

    void deployModeCluster() {
        deployMode = DeployMode.CLUSTER
    }

    @TaskAction
    void runSparkSubmit() {
        //Verification
        if (clusterConfiguration == null) {
            // FIXHERE: Remove once we have a plugin and extension
            throw new GradleException("no cluster configuration found")
        }
        if (jobClass == null) {
            throw new GradleException("No job class given")
        }
        if (jobJar == null) {
            throw new GradleException("No job jar given")
        }

        // Gateway conf
        InstanceConfiguration sparkGateway = clusterConfiguration
            .service(HadoopClusterConfiguration.SPARK)
            .role(SparkYarnServiceDescriptor.GATEWAY)
            .instance(0)

        File baseDir = sparkGateway.getBaseDir()
        File homeDir = new File(baseDir, sparkGateway.getServiceDescriptor().homeDirName(sparkGateway))
        File binDir = new File(homeDir, sparkGateway.getServiceDescriptor().scriptDir(sparkGateway))
        String commandName = 'spark-submit' // TODO: Windows?
        File command = new File(binDir, commandName)

        String argMaster = getMasterURL(sparkGateway)

        String argDeployMode = getDeployModeValue()

        // bin/spark-submit --master yarn --deploy-mode client --class <class> path/to/jar.jar
        List<String> commandLine = [command.toString(),
                                    '--master', argMaster,
                                    '--deploy-mode', argDeployMode,
                                    '--class', jobClass,
                                    jobJar.toString()]

        // HADOOP_CONF_DIR=..../etc/hadoop
        Map<String, String> finalEnv = sparkGateway.getEnvironmentVariables()
        sparkGateway.getServiceDescriptor().finalizeEnv(finalEnv, sparkGateway)

        // Do command
        project.exec { ExecSpec spec ->
            spec.commandLine(commandLine)
            spec.environment(finalEnv)
        }
    }

    private String getMasterURL(InstanceConfiguration sparkGateway) {
        if (master == Master.YARN) {
            return 'yarn'
        } else {
            // TODO: Eventually support standalone or local
            throw new GradleException("Unsupported Master mode $master")
        }
    }

    private String getDeployModeValue() {
        return deployMode.name().toLowerCase()
    }
}
