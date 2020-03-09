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
import org.gradle.api.tasks.TaskAction
import org.gradle.process.ExecSpec

import static org.elasticsearch.hadoop.gradle.util.ObjectUtil.unapplyString

class HadoopMRJob extends AbstractClusterTask {

    String jobClass
    File jobJar
    Map<String, Object> jobSettings = [:]
    List<File> libJars = []
    List<String> args = []
    Map<String, String> systemProperties = [:]

    void jobSetting(String key, Object value) {
        jobSettings.put(key, value)
    }

    void jobSettings(Map<String, Object> settings) {
        jobSettings.putAll(settings)
    }

    void libJars(File... files) {
        libJars.addAll(files)
    }

    void systemProperty(String key, String value) {
        systemProperties.put(key, value)
    }

    void systemProperties(Map<String, String> settings) {
        systemProperties.putAll(settings)
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
        Map<String, String> taskEnv = [:]
        InstanceConfiguration instance = getInstance()

        // Add lib jars to the user classpath if needed
        if (!libJars.isEmpty()) {
            taskEnv.put('YARN_USER_CLASSPATH', libJars.join(":"))
        }

        // Collect the java properties and JVM options from the cluster configuration, as well as the system props from
        // this specific task. Add them to the environment variable that Hadoop would expect them to be specified under.
        String javaPropertyEnvVariable = instance.getServiceDescriptor().javaOptsEnvSetting(instance)
        if (javaPropertyEnvVariable != null) {
            List<String> javaOpts = [taskEnv.get(javaPropertyEnvVariable, '')]
            javaOpts.add(instance.getJvmArgs())
            for (Map<String, String> propertyMap : [instance.getSystemProperties(), systemProperties]) {
                String collectedSystemProperties = propertyMap
                        .collect { key, value -> "-D${key}=${value}" }
                        .join(" ")
                if (!collectedSystemProperties.isEmpty()) {
                    javaOpts.add(collectedSystemProperties)
                }
            }
            // Force the javaOptsEnvSetting to be the correct one for executing a hadoop jar command.
            // We might be running this command "on a different instance" than gateway
            // (it might be running on namenode, and be using namenode's env property names)
            String hadoopJarCommandJavaPropertyEnvVariable = instance
                    .getServiceDescriptor()
                    .javaOptsEnvSetting(defaultInstance(clusterConfiguration))
            taskEnv.put(hadoopJarCommandJavaPropertyEnvVariable, javaOpts.join(" ").trim())
        }

        return taskEnv
    }

    @TaskAction
    void runYarnJar() {
        // Verification
        if (clusterConfiguration == null) {
            throw new GradleException("No cluster configuration found")
        }
        if (jobClass == null) {
            throw new GradleException("No job class given")
        }
        if (jobJar == null) {
            throw new GradleException("No job jar given")
        }

        // Gateway conf
        InstanceConfiguration hadoopGateway = executedOn
        if (hadoopGateway == null) {
            hadoopGateway = clusterConfiguration
                    .service(HadoopClusterConfiguration.HADOOP)
                    .role(HadoopServiceDescriptor.GATEWAY)
                    .instance(0)
        }

        File baseDir = hadoopGateway.getBaseDir()
        File homeDir = new File(baseDir, hadoopGateway.getServiceDescriptor().homeDirName(hadoopGateway))
        File binDir = new File(homeDir, hadoopGateway.serviceDescriptor.scriptDir(hadoopGateway))
        String commandName = 'yarn' // TODO: or yarn.cmd for Windows
        File command = new File(binDir, commandName)
        // bin/yarn jar job.jar full.class.name.Here <genericArgs> <args>
        List<String> commandLine = [command.toString(), 'jar', jobJar.toString(), jobClass]
        if (!libJars.isEmpty()) {
            commandLine.addAll(['-libjars', libJars.join(',')])
        }
        if (!jobSettings.isEmpty()) {
            commandLine.addAll(jobSettings.collect { k, v -> "-D${k}=${unapplyString(v)}"})
        }
        if (!args.isEmpty()) {
            commandLine.addAll(args)
        }

        Map<String, String> finalEnv = collectEnvVars()

        // Do command
        project.logger.info("Executing Command: " + commandLine)
        project.logger.info("Command Env: " + finalEnv)
        project.exec { ExecSpec spec ->
            spec.commandLine(commandLine)
            spec.environment(finalEnv)
        }
    }
}
