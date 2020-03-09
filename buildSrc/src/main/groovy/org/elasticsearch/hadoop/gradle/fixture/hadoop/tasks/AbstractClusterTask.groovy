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

import org.elasticsearch.gradle.testclusters.DefaultTestClustersTask
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.HadoopClusterConfiguration
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.InstanceConfiguration

abstract class AbstractClusterTask extends DefaultTestClustersTask {

    HadoopClusterConfiguration clusterConfiguration
    InstanceConfiguration executedOn
    Map<String, String> environmentVariables = [:]

    AbstractClusterTask() {
        super()
        this.clusterConfiguration = project.extensions.findByName('hadoopFixture') as HadoopClusterConfiguration
    }

    void runOn(InstanceConfiguration instance) {
        executedOn = instance
    }

    abstract InstanceConfiguration defaultInstance(HadoopClusterConfiguration clusterConfiguration)
    abstract Map<String, String> taskEnvironmentVariables()

    protected getInstance() {
        return executedOn == null ? defaultInstance(this.clusterConfiguration) : executedOn
    }

    protected Map<String, String> collectEnvVars() {
        InstanceConfiguration instance = getInstance()

        Map<String, String> finalEnv = [:]

        // Set JAVA_HOME
        finalEnv['JAVA_HOME'] = instance.javaHome

        // User provided environment variables from the cluster configuration
        finalEnv.putAll(instance.getEnvironmentVariables())

        // Finalize the environment variables using the service descriptor
        instance.getServiceDescriptor().finalizeEnv(finalEnv, instance)

        // Add any environment variables that might be based on the specific
        // task's configuration (jvm options via env, lib jars, etc...)
        finalEnv.putAll(taskEnvironmentVariables())

        // Add the explicit env variables from this task instance at the end
        finalEnv.putAll(environmentVariables)

        return finalEnv
    }
}
