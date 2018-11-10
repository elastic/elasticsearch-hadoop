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
import org.elasticsearch.hadoop.gradle.fixture.hadoop.services.HiveServiceDescriptor
import org.gradle.api.GradleException
import org.gradle.api.tasks.TaskAction
import org.gradle.process.ExecSpec

class HiveBeeline extends AbstractClusterTask {

    File script

    @TaskAction
    void runBeeline() {
        // Verification
        if (clusterConfiguration == null) {
            // FIXHERE: Remove once we have a plugin and extension
            throw new GradleException("No cluster configuration found")
        }

        // Gateway conf
        InstanceConfiguration hiveServer = clusterConfiguration
                .service(HadoopClusterConfiguration.HIVE)
                .role(HiveServiceDescriptor.HIVESERVER)
                .instance(0)

        File baseDir = hiveServer.getBaseDir()
        File homeDir = new File(baseDir, hiveServer.getServiceDescriptor().homeDirName(hiveServer))
        File binDir = new File(homeDir, hiveServer.serviceDescriptor.scriptDir(hiveServer))
        String commandName = 'beeline' // No windows option that I can see
        File command = new File(binDir, commandName)

        // Connection String
        String connectionString = getConnectionString(hiveServer)

        // bin/beeline -u <connection> [-f <scriptFile>]
        List<String> commandLine = [command.toString(), '-u', connectionString]

        if (script != null) {
            commandLine.addAll(['-f', script.toString()])
        }

        // Use the service descriptor to pick up HADOOP_HOME=<hadoopHome>
        Map<String, String> environment = hiveServer.getEnvironmentVariables()
        hiveServer.getServiceDescriptor().finalizeEnv(environment, hiveServer)

        project.exec { ExecSpec spec ->
            spec.setCommandLine(commandLine)
            spec.environment(environment)
        }
    }

    static String getConnectionString(InstanceConfiguration hiveServer) {
        Map<String, String> hiveconf = hiveServer
                .getServiceDescriptor()
                .collectConfigFilesContents(hiveServer)
                .get('hive-site.xml')

        String thriftPort = hiveconf.getOrDefault('hive.server2.thrift.port', '10000')
        String thriftBindHost = hiveconf.getOrDefault('hive.server2.thrift.bind.host', 'localhost')

        // jdbc:hive2://localhost:10000
        return "jdbc:hive2://${thriftBindHost}:${thriftPort}"
    }
}
