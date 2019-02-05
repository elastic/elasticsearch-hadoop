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
    List<File> libJars = []
    String hivePrincipal
    Map<String, String> env = [:]

    @TaskAction
    void runBeeline() {
        // Verification
        if (clusterConfiguration == null) {
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
        String connectionString = getConnectionString(hiveServer, hivePrincipal)

        // bin/beeline -u <connection> [-f <scriptFile>]
        List<String> commandLine = [command.toString(), '-u', connectionString]

        if (script != null) {
            File finalScript = rewriteScript(homeDir)
            commandLine.addAll(['-f', finalScript.toString()])
        }

        // Use the service descriptor to pick up HADOOP_HOME=<hadoopHome>
        Map<String, String> environment = hiveServer.getEnvironmentVariables()
        hiveServer.getServiceDescriptor().finalizeEnv(environment, hiveServer)
        environment.putAll(env)

        project.logger.info("Using Environment: $environment")
        project.exec { ExecSpec spec ->
            spec.setCommandLine(commandLine)
            spec.environment(environment)
        }
    }

    static String getConnectionString(InstanceConfiguration hiveServer, String hivePrincipal) {
        Map<String, String> hiveconf = hiveServer
                .getServiceDescriptor()
                .collectConfigFilesContents(hiveServer)
                .get('hive-site.xml')

        String thriftPort = hiveconf.getOrDefault('hive.server2.thrift.port', '10000')
        String thriftBindHost = hiveconf.getOrDefault('hive.server2.thrift.bind.host', 'localhost')

        String authority = ""
        if (hivePrincipal != null && !hivePrincipal.isEmpty()) {
            authority = ";principal=$hivePrincipal"
        }

        // jdbc:hive2://localhost:10000
        return "jdbc:hive2://${thriftBindHost}:${thriftPort}/${authority}"
    }

    File rewriteScript(File hiveHome) {
        // If there are libraries required, copy the contents of the script to a different working location
        // and prepend the libraries to the script with ADD JAR commands.
        if (libJars.isEmpty()) {
            return script
        } else {
            File hiveScriptDir = new File(hiveHome, "usr/scripts")
            hiveScriptDir.mkdirs()
            File modifiedScriptFile = new File(hiveScriptDir, script.getName())
            modifiedScriptFile.createNewFile()
            String addJarDirectives = libJars.collect { jarFile -> "ADD JAR ${jarFile.getAbsolutePath()};" }.join('\n')
            String finalContents = addJarDirectives + '\n' + script.getText()
            modifiedScriptFile.setText(finalContents, 'UTF-8')
            return modifiedScriptFile
        }
    }
}
