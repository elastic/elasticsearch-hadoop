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

package org.elasticsearch.hadoop.gradle.fixture.hadoop

import org.apache.tools.ant.taskdefs.condition.Os
import org.elasticsearch.gradle.testclusters.ElasticsearchCluster
import org.elasticsearch.hadoop.gradle.util.WaitForURL
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.InstanceConfiguration
import org.gradle.api.GradleException
import org.gradle.api.Project

import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import static org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.SettingsContainer.FileSettings

/**
 * Generic information for any process running in a hadoop ecosystem.
 *
 * Might be a yarn node, might be an hdfs node, or even a hive server.
 * Anything that can be run as a service program within Hadoop.
 */
class InstanceInfo {

    /** Gradle project this node is part of */
    Project project

    /** Configuration for this running instance */
    InstanceConfiguration config

    /** The numbered instance of this node */
    int instance

    /** name of the cluster this node is part of */
    // Unneeded? Found 1 use
    String clusterName

    /** root directory all node files and operations happen under */
    File baseDir

    /** shared data directory all services share */
    // Unneeded?
    File sharedDir

    /** the pid file the node will use */
    File pidFile

    /** service home dir */
    File homeDir

    /** service config directory */
    File confDir

    /** The config files */
    List<File> configFiles

    Map<String, FileSettings> configContents

    /** Closure that renders the contents of the config file */
    Closure<String> configFileFormatter

    /** working directory for the service process */
    File cwd

    /** file that if it exists, indicates the node failed to start */
    File failedMarker

    /** stdout/stderr log of the service process for this instance */
    File startLog

    /** Location of the java installation to use when running processes **/
    String javaHome

    /** environment variables to start the node with */
    Map<String, String> env

    /** arguments to start the node with */
    List<String> args

    /** Executable to run the service start up script with, either cmd or sh */
    String executable

    /** Path to the service start script */
    private Object startScript

    /** script to run when running in the background */
    private File backgroundScript

    /** script that wraps the regular start command to get the PID */
    private File pidWrapperScript

    /** buffer for ant output when starting this node */
    ByteArrayOutputStream buffer = new ByteArrayOutputStream()

    /** Elasticsearch cluster dependency for tasks **/
    ElasticsearchCluster elasticsearchCluster

    /**
     * A closure to call before the cluster is considered ready. The closure is passed the node info,
     * as well as a groovy AntBuilder, to enable running ant condition checks. The default wait
     * condition is for http on the http port.
     */
    Closure waitCondition = { InstanceInfo instanceInfo, AntBuilder ant ->
        String waitUrl = instanceInfo.httpUri()
        if (waitUrl == null) {
            return true
        } else {
            ant.echo(message: "==> [${new Date()}] checking health: ${waitUrl}",
                    level: 'info')
            // TODO make maxwait configurable
            return new WaitForURL()
                    .setUrl(waitUrl)
                    .setTrustAllSSLCerts(true)
                    .setTrustAllHostnames(true)
                    .setTotalWaitTime(30, TimeUnit.SECONDS)
                    .setCheckEvery(500, TimeUnit.MILLISECONDS)
                    .checkAvailability(project)
        }
    }

    /** Holds node configuration for part of a test cluster. */
    InstanceInfo(InstanceConfiguration config, Project project, String prefix, File sharedDir) {
        this.config = config
        this.project = project

        this.instance = config.instance
        this.sharedDir = sharedDir

        clusterName = project.path.replace(':', '_').substring(1) + '_' + prefix

        // Note: Many hadoop scripts break when using spaces in names
        baseDir = config.getBaseDir()
        pidFile = new File(baseDir, config.getServiceDescriptor().pidFileName(config))
        homeDir = new File(baseDir, config.getServiceDescriptor().homeDirName(config))
        confDir = new File(homeDir, config.getServiceDescriptor().confDirName(config))

        configFiles = config.getServiceDescriptor().configFiles(config).collect { name -> new File(confDir, name) }
        configContents = config.getServiceDescriptor().collectConfigFilesContents(config)
        configFileFormatter = config.getServiceDescriptor().configFormat(config)
        cwd = new File(baseDir, "cwd")
        failedMarker = new File(cwd, 'run.failed')
        startLog = new File(cwd, 'run.log')

        // We just default to the current runtime at this time
        javaHome = config.getJavaHome()

        // Prepare Environment
        env = [:]
        env.putAll(config.getEnvironmentVariables())
        config.getServiceDescriptor().finalizeEnv(env, config)

        // Add JAVA_HOME to the environment
        env['JAVA_HOME'] = javaHome

        // Prepare startup command and arguments
        args = []
        List<String> startCommandLine = config.getServiceDescriptor().startCommand(config)
        if (Os.isFamily(Os.FAMILY_WINDOWS)) {
            // TODO: Test on windows to see if this actually works
            executable = 'cmd'
            args.add('/C')
            args.add('"') // quote the entire command
            backgroundScript = new File(cwd, "run.bat")
            pidWrapperScript = new File(cwd, "start.bat")
            /*
             * We have to delay building the string as the path will not exist during configuration which will fail on Windows due to
             * getting the short name requiring the path to already exist.
             */
            startScript = "${-> binPath().resolve(startCommandLine.first()).toString()}"
        } else {
            executable = 'bash'
            backgroundScript = new File(cwd, "run")
            pidWrapperScript = new File(cwd, "start")
            startScript = binPath().resolve(startCommandLine.first())
        }

        if (Os.isFamily(Os.FAMILY_WINDOWS)) {
            /*
             * We have to delay building the string as the path will not exist during configuration which will fail on Windows due to
             * getting the short name requiring the path to already exist.
             */
            args.add("${-> getShortPathName(backgroundScript.toString())}")
        } else {
            args.add("${backgroundScript}")
        }

        // Add tail of arguments to the command to run
        if (startCommandLine.size() > 1) {
            args.addAll(startCommandLine.tail())
        }

        if (Os.isFamily(Os.FAMILY_WINDOWS)) {
            args.add('"') // end the entire command, quoted
        }

        this.elasticsearchCluster = config.getElasticsearchCluster()
    }

    Path binPath() {
        String dir = config.getServiceDescriptor().scriptDir(config)
        if (Os.isFamily(Os.FAMILY_WINDOWS)) {
            return Paths.get(getShortPathName(new File(homeDir, dir).toString()))
        } else {
            return Paths.get(new File(homeDir, dir).toURI())
        }
    }

    /*
     * Short Path is a windows filesystem characteristic that lets you refer to folders that have spaces in the name:
     * C:\Program Files ---> C:\PROGRA~1\
     */
    static String getShortPathName(String path) {
//        assert Os.isFamily(Os.FAMILY_WINDOWS)
//        final WString longPath = new WString("\\\\?\\" + path)
//        // first we get the length of the buffer needed
//        final int length = JNAKernel32Library.getInstance().GetShortPathNameW(longPath, null, 0)
//        if (length == 0) {
//            throw new IllegalStateException("path [" + path + "] encountered error [" + Native.getLastError() + "]")
//        }
//        final char[] shortPath = new char[length]
//        // knowing the length of the buffer, now we get the short name
//        if (JNAKernel32Library.getInstance().GetShortPathNameW(longPath, shortPath, length) == 0) {
//            throw new IllegalStateException("path [" + path + "] encountered error [" + Native.getLastError() + "]")
//        }
//        // we have to strip the \\?\ away from the path for cmd.exe
//        return Native.toString(shortPath).substring(4)
        throw new UnsupportedOperationException("JNAKernal32Library is compiled for Java 10 and up.")
    }

    /** Returns debug string for the command that started this node. */
    String getCommandString() {
        String commandString = "\nService ${config.serviceDescriptor.serviceName()}: ${config.roleDescriptor.roleName()} configuration:\n"
        commandString += "|-----------------------------------------\n"
        commandString += "|  cwd: ${cwd}\n"
        commandString += "|  command: ${executable} ${args.join(' ')}\n"
        commandString += '|  environment:\n'
        env.each { k, v -> commandString += "|    ${k}: ${v}\n" }
        commandString += "|\n|  [${backgroundScript.name}]\n"
        backgroundScript.eachLine('UTF-8', { line -> commandString += "    ${line}\n"})
        commandString += "|\n|  [${pidWrapperScript.name}]\n"
        pidWrapperScript.eachLine('UTF-8', { line -> commandString += "    ${line}\n"})
        configFiles.forEach { configFile ->
            commandString += "|\n|  [${configFile.name}]\n"
            configFile.eachLine('UTF-8', { line -> commandString += "|    ${line}\n" })
        }
        commandString += "|-----------------------------------------"
        return commandString
    }

    void writeWrapperScripts() {
        if (Os.isFamily(Os.FAMILY_WINDOWS)) {
            // TODO Eventually support Windows
            throw new GradleException('Full test fixtures on Windows are currently unsupported')
        }
        String pidWrapperScriptContents = "echo \$\$> ${pidFile}; exec \"${startScript}\" \"\$@\" > run.log 2>&1;"
        pidWrapperScript.setText(pidWrapperScriptContents, 'UTF-8')
        pidWrapperScript.setExecutable(true)
        String backgroundScriptContents = "\"${pidWrapperScript}\" \"\$@\"; if [ \\\$? != 0 ]; then touch ${failedMarker}; rm -f ${pidFile}; fi"
        backgroundScript.setText(backgroundScriptContents, 'UTF-8')
        backgroundScript.setExecutable(true)
    }

    String httpUri() {
        return config.serviceDescriptor.httpUri(config, configContents)
    }

    @Override
    String toString() {
        return "InstanceInfo{cluster='${clusterName}', service='${config.serviceDescriptor.serviceName()}', " +
                "role='${config.roleDescriptor.roleName()}', instance=${instance}"
    }
}
