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

import com.sun.jna.Native
import com.sun.jna.WString
import org.apache.tools.ant.taskdefs.condition.Os
import org.elasticsearch.gradle.Version
import org.elasticsearch.gradle.test.JNAKernel32Library
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.InstanceConfiguration
import org.gradle.api.GradleException
import org.gradle.api.Project

import java.nio.file.Path
import java.nio.file.Paths

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

    /** Compound name of this instance's service group and role names */
    ServiceIdentifier serviceId

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
    File pathConf

    /** data directory (as an Object, to allow lazy evaluation) */
    Object dataDir

    /** The config files */
    List<File> configFiles

    Map<String, Map<String, String>> configContents

    /** Closure that renders the contents of the config file */
    Closure<String> configFileFormatter

    /** working directory for the service process */
    File cwd

    /** file that if it exists, indicates the node failed to start */
    File failedMarker

    /** stdout/stderr log of the service process for this instance */
    File startLog

    /** directory to install plugins from */
    // Unneeded
    File pluginsTmpDir

    /** Major version of java this node runs with, or {@code null} if using the runtime java version */
    Integer javaVersion

    /** environment variables to start the node with */
    Map<String, String> env

    /** arguments to start the node with */
    List<String> args

    /** Executable to run the service start up script with, either cmd or sh */
    String executable

    /** Path to the service start script */
    private Object startScript

    /** script to run when running in the background */
    private File wrapperScript

    private File backgroundScript

    private File pidWrapperScript

//    /** true if the service requires us to wrap the execution to ensure daemonizing it */
//    boolean spawn

    /** buffer for ant output when starting this node */
    ByteArrayOutputStream buffer = new ByteArrayOutputStream()

    /** Holds node configuration for part of a test cluster. */
    InstanceInfo(InstanceConfiguration config, Project project, String prefix, File sharedDir) {
        this.config = config
        this.project = project

        this.serviceId = new ServiceIdentifier(
                serviceName: config.serviceConf.serviceDescriptor.serviceName(),
                subGroup: config.serviceConf.serviceDescriptor.serviceSubGroup(),
                roleName: config.roleConf.roleDescriptor.roleName()
        )

        this.instance = config.instance
        this.sharedDir = sharedDir

        Version version = config.serviceConf.version

        clusterName = project.path.replace(':', '_').substring(1) + '_' + prefix

        // Note: Many hadoop scripts break when using spaces in names
        baseDir = config.getBaseDir()
        pidFile = new File(baseDir, config.getServiceDescriptor().pidFileName(serviceId))
        homeDir = new File(baseDir, config.getServiceDescriptor().homeDirName(config))
        pathConf = new File(homeDir, config.getServiceDescriptor().configPath(serviceId))
//        def getDataDir = config.getDataDir()
//        if (getDataDir != null) {
//            dataDir = "${getDataDir(serviceId)}"
//        } else {
//            dataDir = new File(homeDir, "data")
//        }
        configFiles = config.getServiceDescriptor().configFiles(serviceId).collect { name -> new File(pathConf, name) }
        configContents = config.getServiceDescriptor().collectConfigFilesContents(config)
        configFileFormatter = config.getServiceDescriptor().configFormat(serviceId)
        // FIXHERE: Logs can be configured (at least for Hadoop core) via system properties (this is only used for port files though)
        // even for rpm/deb, the logs are under home because we dont start with real services
//        File logsDir = new File(homeDir, 'logs')
        // Probably don't need ports files.
//        httpPortsFile = new File(logsDir, 'http.ports')
//        transportPortsFile = new File(logsDir, 'transport.ports')
        cwd = new File(baseDir, "cwd")
        failedMarker = new File(cwd, 'run.failed')
        startLog = new File(cwd, 'run.log')

        // FIXHERE Is this needed for Hadoop?
        // Determine Java Version
//        if (version.before("6.2.0")) {
//            javaVersion = 8
//        } else if (version.onOrAfter("6.2.0") && version.before("6.3.0")) {
//            javaVersion = 9
//        } else if (project.inFipsJvm && version.onOrAfter("6.3.0") && version.before("6.4.0")) {
//            /*
//             * Elasticsearch versions before 6.4.0 cannot be run in a FIPS-140 JVM. If we're running
//             * bwc tests in a FIPS-140 JVM, ensure that the pre v6.4.0 nodes use a Java 10 JVM instead.
//             */
//            javaVersion = 10
//        }
        javaVersion = 8

        // Prepare Environment
        env = [:]
        env.putAll(config.getEnvironmentVariables())
        config.getServiceDescriptor().finalizeEnv(env, config, baseDir)

        // FIXHERE: Is this needed / supported uniformly for hadoop?
//        if (Os.isFamily(Os.FAMILY_WINDOWS)) {
//            /*
//             * We have to delay building the string as the path will not exist during configuration which will fail on Windows due to
//             * getting the short name requiring the path to already exist.
//             */
//            env.put('ES_PATH_CONF', "${-> getShortPathName(pathConf.toString())}")
//        }
//        else {
//            env.put('ES_PATH_CONF', pathConf)
//        }

        // Prepare startup command and arguments
        args = []
//        List<String> startCommandLine = config.isDaemonized() ? config.getServiceDescriptor().daemonStartCommand(serviceId) : config.getServiceDescriptor().startCommand(serviceId)
//        List<String> startCommandLine = config.getServiceDescriptor().daemonStartCommand(serviceId)
        List<String> startCommandLine = config.getServiceDescriptor().startCommand(serviceId)
        if (Os.isFamily(Os.FAMILY_WINDOWS)) {
            // FIXHERE: Test on windows to see if this actually works
            executable = 'cmd'
            args.add('/C')
            args.add('"') // quote the entire command
//            wrapperScript = new File(cwd, "run.bat")
            backgroundScript = new File(cwd, "run.bat")
            pidWrapperScript = new File(cwd, "start.bat")
            /*
             * We have to delay building the string as the path will not exist during configuration which will fail on Windows due to
             * getting the short name requiring the path to already exist.
             */
            startScript = "${-> binPath().resolve(startCommandLine.first()).toString()}"
        } else {
            executable = 'bash'
//            wrapperScript = new File(cwd, "run")
            backgroundScript = new File(cwd, "run")
            pidWrapperScript = new File(cwd, "start")
            startScript = binPath().resolve(startCommandLine.first())
        }
//        spawn = config.getServiceDescriptor().wrapScript(serviceId)
//        if (config.isDaemonized() && spawn) {
            if (Os.isFamily(Os.FAMILY_WINDOWS)) {
                /*
                 * We have to delay building the string as the path will not exist during configuration which will fail on Windows due to
                 * getting the short name requiring the path to already exist.
                 */
//                args.add("${-> getShortPathName(wrapperScript.toString())}")
                args.add("${-> getShortPathName(backgroundScript.toString())}")
            } else {
                args.add("${backgroundScript}")
            }
//        } else {
//            args.add("${startScript}")
//        }

        // Add tail of arguments to the command to run
        if (startCommandLine.size() > 1) {
            args.addAll(startCommandLine.tail())
        }

        // FIXHERE Is this needed for Hadoop?
//        for (Map.Entry<String, String> property : System.properties.entrySet()) {
//            if (property.key.startsWith('tests.es.')) {
//                args.add("-E")
//                args.add("${property.key.substring('tests.es.'.size())}=${property.value}")
//            }
//        }

        // FIXHERE setting data directory from command line might not be possible/advised with Hadoop
//        if (!System.properties.containsKey("tests.es.path.data")) {
//            if (Os.isFamily(Os.FAMILY_WINDOWS)) {
//                /*
//                 * We have to delay building the string as the path will not exist during configuration which will fail on Windows due to
//                 * getting the short name requiring the path to already exist. This one is extra tricky because usually we rely on the node
//                 * creating its data directory on startup but we simply can not do that here because getting the short path name requires
//                 * the directory to already exist. Therefore, we create this directory immediately before getting the short name.
//                 */
//                args.addAll("-E", "path.data=${-> Files.createDirectories(Paths.get(dataDir.toString())); getShortPathName(dataDir.toString())}")
//            } else {
//                args.addAll("-E", "path.data=${-> dataDir.toString()}")
//            }
//        }

        if (Os.isFamily(Os.FAMILY_WINDOWS)) {
            args.add('"') // end the entire command, quoted
        }
    }

    Path binPath() {
//        String dir = config.isDaemonized() ? config.getServiceDescriptor().daemonScriptDir(serviceId) : config.getServiceDescriptor().scriptDir(serviceId)
//        String dir = config.getServiceDescriptor().daemonScriptDir(serviceId)
        String dir = config.getServiceDescriptor().scriptDir(serviceId)
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
        assert Os.isFamily(Os.FAMILY_WINDOWS)
        final WString longPath = new WString("\\\\?\\" + path)
        // first we get the length of the buffer needed
        final int length = JNAKernel32Library.getInstance().GetShortPathNameW(longPath, null, 0)
        if (length == 0) {
            throw new IllegalStateException("path [" + path + "] encountered error [" + Native.getLastError() + "]")
        }
        final char[] shortPath = new char[length]
        // knowing the length of the buffer, now we get the short name
        if (JNAKernel32Library.getInstance().GetShortPathNameW(longPath, shortPath, length) == 0) {
            throw new IllegalStateException("path [" + path + "] encountered error [" + Native.getLastError() + "]")
        }
        // we have to strip the \\?\ away from the path for cmd.exe
        return Native.toString(shortPath).substring(4)
    }

    /** Return the java home used by this node. */
    String getJavaHome() {
        return javaVersion == null ? project.runtimeJavaHome : project.javaVersions.get(javaVersion)
    }

    /** Returns debug string for the command that started this node. */
    String getCommandString() {
        String commandString = "\nService ${serviceId.serviceName}: ${serviceId.roleName} configuration:\n"
        commandString += "|-----------------------------------------\n"
        commandString += "|  cwd: ${cwd}\n"
        commandString += "|  command: ${executable} ${args.join(' ')}\n"
        commandString += '|  environment:\n'
        commandString += "|    JAVA_HOME: ${javaHome}\n"
        env.each { k, v -> commandString += "|    ${k}: ${v}\n" }
//        if (config.isDaemonized() && spawn) {
            commandString += "|\n|  [${backgroundScript.name}]\n"
            backgroundScript.eachLine('UTF-8', { line -> commandString += "    ${line}\n"})
            commandString += "|\n|  [${pidWrapperScript.name}]\n"
            pidWrapperScript.eachLine('UTF-8', { line -> commandString += "    ${line}\n"})
//            commandString += "|\n|  [${wrapperScript.name}]\n"
//            wrapperScript.eachLine('UTF-8', { line -> commandString += "    ${line}\n"})
//        }
        configFiles.forEach { configFile ->
            commandString += "|\n|  [${configFile.name}]\n"
            configFile.eachLine('UTF-8', { line -> commandString += "|    ${line}\n" })
        }
        commandString += "|-----------------------------------------"
        return commandString
    }

    void writeWrapperScript() {
        writeWrapperScripts()
    }

    void writeWrapperScripts() {
//        String argsPasser = '"$@"'
//        String exitMarker = "; if [ \$? != 0 ]; then touch run.failed; fi"
        if (Os.isFamily(Os.FAMILY_WINDOWS)) {
            // FIXHERE Eventually support Windows
//            argsPasser = '%*'
//            exitMarker = "\r\n if \"%errorlevel%\" neq \"0\" ( type nul >> run.failed )"
            throw new GradleException('Full test fixtures on Windows are currently unsupported')
        }
//        wrapperScript.setText("\"${startScript}\" ${argsPasser} > run.log 2>&1 ${exitMarker}", 'UTF-8')

//        String scriptContents = "echo \$\$> ${pidFile}; \"${startScript}\" \"\$@\" > run.log 2>&1; if [ \$? != 0 ]; then touch run.failed; rm ${pidFile}; fi"
//        wrapperScript.setText(scriptContents, 'UTF-8')

        String pidWrapperScriptContents = "echo \$\$> ${pidFile}; exec \"${startScript}\" \"\$@\" > run.log 2>&1;"
        pidWrapperScript.setText(pidWrapperScriptContents, 'UTF-8')
        pidWrapperScript.setExecutable(true)
        String backgroundScriptContents = "\"${pidWrapperScript}\" \"\$@\"; if [ \\\$? != 0 ]; then touch run.failed; rm -f ${pidFile}; fi"
        backgroundScript.setText(backgroundScriptContents, 'UTF-8')
        backgroundScript.setExecutable(true)

    }

//    /** Returns an address and port suitable for a uri to connect to this node over http */
//    String httpUri() {
//        return httpPortsFile.readLines("UTF-8").get(0)
//    }
//
//    /** Returns an address and port suitable for a uri to connect to this node over transport protocol */
//    String transportUri() {
//        return transportPortsFile.readLines("UTF-8").get(0)
//    }
//
//    /** Returns the file which contains the transport protocol ports for this node */
//    File getTransportPortsFile() {
//        return transportPortsFile
//    }

//    /** Returns the data directory for this node */
//    File getDataDir() {
//        if (!(dataDir instanceof File)) {
//            return new File(dataDir)
//        }
//        return dataDir
//    }
}
