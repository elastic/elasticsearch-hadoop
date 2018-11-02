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

import org.apache.tools.ant.DefaultLogger
import org.elasticsearch.gradle.LoggedExec
import org.elasticsearch.gradle.Version
import org.elasticsearch.gradle.test.Fixture
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.HadoopClusterConfiguration
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.InstanceConfiguration
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.ServiceConfiguration
import org.elasticsearch.hadoop.gradle.tasks.ApacheMirrorDownload
import org.elasticsearch.hadoop.gradle.tasks.VerifyChecksums
import org.gradle.api.AntBuilder
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.Task
import org.gradle.api.tasks.Copy
import org.gradle.api.tasks.Delete
import org.gradle.api.tasks.Exec
import shadow.org.codehaus.plexus.util.Os

import java.nio.file.Paths;

/**
 * A helper for creating tasks to build a cluster that is used by a task, and tear down the cluster
 * when the task is finished.
 */
class HadoopClusterFormationTasks {

    // Pie in the sky DSL
//    hadoopFixture {
//        hdfs {
//            namenode {
//                config 'key', 'value'
//                jvmArg 'arg'
//                env 'key', 'value'
//            }
//            datanode {
//                config 'key', 'value'
//                jvmArg 'arg'
//                env 'key', 'value'
//            }
//        }
//        yarn {
//            resourcemanager {
//                config 'key', 'value'
//                jvmArg 'arg'
//                env 'key', 'value'
//            }
//            nodemanager {
//                config 'key', 'value'
//                jvmArg 'arg'
//                env 'key', 'value'
//            }
//        }
//        hive {
//            hiveserver {
//                config 'key', 'value'
//                jvmArg 'arg'
//                env 'key', 'value'
//            }
//        }
//    }

    static class InstanceTasks {
        Task startTask
        Task stopTask
    }

    /**
     * Adds dependent tasks to the given task to start and stop a cluster with the given configuration.
     * <p>
     * Returns a list of NodeInfo objects for each node in the cluster.
     *
     * Based on {@link org.elasticsearch.gradle.test.ClusterFormationTasks}
     */
    static List<InstanceInfo> setup(Project project, Task runner, HadoopClusterConfiguration config) {
        String prefix = config.getName()

        File sharedDir = new File(project.buildDir, "fixtures/shared")
        Object startDependencies = config.getDependencies()

        /*
         * First clean the environment
         */
        Task cleanup = project.tasks.create(
            name: "${prefix}#prepareCluster.cleanShared",
            type: Delete,
            dependsOn: startDependencies) {
                delete sharedDir
                doLast {
                    sharedDir.mkdirs()
                }
        }
        startDependencies = cleanup

        List<Task> serviceStartTasks = []
        List<Task> serviceStopTasks = []
        List<InstanceInfo> nodes = []

        // FIXHERE: Check node topologies
        // Do we actually need to do this at all?
        // - check node ranges, (num nodes >= bwc nodes)
        // - require bwc version on non zero bwc node count
        for (ServiceConfiguration serviceConfiguration : config.getServices()) {
            List<Task> instanceStartTasks = []
            List<Task> instanceStopTasks = []

            // Service depends either on the cluster's start dependencies, or the start task for the previous service
            List<Object> serviceDependencies = []
            if (serviceStartTasks.empty) {
                serviceDependencies.addAll(startDependencies)
            } else {
                serviceDependencies.add(serviceStartTasks.last())
            }

            // Get the download task for this service's package and add it to the service's dependency tasks
            DistributionTasks distributionTasks = getOrConfigureDistributionDownload(project, serviceConfiguration)

            // Get the instances for this service in their deployment order
            List<InstanceConfiguration> instances = serviceConfiguration.getRoles()
                    .collect { role -> role.getInstances() }.flatten() as List<InstanceConfiguration>

            // Set up each instance
            for (InstanceConfiguration instanceConfiguration : instances) {
                InstanceInfo instanceInfo = new InstanceInfo(instanceConfiguration, project, prefix, sharedDir)
                nodes.add(instanceInfo)
                // First instance in a service needs to also depend on its download task (serviceDependencies)
                Object dependsOn = serviceStartTasks.empty ? startDependencies : serviceStartTasks.last()
                InstanceTasks instanceTasks = configureNode(project, prefix, runner, dependsOn, instanceInfo, distributionTasks)
                serviceStartTasks.add(instanceTasks.startTask)
                instanceStartTasks.add(instanceTasks.startTask)
                if (instanceTasks.stopTask != null) {
                    instanceStopTasks.add(instanceTasks.stopTask)
                }
            }

            // FIXHERE: Configure wait command
            // wait command for entire service
            // wait depends on last instance start task
            // runner depends on wait task

            runner.dependsOn(instanceStartTasks)
        }


        return nodes
    }

    /**
     * Pairing of download and verification tasks for a distribution
     */
    static class DistributionTasks {
        ApacheMirrorDownload download
        VerifyChecksums verify
    }

    /**
     * Returns a download task for this service's packages, either an already created one from the root project, or
     * a newly created download task.
     */
    static DistributionTasks getOrConfigureDistributionDownload(Project project, ServiceConfiguration serviceConfiguration) {
        Version serviceVersion = serviceConfiguration.getVersion()

        String downloadTaskName = "download${serviceConfiguration.serviceDescriptor.packageName().capitalize()}#${serviceVersion}"
        String verifyTaskName = "verify${serviceConfiguration.serviceDescriptor.packageName().capitalize()}#${serviceVersion}"

        ApacheMirrorDownload downloadTask = project.rootProject.tasks.findByName(downloadTaskName) as ApacheMirrorDownload
        if (downloadTask == null) {
            downloadTask = project.rootProject.tasks.create(name: downloadTaskName, type: ApacheMirrorDownload) as ApacheMirrorDownload
            serviceConfiguration.getServiceDescriptor().configureDownload(downloadTask, serviceConfiguration)
            downloadTask.onlyIf { !downloadTask.outputFile().exists() }
        }

        VerifyChecksums verifyTask = project.rootProject.tasks.findByName(verifyTaskName) as VerifyChecksums
        if (verifyTask == null) {
            verifyTask = project.rootProject.tasks.create(name: verifyTaskName, type: VerifyChecksums) as VerifyChecksums
            verifyTask.dependsOn downloadTask
            verifyTask.inputFile downloadTask.outputFile()
            for (Map.Entry<String, String> hash : serviceConfiguration.serviceDescriptor.packageHashVerification(serviceVersion)) {
                verifyTask.checksum hash.key, hash.value
            }
        }

        return new DistributionTasks(download: downloadTask, verify: verifyTask)
    }

    static InstanceTasks configureNode(Project project, String prefix, Task runner, Object dependsOn, InstanceInfo node,
                              DistributionTasks distribution) {
        Task setup = project.tasks.create(name: taskName(prefix, node, 'clean'), type: Delete, dependsOn: dependsOn) {
            delete node.homeDir
            delete node.cwd
        }

        // Only create CWD and check previous if the role is an executable process
        if (node.getConfig().getRoleDescriptor().isExecutableProcess()) {
            setup = project.tasks.create(name: taskName(prefix, node, 'createCwd'), type: DefaultTask, dependsOn: setup) {
                doLast {
                    node.cwd.mkdirs()
                }
                outputs.dir node.cwd
            }
            // FIXHERE: Check Previous
            setup = configureCheckPreviousTask(taskName(prefix, node, 'checkPrevious'), project, setup, node)
            setup = configureStopTask(taskName(prefix, node, 'stopPrevious'), project, setup, node)
        }

        // Always extract the package contents, and configure the files
        setup = configureExtractTask(taskName(prefix, node, 'extract'), project, setup, node, distribution)
        setup = configureWriteConfigTask(taskName(prefix, node, 'configure'), project, setup, node)
        // FIXHERE: Extra Config Files
        setup = configureExtraConfigFilesTask(taskName(prefix, node, 'extraConfig'), project, setup, node)

        // If the role for this instance is not a process, we skip creating start and stop tasks for it.
        if (!node.getConfig().getRoleDescriptor().isExecutableProcess()) {
            return new InstanceTasks(startTask: setup)
        }

        Map<String, Object[]> setupCommands = new LinkedHashMap<>()
        setupCommands.putAll(node.config.getServiceDescriptor().defaultSetupCommands(node.serviceId))
        setupCommands.putAll(node.config.getSetupCommands())
        for (Map.Entry<String, Object[]> command : setupCommands) {
            // the first argument is the actual script name, relative to home
            Object[] args = command.getValue().clone()
            final Object commandPath
            if (Os.isFamily(Os.FAMILY_WINDOWS)) {
                /*
                 * We have to delay building the string as the path will not exist during configuration which will fail on
                 * Windows due to getting the short name requiring the path to already exist. Note that we have to capture the
                 * value of arg[0] now otherwise we would stack overflow later since arg[0] is replaced below.
                 */
                String argsZero = args[0]
                commandPath = "${-> Paths.get(InstanceInfo.getShortPathName(node.homeDir.toString())).resolve(argsZero.toString()).toString()}"
            } else {
                commandPath = node.homeDir.toPath().resolve(args[0].toString()).toString()
            }
            args[0] = commandPath
            setup = configureExecTask(taskName(prefix, node, command.getKey()), project, setup, node, args)
        }

        Task start = configureStartTask(taskName(prefix, node, 'start'), project, setup, node)

        // Configure daemon stop task

//        if (node.config.daemonized) {
            Task stop = configureStopTask(taskName(prefix, node, 'stop'), project, [], node)
            // We're running in the background, so make sure that the stop command is called after the runner task
            // finishes
            runner.finalizedBy(stop)
            start.finalizedBy(stop)
            for (Object dependency : node.config.getClusterConf().getDependencies()) {
                if (dependency instanceof Fixture) {
                    def depStop = ((Fixture)dependency).stopTask
                    runner.finalizedBy(depStop)
                    start.finalizedBy(depStop)
                }
            }
            return new InstanceTasks(startTask: start, stopTask: stop)
//        }
//        return new InstanceTasks(startTask: start)
    }

    static Task configureCheckPreviousTask(String name, Project project, Task setup, InstanceInfo node) {
        return setup
    }

    static Task configureExtractTask(String name, Project project, Task setup, InstanceInfo node, DistributionTasks distribution) {
        List extractDependsOn = [distribution.verify, setup]
        return project.tasks.create(name: name, type: Copy, dependsOn: extractDependsOn) {
            // FIXHERE: Switch logic if a service is ever not a tar distribution
            from {
                project.tarTree(project.resources.gzip(distribution.download.outputFile()))
            }
            into node.baseDir
        }
    }

    static Task configureWriteConfigTask(String name, Project project, Task setup, InstanceInfo node) {
        // Add all node level configs to node Configuration
        return project.tasks.create(name: name, type: DefaultTask, dependsOn: setup) {
            doFirst {
                // Write each config file needed
                node.configFiles.forEach { configFile ->
                    String configName = configFile.getName()
                    Map<String, String> configFileEntries = node.configContents.get(configName)
                    if (configFileEntries == null) {
                        throw new GradleException("Could not find contents of [${configFile}] settings file from deployment options.")
                    }
                    String contents = node.configFileFormatter(configFileEntries)
                    configFile.setText(contents, 'UTF-8')
                }
            }
        }
    }

    static Task configureExtraConfigFilesTask(String name, Project project, Task setup, InstanceInfo node) {
        return setup
    }

    /**
     * Wrapper for command line arguments: surrounds arguments that have commas with double quotes
     */
    private static class EscapeCommaWrapper {
        Object arg

        public String toString() {
            String s = arg.toString()

            /// Surround strings that contains a comma with double quotes
            if (s.indexOf(',') != -1) {
                return "\"${s}\""
            }
            return s
        }
    }

    static Task configureExecTask(String name, Project project, Task setup, InstanceInfo node, Object[] execArgs) {
        return project.tasks.create(name: name, type: LoggedExec, dependsOn: setup) { Exec exec ->
            exec.workingDir node.cwd
            exec.environment 'JAVA_HOME', node.getJavaHome()
            if (Os.isFamily(Os.FAMILY_WINDOWS)) {
                // FIXHERE eventually
                exec.executable 'cmd'
                exec.args '/C', 'call'
                exec.args execArgs.collect { a -> new EscapeCommaWrapper(arg: a)}
            } else {
                exec.commandLine execArgs
            }
        }
    }

    static Task configureStartTask(String name, Project project, Task setup, InstanceInfo node) {
        Task start = project.tasks.create(name: name, type: DefaultTask, dependsOn: setup)
        // FIXHERE Do we need this?
        //if (node.javaVersion != null) {
        //    BuildPlugin.requireJavaHome(start, node.javaVersion)
        //}
        start.doFirst {
            // Configure HADOOP_OPTS (or similar env) - adds system properties, assertion flags, remote debug etc
            String javaOptsEnvKey = node.config.getServiceDescriptor().javaOptsEnvSetting(node.serviceId)
            List<String> javaOpts = [node.env.get(javaOptsEnvKey, '')]
            String collectedSystemProperties = node.config.systemProperties.collect { key, value -> "-D${key}=${value}" }.join(" ")
            javaOpts.add(collectedSystemProperties)
            javaOpts.add(node.config.jvmArgs)
            if (Boolean.parseBoolean(System.getProperty('tests.asserts', 'true'))) {
                // put the enable assertions options before other options to allow
                // flexibility to disable assertions for specific packages or classes
                // in the cluster-specific options
                javaOpts.add("-ea")
                javaOpts.add("-esa")
            }
            // we must add debug options inside the closure so the config is read at execution time, as
            // gradle task options are not processed until the end of the configuration phase
            if (node.config.debug) {
                println "Running ${node.serviceId.roleName} ${node.instance} in debug mode, " +
                        "suspending until connected on port 8000"
                javaOpts.add('-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=8000')
            }
            node.env[javaOptsEnvKey] = javaOpts.join(" ")

            project.logger.info("Starting ${node.serviceId.roleName} in ${node.clusterName}")
        }
        start.doLast {
            // Due to how ant exec works with the spawn option, we lose all stdout/stderr from the
            // process executed. To work around this, when spawning, we wrap the service start
            // command inside another shell script, which simply internally redirects the output
            // of the real start script. This allows ant to keep the streams open with the
            // dummy process, but us to have the output available if there is an error in the
            // services start script
//            if (node.spawn) {
                node.writeWrapperScript()
//            }

            node.getCommandString().eachLine { line -> project.logger.info(line) }

            // this closure is the ant command to be wrapped in our stream redirection and then executed
            Closure antRunner = { AntBuilder ant ->
//                ant.exec(executable: node.executable, spawn: node.spawn, dir: node.cwd, taskName: node.serviceId.roleName) {
                ant.exec(executable: node.executable, spawn: true, dir: node.cwd, taskName: node.serviceId.roleName) {
                    node.env.each { key, value -> env(key: key, value: value) }
                    node.args.each { arg(value: it) }
                }
            }

//            if (project.logger.isInfoEnabled() || node.config.isDaemonized() == false) {
//                runAntCommand(project, antRunner, System.out, System.err)
//            } else {
                // buffer the output, we may not need to print it
                PrintStream captureStream = new PrintStream(node.buffer, true, "UTF-8")
                runAntCommand(project, antRunner, captureStream, captureStream)
//            }
        }
        return start
    }

    static Task configureStopTask(String name, Project project, Object depends, InstanceInfo node) {
        // FIXHERE: Fix the pidDir vs pidFile resolution when we get the daemon tasks to generate pids correctly
        return project.tasks.create(name: name, type: LoggedExec, dependsOn: depends) {
            onlyIf { node.pidFile.exists() }
            // the pid file won't actually be read until execution time, since the read is wrapped within an inner closure of the GString
            ext.pid = "${ -> node.pidFile.getText('UTF-8').trim()}"
            doFirst {
                project.logger.info("Shutting down external service with pid ${pid}")
            }
            if (Os.isFamily(Os.FAMILY_WINDOWS)) {
                executable 'Taskkill'
                args '/PID', pid, '/F'
            } else {
                executable 'kill'
                args '-9', pid
            }
            doLast {
                project.delete(node.pidFile)
            }
        }
    }

    /**
     * Runs an ant command, sending output to the given out and error streams
     */
    static Object runAntCommand(Project project, Closure command, PrintStream outputStream, PrintStream errorStream) {
        DefaultLogger listener = new DefaultLogger(
                errorPrintStream: errorStream,
                outputPrintStream: outputStream,
                messageOutputLevel: org.apache.tools.ant.Project.MSG_INFO)

        project.ant.project.addBuildListener(listener)
        Object retVal = command(project.ant)
        project.ant.project.removeBuildListener(listener)
        return retVal
    }

    /**
     * Generate a unique task name
     * @param prefix cluster prefix
     * @param node the node instance to generate for
     * @param action the base task name
     * @return unique cluster formation task name
     */
    static String taskName(String prefix, InstanceInfo node, String action) {
        if (node.getConfig().getRoleConf().getInstances().size() > 1) {
            return "${prefix}#${node.serviceId.roleName}${node.instance}.${action}"
        } else {
            return "${prefix}#${node.serviceId.roleName}.${action}"
        }
    }

}
