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

import org.elasticsearch.gradle.OS
import org.elasticsearch.gradle.Version
import org.elasticsearch.gradle.testclusters.DefaultTestClustersTask
import org.elasticsearch.hadoop.gradle.buildtools.Fixture
import org.elasticsearch.hadoop.gradle.buildtools.FixtureProcesses
import org.elasticsearch.hadoop.gradle.buildtools.LoggedExec
import org.elasticsearch.hadoop.gradle.buildtools.StopFixtureProcess
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.HadoopClusterConfiguration
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.InstanceConfiguration
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.RoleConfiguration
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.ServiceConfiguration
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.artifacts.Configuration
import org.gradle.api.logging.Logger
import org.gradle.api.tasks.Copy
import org.gradle.api.tasks.Delete
import org.gradle.api.tasks.Exec

import java.nio.file.Paths
import java.time.Duration

import static org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.SettingsContainer.FileSettings

/**
 * A helper for creating tasks to build a cluster that is used by a task, and tear down the cluster
 * when the task is finished.
 */
class HadoopClusterFormationTasks {

    /**
     * A start and stop task for a fixture
     */
    static class TaskPair {
        Task startTask
        Task stopTask
    }

    /**
     * Adds dependent tasks to the given task to start and stop a cluster with the given configuration.
     * <p>
     * Returns a list of NodeInfo objects for each node in the cluster.
     */
    static List<InstanceInfo> setup(Project project, HadoopClusterConfiguration clusterConfiguration) {
        String prefix = clusterConfiguration.getName()

        // The cluster wide dependencies
        Object clusterDependencies = clusterConfiguration.getDependencies()

        // Create cluster wide shared dir, just in case we ever need one
        File sharedDir = new File(project.buildDir, "fixtures/shared")

        // First task is to clean the shared directory
        Task cleanShared = project.tasks.create(
            name: "${prefix}#prepareCluster.cleanShared",
            type: Delete,
            group: 'hadoopFixture',
            dependsOn: clusterDependencies) {
                delete sharedDir
                doLast {
                    sharedDir.mkdirs()
                }
        }

        // This is the initial start dependency that the first instance will pick up.
        Object startDependency = cleanShared

        // The complete list of fixture nodes that are a part of a cluster.
        List<InstanceInfo> nodes = []

        // Create the fixtures for each service
        List<TaskPair> clusterTaskPairs = []
        for (ServiceConfiguration serviceConfiguration : clusterConfiguration.getServices()) {

            // Get the download task for this service's package and add it to the service's dependency tasks
            Configuration distributionConfiguration = getOrConfigureDistributionDownload(project, serviceConfiguration)

            // Keep track of the start tasks in this service
            List<TaskPair> serviceTaskPairs = []

            // Create fixtures for each role in the service
            for (RoleConfiguration roleConfiguration : serviceConfiguration.getRoles()) {

                // Keep track of the start tasks in this role
                List<TaskPair> roleTaskPairs = []

                // Create fixtures for each instance in the role
                for (InstanceConfiguration instanceConfiguration : roleConfiguration.getInstances()) {
                    // Every instance depends on its cluster & service & role's start dependencies.
                    // If it's the first instance, it depends on the start dependency (clean shared)
                    // If it's a later instance, it depends on the the preceding instance
                    List<Object> instanceDependencies = []
                    instanceDependencies.addAll(instanceConfiguration.getDependencies())
                    if (clusterTaskPairs.empty) {
                        instanceDependencies.add(startDependency)
                    } else {
                        instanceDependencies.add(clusterTaskPairs.last().startTask)
                    }

                    // Collect the tasks that are depend on the service being up, and are finalized by
                    // the service stopping
                    def instanceClusterTasks = instanceConfiguration.getClusterTasks()

                    // Collect the instance info
                    InstanceInfo instanceInfo = new InstanceInfo(instanceConfiguration, project, prefix, sharedDir)
                    nodes.add(instanceInfo)

                    // Create the tasks for the instance
                    TaskPair instanceTasks
                    try {
                        instanceTasks = configureNode(project, prefix, instanceDependencies, instanceInfo,
                                distributionConfiguration)
                    } catch (Exception e) {
                        throw new GradleException(
                                "Exception occurred while initializing instance [${instanceInfo.toString()}]", e)
                    }

                    // Add the task pair to the task lists
                    roleTaskPairs.add(instanceTasks)
                    serviceTaskPairs.add(instanceTasks)
                    clusterTaskPairs.add(instanceTasks)

                    // Make each cluster task that needs this instance depend on it, and also be finalized by it.
                    instanceClusterTasks.forEach { Task clusterTask ->
                        clusterTask.dependsOn(instanceTasks.startTask)
                        if (instanceTasks.stopTask != null) {
                            clusterTask.finalizedBy(instanceTasks.stopTask)
                        }
                    }

                    // Check to see if any dependencies are Fixtures, and if they are, transfer the Fixture stop tasks
                    // to the cluster task finalized-by tasks.
                    for (Object dependency : instanceConfiguration.getDependencies()) {
                        if (dependency instanceof Fixture) {
                            def depStop = ((Fixture)dependency).stopTask
                            instanceClusterTasks.forEach { Task clusterTask ->
                                clusterTask.finalizedBy(depStop)
                            }
                        }
                    }
                }
                // Make each task in the role depend on and also be finalized by each instance in the service.
                List<Task> startTasks = roleTaskPairs.collect{it.startTask}
                List<Task> stopTasks = roleTaskPairs.findAll{it.stopTask != null}.collect{it.stopTask}
                roleConfiguration.getClusterTasks().forEach { Task clusterTask ->
                    clusterTask.dependsOn(startTasks)
                    if (!stopTasks.isEmpty()) {
                        clusterTask.finalizedBy(stopTasks)
                    }
                }
            }
            // Make each task in the service depend on and also be finalized by each instance in the service.
            List<Task> startTasks = serviceTaskPairs.collect{it.startTask}
            List<Task> stopTasks = serviceTaskPairs.findAll{it.stopTask != null}.collect{it.stopTask}
            serviceConfiguration.getClusterTasks().forEach { Task clusterTask ->
                clusterTask.dependsOn(startTasks)
                if (!stopTasks.isEmpty()) {
                    clusterTask.finalizedBy(stopTasks)
                }
            }
        }
        // Make each task in the cluster depend on and also be finalized by each instance in the cluster.
        List<Task> startTasks = clusterTaskPairs.collect{it.startTask}
        List<Task> stopTasks = clusterTaskPairs.findAll{it.stopTask != null}.collect{it.stopTask}
        clusterConfiguration.getClusterTasks().forEach { Task clusterTask ->
            clusterTask.dependsOn(startTasks)
            if (!stopTasks.isEmpty()) {
                clusterTask.finalizedBy(stopTasks)
            }
        }
        return nodes
    }

    /**
     * Returns the distribution tasks. These contain a download task for this service's packages, which itself is
     * either an already created one from the root project, or a newly created download task. These also contain the
     * verify task to ensure the download has been securely captured.
     */
    static Configuration getOrConfigureDistributionDownload(Project project, ServiceConfiguration serviceConfiguration) {
        Version serviceVersion = serviceConfiguration.getVersion()

        String configurationName = "download${serviceConfiguration.serviceDescriptor.packageName().capitalize()}#${serviceVersion}"
        Configuration configuration = project.configurations.findByName(configurationName)
        if (configuration == null) {
            configuration = project.configurations.create(configurationName)
            project.dependencies.add(configurationName, serviceConfiguration.getServiceDescriptor().getDependencyCoordinates(serviceConfiguration))
        }

        return configuration
    }

    static TaskPair configureNode(Project project, String prefix, Object dependsOn, InstanceInfo node,
                                  Configuration distributionConfiguration) {
        Task setup = project.tasks.create(name: taskName(prefix, node, 'clean'), type: Delete, dependsOn: dependsOn) {
            delete node.homeDir
            delete node.cwd
            group = 'hadoopFixture'
        }

        // Only create CWD and check previous if the role is an executable process
        if (node.getConfig().getRoleDescriptor().isExecutableProcess()) {
            setup = project.tasks.create(name: taskName(prefix, node, 'createCwd'), type: DefaultTask, dependsOn: setup) {
                doLast {
                    node.cwd.mkdirs()
                }
                outputs.dir node.cwd
                group = 'hadoopFixture'
            }
            setup = configureCheckPreviousTask(taskName(prefix, node, 'checkPrevious'), project, setup, node)
            setup = configureStopTask(taskName(prefix, node, 'stopPrevious'), project, setup, node)
        }

        // Always extract the package contents, and configure the files
        setup = configureExtractTask(taskName(prefix, node, 'extract'), project, setup, node, distributionConfiguration)
        setup = configureWriteConfigTask(taskName(prefix, node, 'configure'), project, setup, node)
        setup = configureExtraConfigFilesTask(taskName(prefix, node, 'extraConfig'), project, setup, node)

        // Give the service descriptor a chance to provide service-specific setup tasks
        SetupTaskFactory setupTaskFactory = new SetupTaskFactory(project, { String name -> taskName(prefix, node, name) }, setup)
        node.config.getServiceDescriptor().configureSetupTasks(node.config, setupTaskFactory)
        setup = setupTaskFactory.getLastSetupTask()

        // If the role for this instance is not a process, we skip creating start and stop tasks for it.
        if (!node.getConfig().getRoleDescriptor().isExecutableProcess()) {
            // Go through the given dependencies for the instance and if any of them are Fixtures, pick the stop tasks off
            // and set the instance tasks as finalized by them.
            for (Object dependency : node.config.getDependencies()) {
                if (dependency instanceof Fixture) {
                    def depStop = ((Fixture)dependency).stopTask
                    setup.finalizedBy(depStop)
                }
            }
            return new TaskPair(startTask: setup)
        }

        Map<String, Object[]> setupCommands = new LinkedHashMap<>()
        setupCommands.putAll(node.config.getServiceDescriptor().defaultSetupCommands(node.config))
        setupCommands.putAll(node.config.getSetupCommands())
        for (Map.Entry<String, Object[]> command : setupCommands) {
            // the first argument is the actual script name, relative to home
            Object[] args = command.getValue().clone()
            if (args == null || args.length == 0) {
                throw new GradleException("Empty command line for setup command [$command.key]")
            }
            final Object commandPath
            if (OS.current() == OS.WINDOWS) {
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

        // Configure daemon start task
        Task start = configureStartTask(taskName(prefix, node, 'start'), project, setup, node)

        // Configure wait task
        Task wait = configureWaitTask(taskName(prefix, node, 'wait'), project, node, start, 30)

        // Configure daemon stop task
        Task stop = configureStopTask(taskName(prefix, node, 'stop'), project, [], node)

        // We're running in the background, so make sure that the stop command is called after all cluster tasks finish
        wait.finalizedBy(stop)

        // Go through the given dependencies for the instance and if any of them are Fixtures, pick the stop tasks off
        // and set the instance tasks as finalized by them.
        for (Object dependency : node.config.getDependencies()) {
            if (dependency instanceof Fixture) {
                def depStop = ((Fixture)dependency).stopTask
                wait.finalizedBy(depStop)
                stop.finalizedBy(depStop)
            }
        }
        return new TaskPair(startTask: wait, stopTask: stop)
    }

    static Task configureCheckPreviousTask(String name, Project project, Task setup, InstanceInfo node) {
        // TODO - This is a later item for CI stability
        // This is here for the moment to keep parity with the ES fixture
        return setup
    }

    static Task configureExtractTask(String name, Project project, Task setup, InstanceInfo node, Configuration distributionConfiguration) {
        List extractDependsOn = [distributionConfiguration, setup]
        return project.tasks.create(name: name, type: Copy, dependsOn: extractDependsOn) {
            group = 'hadoopFixture'
            // TODO: Switch logic if a service is ever not a tar distribution
            from {
                project.tarTree(project.resources.gzip(distributionConfiguration.files.first()))
            }
            exclude(node.config.serviceDescriptor.excludeFromArchiveExtraction(node.config))
            into node.baseDir
        }
    }

    static Task configureWriteConfigTask(String name, Project project, Task setup, InstanceInfo node) {
        // Add all node level configs to node Configuration
        return project.tasks.create(name: name, type: DefaultTestClustersTask, dependsOn: setup) {
            group = 'hadoopFixture'
            if (node.elasticsearchCluster != null) {
                useCluster(node.elasticsearchCluster)
            }
            doFirst {
                // Write each config file needed
                node.configFiles.forEach { configFile ->
                    String configName = configFile.getName()
                    FileSettings configFileEntries = node.configContents.get(configName)
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
        // TODO: Extra Config Files
        // We don't really need them at the moment, but might in the future.
        // This is just here to keep parity with the ES fixture
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
            exec.group = 'hadoopFixture'
            exec.workingDir node.cwd
            exec.environment(node.env)

            // Configure HADOOP_OPTS (or similar env) - adds system properties, assertion flags, remote debug etc
            String javaOptsEnvKey = node.config.getServiceDescriptor().javaOptsEnvSetting(node.config)
            if (javaOptsEnvKey != null) {
                List<String> javaOpts = [node.env.get(javaOptsEnvKey, '')]
                String collectedSystemProperties = node.config.systemProperties.collect { key, value -> "-D${key}=${value}" }.join(" ")
                javaOpts.add(collectedSystemProperties)
                javaOpts.add(node.config.jvmArgs)
                exec.environment javaOptsEnvKey, javaOpts.join(" ")
            }

            if (OS.current() == OS.WINDOWS) {
                // TODO Test on Windows
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
        start.group = 'hadoopFixture'
        start.doFirst {
            // Configure HADOOP_OPTS (or similar env) - adds system properties, assertion flags, remote debug etc
            String javaOptsEnvKey = node.config.getServiceDescriptor().javaOptsEnvSetting(node.config)
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
                println "Running ${node.config.roleDescriptor.roleName()} ${node.instance} in debug mode, " +
                        "suspending until connected on port 8000"
                javaOpts.add('-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=8000')
            }
            node.env[javaOptsEnvKey] = javaOpts.join(" ")

            project.logger.info("Starting ${node.config.roleDescriptor.roleName()} in ${node.clusterName}")
        }
        start.doLast {
            if (OS.current() == OS.WINDOWS) {
                // TODO Eventually support Windows
                throw new GradleException('Full test fixtures on Windows are currently unsupported')
            }
            node.getCommandString().eachLine { line -> project.logger.info(line) }

            // The service runs in the background, with its output captured in the start log
            FixtureProcesses.start(node.commandLine, node.cwd, node.env.collectEntries { k, v -> [(k): v.toString()] }, node.startLog, node.pidFile)
        }
        return start
    }

    static Task configureStopTask(String name, Project project, Object depends, InstanceInfo node) {
        return project.tasks.create(name: name, type: StopFixtureProcess, dependsOn: depends) { StopFixtureProcess stop ->
            stop.group = 'hadoopFixture'
            stop.pidFile.set(node.pidFile)
        }
    }

    static Task configureWaitTask(String name, Project project, InstanceInfo instance, Task startTasks, int waitSeconds) {
        Task wait = project.tasks.create(name: name, dependsOn: startTasks)
        wait.doLast {
            long pid = FixtureProcesses.readPidFile(instance.pidFile)
            InetSocketAddress readinessAddress = instance.readinessCheckSocketAddress

            // wait until either the process died or it accepts connections on its readiness address
            boolean ready = FixtureProcesses.waitFor(Duration.ofSeconds(waitSeconds), Duration.ofMillis(500)) {
                FixtureProcesses.isAlive(pid) == false || readinessAddress == null || FixtureProcesses.isListening(readinessAddress)
            }

            if (FixtureProcesses.isAlive(pid) == false) {
                waitFailed(project, instance, project.logger, "Failed to start ${instance.config.roleDescriptor.roleName()}: process exited")
            }
            if (ready == false) {
                waitFailed(project, instance, project.logger, "Failed to start ${instance.config.roleDescriptor.roleName()}: timed out " +
                        "after ${waitSeconds} seconds")
            }

            boolean success = instance.waitCondition.call(instance)
            if (success == false) {
                waitFailed(project, instance, project.logger, 'Hadoop cluster failed to pass wait condition')
            }
        }
        return wait
    }

    static void waitFailed(Project project, InstanceInfo instance, Logger logger, String msg) {
        if (logger.isInfoEnabled() == false) {
            // We already log the command at info level. No need to do it twice.
            instance.getCommandString().eachLine { line -> logger.error(line) }
        }
        logger.error("Node ${instance.config.roleDescriptor.roleName()} ${instance.instance} output:")
        logger.error("|-----------------------------------------")
        logger.error("|  pid file exists: ${instance.pidFile.exists()}")
        // dump the log file for the startup script (which includes the service output to stdout/stderr)
        if (instance.startLog.exists()) {
            logger.error("|\n|  [log]")
            instance.startLog.eachLine { line -> logger.error("|    ${line}") }
        }
        logger.error("|-----------------------------------------")
        throw new GradleException(msg)
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
            return "${prefix}#${node.config.roleDescriptor.roleName()}${node.instance}.${action}"
        } else {
            return "${prefix}#${node.config.roleDescriptor.roleName()}.${action}"
        }
    }

}
