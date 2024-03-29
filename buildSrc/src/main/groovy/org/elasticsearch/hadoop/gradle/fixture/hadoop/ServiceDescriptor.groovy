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

import org.elasticsearch.gradle.Version
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.InstanceConfiguration
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.ServiceConfiguration
import org.gradle.api.Task

import static org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.SettingsContainer.FileSettings

/**
 * Describes deployment characteristics for different Hadoop ecosystem projects.
 *
 * Used to select different behaviors or property names when standing up or tearing down
 * a cluster of diverse services (Even HDFS and YARN use very different properties).
 */
interface ServiceDescriptor {

    /**
     * The id of this service, usually the name.
     */
    String id()

    /**
     * The name of this service.
     */
    String serviceName()

    /**
     * The list of services that must be stood up in order for this service to operate.
     */
    List<ServiceDescriptor> serviceDependencies()

    /**
     * The list of roles that is supported by the service.
     */
    List<RoleDescriptor> roles()

    /**
     * The default supported version of the service (may be overridden in the config though).
     */
    Version defaultVersion()

    /**
     * The coordinates for this dependency that will be used with a custom Ivy Repository to download the artifact from
     * an Apache mirror.
     */
    String getDependencyCoordinates(ServiceConfiguration configuration)

    /**
     * The official apache package name for the artifact.
     */
    String packageName()

    /**
     * The name of the artifact that will be downloaded.
     */
    String artifactName(ServiceConfiguration configuration)

    /**
     * Returns a collection of directories/files that can be excluded when extracting an archive. Some distributions
     * include entire directories full of things we do not need in the fixture like documentation and example code.
     */
    Collection<String> excludeFromArchiveExtraction(InstanceConfiguration configuration)

    /**
     * The name of the directory under the base dir that contains the package contents.
     */
    String homeDirName(InstanceConfiguration configuration)

    /**
     * The name of the file to use to store the process pid when it starts up.
     */
    String pidFileName(InstanceConfiguration configuration)

    /**
     * The name of the directory under the home dir that contains the configurations.
     */
    String confDirName(InstanceConfiguration configuration)

    /**
     * Returns the list of configuration files that need to be provided to the process.
     */
    List<String> configFiles(InstanceConfiguration configuration)

    /**
     * Collect all configuration entries, setting defaults for the service, role, and instance.
     */
    Map<String, FileSettings> collectConfigFilesContents(InstanceConfiguration configuration)

    /**
     * Closure that formats a configuration map into a String for the config file contents.
     */
    Closure<String> configFormat(InstanceConfiguration configuration)

    /**
     * Produces the HTTP/S URI to reach the web front end for a running instance, or null if there is no web interface.
     */
    String httpUri(InstanceConfiguration configuration, Map<String, FileSettings> configFileContents)

    /**
     * The command line to use for starting the given role and instance.
     */
    List<String> startCommand(InstanceConfiguration configuration)

    /**
     * The name of the directory under the home dir that contains the run scripts.
     */
    String scriptDir(InstanceConfiguration instance)

    /**
     * The Environment Variable used to give the underlying process its java options.
     */
    String javaOptsEnvSetting(InstanceConfiguration configuration)

    /**
     * Finalize the environment variables for a given instance.
     */
    void finalizeEnv(Map<String, String> env, InstanceConfiguration configuration)

    /**
     * Configure a list of Gradle tasks that are specific to setting up this service. If a setup step needs
     * to run scripts that are provided by the service installation's bin directory, use defaultSetupCommands
     * instead.
     */
    void configureSetupTasks(InstanceConfiguration configuration, SetupTaskFactory taskFactory)

    /**
     * A map of default setup commands to run for an instance. Each entry's key is the taskName to use for the
     * exec command, and value is the command line to execute. Every command is resolved against the instance's
     * home directory. If you need to perform set up operations with Gradle tasks, use configureSetupTasks.
     */
    Map<String, Object[]> defaultSetupCommands(InstanceConfiguration configuration)

    /**
     * This is a host/port that can be used to check if an instance of the service is ready to be used.
     * @param configuration The configuration of the instance we want to get the socket address for
     * @return An InetSocketAddress that can be used to check if the instance whose configuration is given is ready to be used
     */
    InetSocketAddress readinessCheckHostAndPort(InstanceConfiguration configuration)
}