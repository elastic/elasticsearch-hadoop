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

/**
 * Describes deployment characteristics for different Hadoop ecosystem projects.
 *
 * Used to select different behaviors or property names when standing up or tearing down
 * a cluster of diverse services (Even HDFS and YARN use very different properties)
 */
interface ServiceDescriptor {

    String id()

    String serviceName()

    String serviceSubGroup()

    List<ServiceDescriptor> serviceDependencies()

    List<RoleDescriptor> roles()

    Version defaultVersion()

    String packageName()

    String packagePath()

    String packageDistro()

    String packageSha512(Version version)

    String pidFileName(ServiceIdentifier service)

    String configPath(ServiceIdentifier instance)

    String configFile(ServiceIdentifier instance)

    List<String> startCommand(ServiceIdentifier instance)

    List<String> daemonStartCommand(ServiceIdentifier serviceIdentifier)

    List<String> daemonStopCommand(ServiceIdentifier serviceIdentifier)

    String daemonScriptDir(ServiceIdentifier serviceIdentifier)

    boolean wrapScript(ServiceIdentifier instance)

    String envIdentString(ServiceIdentifier instance)

    String scriptDir(ServiceIdentifier serviceIdentifier)

    String pidFileEnvSetting(ServiceIdentifier instance)

    String javaOptsEnvSetting(ServiceIdentifier instance)

    Map<String, Object[]> defaultSetupCommands(ServiceIdentifier instance)
}