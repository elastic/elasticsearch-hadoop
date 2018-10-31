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
import org.elasticsearch.hadoop.gradle.tasks.ApacheMirrorDownload

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

    void configureDownload(ApacheMirrorDownload task, ServiceConfiguration configuration)

    String packageName()

    String artifactName(ServiceConfiguration configuration)

    Map<String, String> packageHashVerification(Version version)

    String homeDirName(Version version)

    String pidFileName(ServiceIdentifier service)

    String configPath(ServiceIdentifier instance)

    String configFile(ServiceIdentifier instance)

    List<String> startCommand(ServiceIdentifier instance)

    String scriptDir(ServiceIdentifier serviceIdentifier)

    /**
     * The Environment Variable used to give the underlying process its java options.
     */
    String javaOptsEnvSetting(ServiceIdentifier instance)

    /**
     * Finalize the environment variables for a given instance.
     */
    void finalizeEnv(Map<String, String> env, InstanceConfiguration configuration, File baseDir)

    Map<String, Object[]> defaultSetupCommands(ServiceIdentifier instance)
}