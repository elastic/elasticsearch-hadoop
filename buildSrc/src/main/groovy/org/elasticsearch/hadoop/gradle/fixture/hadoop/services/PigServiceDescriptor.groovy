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

package org.elasticsearch.hadoop.gradle.fixture.hadoop.services

import org.elasticsearch.gradle.Version
import org.elasticsearch.hadoop.gradle.fixture.hadoop.ConfigFormats
import org.elasticsearch.hadoop.gradle.fixture.hadoop.RoleDescriptor
import org.elasticsearch.hadoop.gradle.fixture.hadoop.ServiceDescriptor
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.HadoopClusterConfiguration
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.InstanceConfiguration
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.ServiceConfiguration

import static org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.SettingsContainer.FileSettings

class PigServiceDescriptor implements ServiceDescriptor {

    static RoleDescriptor GATEWAY = RoleDescriptor.requiredGateway('pig', [])

    @Override
    String id() {
        return serviceName()
    }

    @Override
    String serviceName() {
        return 'pig'
    }

    @Override
    List<ServiceDescriptor> serviceDependencies() {
        return [HadoopClusterConfiguration.HADOOP]
    }

    @Override
    List<RoleDescriptor> roles() {
        return [GATEWAY]
    }

    @Override
    Version defaultVersion() {
        return new Version(0, 17, 0)
    }

    @Override
    String getDependencyCoordinates(ServiceConfiguration configuration) {
        return "pig:pig-${configuration.getVersion()}:${artifactName(configuration)}@tar.gz"
    }

    @Override
    String packageName() {
        return 'pig'
    }

    @Override
    String artifactName(ServiceConfiguration configuration) {
        return "pig-${configuration.getVersion()}"
    }

    @Override
    String homeDirName(InstanceConfiguration configuration) {
        return artifactName(configuration.getServiceConf())
    }

    @Override
    String pidFileName(InstanceConfiguration configuration) {
        return 'pig.pid' // Not needed for gateway
    }

    @Override
    String confDirName(InstanceConfiguration configuration) {
        return 'conf'
    }

    @Override
    List<String> configFiles(InstanceConfiguration configuration) {
        return ['pig.properties']
    }

    @Override
    Map<String, FileSettings> collectConfigFilesContents(InstanceConfiguration configuration) {
        return ['pig.properties' : configuration.getSettingsContainer().flattenFile('pig.properties')]
    }

    @Override
    Closure<String> configFormat(InstanceConfiguration configuration) {
        return ConfigFormats.propertyFile()
    }

    @Override
    String httpUri(InstanceConfiguration configuration, Map<String, FileSettings> configFileContents) {
        if (GATEWAY.equals(configuration.roleDescriptor)) {
            return null
        }
        throw new UnsupportedOperationException("Unknown instance [${configuration.roleDescriptor.roleName()}]")
    }

    @Override
    List<String> startCommand(InstanceConfiguration configuration) {
        return ['']
    }

    @Override
    String scriptDir(InstanceConfiguration instance) {
        return 'bin'
    }

    @Override
    String javaOptsEnvSetting(InstanceConfiguration configuration) {
        return 'PIG_OPTS' // Only used when launching pig scripts
    }

    @Override
    void finalizeEnv(Map<String, String> env, InstanceConfiguration configuration) {
        // see bin/pig for env options
        File pigHome = new File(configuration.baseDir, homeDirName(configuration))
        env.put('PIG_HOME', pigHome.toString())
        env.put('PIG_LOG_DIR', new File(pigHome, 'logs').toString())

        // Add HADOOP_HOME for
        InstanceConfiguration hadoopGateway = configuration
                .getClusterConf()
                .service(HadoopClusterConfiguration.HADOOP)
                .role(HadoopServiceDescriptor.GATEWAY)
                .instance(0)

        ServiceDescriptor hadoopServiceDescriptor = hadoopGateway.getServiceDescriptor()

        File hadoopBaseDir = hadoopGateway.getBaseDir()
        File hadoopHomeDir = new File(hadoopBaseDir, hadoopServiceDescriptor.homeDirName(hadoopGateway))
        File hadoopConfDir = new File(hadoopHomeDir, hadoopServiceDescriptor.confDirName(hadoopGateway))

        env.put('HADOOP_HOME', hadoopHomeDir.toString())
        env.put('HADOOP_CONF_DIR', hadoopConfDir.toString())
    }

    @Override
    Map<String, Object[]> defaultSetupCommands(InstanceConfiguration configuration) {
        return [:]
    }
}
