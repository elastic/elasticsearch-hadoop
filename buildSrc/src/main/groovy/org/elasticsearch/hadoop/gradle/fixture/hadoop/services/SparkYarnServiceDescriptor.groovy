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

class SparkYarnServiceDescriptor implements ServiceDescriptor {

    static RoleDescriptor GATEWAY = RoleDescriptor.requiredGateway('spark', [])

    @Override
    String id() {
        return 'spark'
    }

    @Override
    String serviceName() {
        return 'spark'
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
        return new Version(2, 3, 4)
    }

    @Override
    String getDependencyCoordinates(ServiceConfiguration configuration) {
        return "spark:spark-${configuration.getVersion()}:${artifactName(configuration)}@tgz"
    }

    @Override
    String packageName() {
        return 'spark'
    }

    @Override
    String artifactName(ServiceConfiguration configuration) {
        // The spark artifacts that interface with Hadoop have a hadoop version in their names.
        Version version = configuration.getVersion()
        Version hadoopVersion = configuration.getClusterConf().service(HadoopClusterConfiguration.HADOOP).getVersion()
        return "spark-$version-bin-hadoop${hadoopVersion.major}.${hadoopVersion.minor}"
    }

    @Override
    String homeDirName(InstanceConfiguration configuration) {
        return artifactName(configuration.getServiceConf())
    }

    @Override
    String pidFileName(InstanceConfiguration configuration) {
        return 'spark.pid'
    }

    @Override
    String confDirName(InstanceConfiguration configuration) {
        return 'conf'
    }

    @Override
    List<String> configFiles(InstanceConfiguration configuration) {
        return ['spark-defaults.conf']
    }

    @Override
    Map<String, FileSettings> collectConfigFilesContents(InstanceConfiguration configuration) {
        return ['spark-defaults.conf' : configuration.getSettingsContainer().flattenFile('spark-defaults.conf')]
    }

    @Override
    Closure<String> configFormat(InstanceConfiguration configuration) {
        return ConfigFormats.whiteSpaced()
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
        // No start command for gateway services
        return ['']
    }

    @Override
    String scriptDir(InstanceConfiguration instance) {
        return 'bin'
    }

    @Override
    String javaOptsEnvSetting(InstanceConfiguration configuration) {
        return ''
    }

    @Override
    void finalizeEnv(Map<String, String> env, InstanceConfiguration configuration) {
        // Get Hadoop Conf Dir
        InstanceConfiguration hadoopGateway = configuration.getClusterConf()
            .service(HadoopClusterConfiguration.HADOOP)
            .role(HadoopServiceDescriptor.GATEWAY)
            .instance(0)

        ServiceDescriptor hadoopServiceDescriptor = hadoopGateway.getServiceDescriptor()

        File baseDir = hadoopGateway.getBaseDir()
        File homeDir = new File(baseDir, hadoopServiceDescriptor.homeDirName(hadoopGateway))
        File confDir = new File(homeDir, hadoopServiceDescriptor.confDirName(hadoopGateway))

        // HADOOP_CONF_DIR -> ...../etc/hadoop/
        env.put('HADOOP_CONF_DIR', confDir.toString())
    }

    @Override
    Map<String, Object[]> defaultSetupCommands(InstanceConfiguration configuration) {
        return [:]
    }
}
