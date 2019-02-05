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
import org.elasticsearch.hadoop.gradle.tasks.ApacheMirrorDownload
import org.gradle.api.GradleException

class PigServiceDescriptor implements ServiceDescriptor {

    static final Map<Version, Map<String, String>> VERSION_MAP = [:]
    static {
        VERSION_MAP.put(new Version(0, 17, 0),
                ['MD5': 'da76998409fe88717b970b45678e00d4'])
    }

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
    void configureDownload(ApacheMirrorDownload task, ServiceConfiguration configuration) {
        task.setPackagePath('pig')
        task.setPackageName('pig')
        task.setVersion(configuration.getVersion().toString())
        task.setArtifactFileName("${artifactName(configuration)}.tar.gz")
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
    Map<String, String> packageHashVerification(Version version) {
        Map<String, String> hashVerifications = VERSION_MAP.get(version)
        if (hashVerifications == null) {
            throw new GradleException("Unsupported version [$version] - No download hash configured")
        }
        return hashVerifications
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
    Map<String, Map<String, String>> collectConfigFilesContents(InstanceConfiguration configuration) {
        return ['pig.properties' : configuration.getSettingsContainer().flattenFile('pig.properties')]
    }

    @Override
    Closure<String> configFormat(InstanceConfiguration configuration) {
        return ConfigFormats.propertyFile()
    }

    @Override
    String httpUri(InstanceConfiguration configuration, Map<String, Map<String, String>> configFileContents) {
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
