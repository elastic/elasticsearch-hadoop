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

package org.elasticsearch.hadoop.gradle.fixture.hadoop.pig

import org.elasticsearch.gradle.Version
import org.elasticsearch.hadoop.gradle.fixture.hadoop.RoleDescriptor
import org.elasticsearch.hadoop.gradle.fixture.hadoop.ServiceDescriptor
import org.elasticsearch.hadoop.gradle.fixture.hadoop.ServiceIdentifier
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.HadoopClusterConfiguration
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.InstanceConfiguration
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.ServiceConfiguration
import org.elasticsearch.hadoop.gradle.tasks.ApacheMirrorDownload

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
    String serviceSubGroup() {
        return null
    }

    @Override
    List<ServiceDescriptor> serviceDependencies() {
        return [HadoopClusterConfiguration.HDFS, HadoopClusterConfiguration.YARN]
    }

    @Override
    List<RoleDescriptor> roles() {
        return [GATEWAY]
    }

    @Override
    Version defaultVersion() {
        return new Version(0, 17, 0, null, false)
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
        return ['MD5': 'da76998409fe88717b970b45678e00d4']
    }

    @Override
    String homeDirName(InstanceConfiguration configuration) {
        return artifactName(configuration.getServiceConf())
    }

    @Override
    String pidFileName(ServiceIdentifier service) {
        return 'pig.pid' // Not needed for gateway
    }

    @Override
    String configPath(ServiceIdentifier instance) {
        return 'conf'
    }

    @Override
    String configFile(ServiceIdentifier instance) {
        return 'pig.properties'
    }

    @Override
    List<String> startCommand(ServiceIdentifier instance) {
        return ['']
    }

    @Override
    String scriptDir(ServiceIdentifier serviceIdentifier) {
        return ''
    }

    @Override
    String javaOptsEnvSetting(ServiceIdentifier instance) {
        return 'PIG_OPTS' // Only used when launching pig scripts
    }

    @Override
    void finalizeEnv(Map<String, String> env, InstanceConfiguration configuration, File baseDir) {
        // see bin/pig for env options
        env.put('PIG_HOME', new File(configuration.baseDir, homeDirName(configuration)).toString())
    }

    @Override
    Map<String, Object[]> defaultSetupCommands(ServiceIdentifier instance) {
        return [:]
    }
}
