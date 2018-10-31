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

package org.elasticsearch.hadoop.gradle.fixture.hadoop.hive

import org.elasticsearch.gradle.Version
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.HadoopClusterConfiguration
import org.elasticsearch.hadoop.gradle.fixture.hadoop.RoleDescriptor
import org.elasticsearch.hadoop.gradle.fixture.hadoop.ServiceDescriptor
import org.elasticsearch.hadoop.gradle.fixture.hadoop.ServiceIdentifier
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.InstanceConfiguration
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.ServiceConfiguration
import org.elasticsearch.hadoop.gradle.fixture.hadoop.hdfs.HdfsServiceDescriptor
import org.elasticsearch.hadoop.gradle.tasks.ApacheMirrorDownload

class HiveServiceDescriptor implements ServiceDescriptor {

    static RoleDescriptor HIVESERVER = RoleDescriptor.requiredProcess('hiveserver')

    @Override
    String id() {
        return serviceName()
    }

    @Override
    String serviceName() {
        return 'hive'
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
        return [HIVESERVER]
    }

    @Override
    Version defaultVersion() {
        return new Version(1, 2, 2, null, false)
    }

    @Override
    void configureDownload(ApacheMirrorDownload task, ServiceConfiguration configuration) {
        Version version = configuration.getVersion()
        task.packagePath = 'hive'
        task.packageName = 'hive'
        task.artifactFileName = "apache-hive-${version}-bin.tar.gz"
        task.version = "${version}"
    }

    @Override
    String packageName() {
        return 'hive'
    }

    @Override
    String artifactName(ServiceConfiguration configuration) {
        Version version = configuration.getVersion()
        return "apache-hive-${version}-bin"
    }

    @Override
    Map<String, String> packageHashVerification(Version version) {
        // FIXHERE only for 1.2.2
        return ['SHA-256': '763b246a1a1ceeb815493d1e5e1d71836b0c5b9be1c4cd9c8d685565113771d1']
    }

    @Override
    String homeDirName(InstanceConfiguration configuration) {
        return artifactName(configuration.getServiceConf())
    }

    @Override
    String pidFileName(ServiceIdentifier service) {
        return 'hive.pid'
    }

    @Override
    String configPath(ServiceIdentifier instance) {
        return 'conf'
    }

    @Override
    String configFile(ServiceIdentifier instance) {
        return 'hive-site.xml'
    }

    @Override
    List<String> startCommand(ServiceIdentifier instance) {
        // We specify the hive root logger to print to console via the hiveconf override.
        // FIXHERE: This might make sense to put in the default settings?
        return ['hiveserver2', '--hiveconf', 'hive.root.logger=INFO,console']
    }

    @Override
    String scriptDir(ServiceIdentifier serviceIdentifier) {
        return 'bin'
    }

    @Override
    String javaOptsEnvSetting(ServiceIdentifier instance) {
        // The jvm that launches Hiveserver2 executes by means of the `hadoop jar` command.
        // Thus, to specify java options to the Hive server, we do so through the same channels
        // that one would use to specify them to any hadoop job.
        return 'HADOOP_OPTS'
    }

    @Override
    void finalizeEnv(Map<String, String> env, InstanceConfiguration configuration, File baseDir) {
        // Need to add HADOOP_HOME to the env. Just use the namenode instance for it.
        ServiceConfiguration hdfsServiceConf = configuration
                .getClusterConf()
                .service(HadoopClusterConfiguration.HDFS.id())

        ServiceDescriptor hdfsServiceDescriptor = hdfsServiceConf.getServiceDescriptor()

        File hadoopBaseDir = hdfsServiceConf
                .role(HdfsServiceDescriptor.NAMENODE_ROLE.roleName())
                .instance(0)
                .getBaseDir()

        File hadoopHome = new File(hadoopBaseDir, hdfsServiceDescriptor.artifactName(hdfsServiceConf))

        env.put('HADOOP_HOME', hadoopHome.toString())
    }

    @Override
    Map<String, Object[]> defaultSetupCommands(ServiceIdentifier instance) {
        return [:] // None for now. Hive may require a schema tool to be run in the future though.
    }
}
