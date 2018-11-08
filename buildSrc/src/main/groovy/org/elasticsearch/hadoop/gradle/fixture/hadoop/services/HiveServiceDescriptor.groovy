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
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.HadoopClusterConfiguration
import org.elasticsearch.hadoop.gradle.fixture.hadoop.RoleDescriptor
import org.elasticsearch.hadoop.gradle.fixture.hadoop.ServiceDescriptor
import org.elasticsearch.hadoop.gradle.fixture.hadoop.ServiceIdentifier
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.InstanceConfiguration
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.ServiceConfiguration
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
        return [HadoopClusterConfiguration.HADOOP]
    }

    @Override
    List<RoleDescriptor> roles() {
        return [HIVESERVER]
    }

    @Override
    Version defaultVersion() {
        return new Version(1, 2, 2)
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
    List<String> configFiles(ServiceIdentifier instance) {
        return ['hive-site.xml']
    }

    @Override
    Map<String, Map<String, String>> collectConfigFilesContents(InstanceConfiguration configuration) {
        Map<String, String> hiveSite = configuration.getSettingsContainer().flattenFile('hive-site.xml')
//        hiveSite.putIfAbsent('hadoop.proxyuser.hive.groups', '*')
//        hiveSite.putIfAbsent('hadoop.proxyuser.hive.hosts', '*')
        return ['hive-site.xml' : hiveSite]
    }

    @Override
    Closure<String> configFormat(ServiceIdentifier instance) {
        return ConfigFormats.hadoopXML()
    }

    @Override
    List<String> startCommand(ServiceIdentifier instance) {
        // We specify the hive root logger to print to console via the hiveconf override.
        // FIXHERE: This might make sense to put in the default settings?
        return ['hiveserver2', '--hiveconf', 'hive.root.logger=INFO,console']
    }

    @Override
    String scriptDir(InstanceConfiguration instance) {
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
    void finalizeEnv(Map<String, String> env, InstanceConfiguration configuration) {
        // Need to add HADOOP_HOME to the env. Just use the namenode instance for it.
        InstanceConfiguration namenodeConfiguration = configuration
                .getClusterConf()
                .service(HadoopClusterConfiguration.HADOOP.id())
                .role(HadoopServiceDescriptor.NAMENODE.roleName())
                .instance(0)

        ServiceDescriptor hdfsServiceDescriptor = namenodeConfiguration.getServiceDescriptor()

        File hadoopBaseDir = namenodeConfiguration.getBaseDir()
        String homeDirName = hdfsServiceDescriptor.homeDirName(namenodeConfiguration)
        File hadoopHome = new File(hadoopBaseDir, homeDirName)
        env.put('HADOOP_HOME', hadoopHome.toString())

//        env.put('HADOOP_USER_NAME', 'hadoop')
    }

    @Override
    Map<String, Object[]> defaultSetupCommands(ServiceIdentifier instance) {
        //FIXHERE: Need a tmp dir accessible to all in HDFS
        return [:] // None for now. Hive may require a schema tool to be run in the future though.
    }
}
