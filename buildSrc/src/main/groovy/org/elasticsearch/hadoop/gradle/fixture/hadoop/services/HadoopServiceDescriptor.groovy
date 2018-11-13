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

import org.apache.tools.ant.taskdefs.condition.Os
import org.elasticsearch.gradle.Version
import org.elasticsearch.hadoop.gradle.fixture.hadoop.ConfigFormats
import org.elasticsearch.hadoop.gradle.fixture.hadoop.RoleDescriptor
import org.elasticsearch.hadoop.gradle.fixture.hadoop.ServiceDescriptor
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.InstanceConfiguration
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.ServiceConfiguration
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.SettingsContainer
import org.elasticsearch.hadoop.gradle.tasks.ApacheMirrorDownload

class HadoopServiceDescriptor implements ServiceDescriptor {

    static final RoleDescriptor NAMENODE = RoleDescriptor.requiredProcess('namenode')
    static final RoleDescriptor DATANODE = RoleDescriptor.requiredProcess('datanode', [NAMENODE])
    static final RoleDescriptor RESOURCEMANAGER = RoleDescriptor.requiredProcess('resourcemanager')
    static final RoleDescriptor NODEMANAGER = RoleDescriptor.requiredProcess('nodemanager', [RESOURCEMANAGER])
    static final RoleDescriptor GATEWAY = RoleDescriptor.requiredGateway('hadoop', [NAMENODE, DATANODE, RESOURCEMANAGER, NODEMANAGER])

    @Override
    String id() {
        return serviceName()
    }

    @Override
    String serviceName() {
        return 'hadoop'
    }

    @Override
    List<ServiceDescriptor> serviceDependencies() {
        return []
    }

    @Override
    List<RoleDescriptor> roles() {
        return [NAMENODE, DATANODE, RESOURCEMANAGER, NODEMANAGER, GATEWAY]
    }

    @Override
    Version defaultVersion() {
        return new Version(2, 7, 7)
    }

    @Override
    void configureDownload(ApacheMirrorDownload task, ServiceConfiguration configuration) {
        Version version = configuration.getVersion()
        task.packagePath = 'hadoop/common'
        task.packageName = 'hadoop'
        task.artifactFileName = "hadoop-${version}.tar.gz"
        task.version = "${version}"
    }

    @Override
    String packageName() {
        return 'hadoop'
    }

    @Override
    String artifactName(ServiceConfiguration configuration) {
        Version version = configuration.getVersion()
        return "hadoop-${version}"
    }

    @Override
    Map<String, String> packageHashVerification(Version version) {
        if (version.major == 2 && version.minor == 7 && version.revision == 7) {
            return ['SHA-512': '17c8917211dd4c25f78bf60130a390f9e273b0149737094e45f4ae5c917b1174b97eb90818c5df068e607835120126281bcc07514f38bd7fd3cb8e9d3db1bdde']
        }
    }

    @Override
    String homeDirName(InstanceConfiguration configuration) {
        return artifactName(configuration.getServiceConf())
    }

    @Override
    String pidFileName(InstanceConfiguration configuration) {
        return "${configuration.roleDescriptor.roleName()}.pid"
    }

    @Override
    String confDirName(InstanceConfiguration configuration) {
        return "etc/hadoop"
    }

    @Override
    List<String> configFiles(InstanceConfiguration configuration) {
        return ['core-site.xml', 'hdfs-site.xml', 'yarn-site.xml']
    }

    @Override
    Map<String, Map<String, String>> collectConfigFilesContents(InstanceConfiguration configuration) {
        SettingsContainer container = configuration.getSettingsContainer()
        Map<String, Map<String, String>> files = [:]

        // hdfs-site.xml:
        Map<String, String> hdfsSite = container.flattenFile('hdfs-site.xml')

        // default replication should be 1
        hdfsSite.putIfAbsent('dfs.replication', '1')

        // data directories
        File defaultDataDir = new File(new File(configuration.getBaseDir(), homeDirName(configuration)), 'data')
        hdfsSite.putIfAbsent('dfs.namenode.name.dir', new File(defaultDataDir, "dfs/name/").toURI().toString())
        hdfsSite.putIfAbsent('dfs.datanode.data.dir', new File(defaultDataDir, "dfs/data/").toURI().toString())
        hdfsSite.putIfAbsent('dfs.namenode.checkpoint.dir', new File(defaultDataDir, "dfs/namesecondary/").toURI().toString())

        // namenode rpc-address
        hdfsSite.putIfAbsent('dfs.namenode.rpc-address', 'localhost:9000')

        // default permissions to disabled
        hdfsSite.putIfAbsent('dfs.permissions.enabled', 'false')

        files.put('hdfs-site.xml', hdfsSite)

        // yarn-site.xml:
        Map<String, String> yarnSite = container.flattenFile('yarn-site.xml')

        files.put('yarn-site.xml', yarnSite)

        // core-site.xml:
        Map<String, String> coreSite = container.flattenFile('core-site.xml')

        // default FS settings
        coreSite.putIfAbsent('fs.defaultFS', "hdfs://${hdfsSite.get('dfs.namenode.rpc-address')}")

        files.put('core-site.xml', coreSite)

        return files
    }

    @Override
    Closure<String> configFormat(InstanceConfiguration configuration) {
        return ConfigFormats.hadoopXML()
    }

    @Override
    List<String> startCommand(InstanceConfiguration configuration) {
        RoleDescriptor role = configuration.roleDescriptor
        if (NAMENODE.equals(role) || DATANODE.equals(role)) {
            if (Os.isFamily(Os.FAMILY_WINDOWS)) {
                return ['hdfs.cmd', role.roleName()]
            } else {
                return ['hdfs', role.roleName()]
            }
        } else if (RESOURCEMANAGER.equals(role) || NODEMANAGER.equals(role)) {
            if (Os.isFamily(Os.FAMILY_WINDOWS)) {
                return ['yarn.cmd', role.roleName()]
            } else {
                return ['yarn', role.roleName()]
            }
        } else if (GATEWAY.equals(role)) {
            return [""]
        }
        throw new UnsupportedOperationException("Unknown instance [${role.roleName()}]")
    }

    @Override
    String scriptDir(InstanceConfiguration instance) {
        return 'bin'
    }

    @Override
    String javaOptsEnvSetting(InstanceConfiguration configuration) {
        if (configuration.roleDescriptor == NAMENODE) {
            return "HADOOP_NAMENODE_OPTS"
        } else if (configuration.roleDescriptor == DATANODE) {
            return "HADOOP_DATANODE_OPTS"
        } else if (configuration.roleDescriptor == RESOURCEMANAGER) {
            return "YARN_RESOURCEMANAGER_OPTS"
        } else if (configuration.roleDescriptor == NODEMANAGER) {
            return "YARN_NODEMANAGER_OPTS"
        }
        throw new UnsupportedOperationException("Unknown instance [${configuration.roleDescriptor.roleName()}]")
    }

    @Override
    void finalizeEnv(Map<String, String> env, InstanceConfiguration configuration) {

    }

    @Override
    Map<String, Object[]> defaultSetupCommands(InstanceConfiguration configuration) {
        if (configuration.roleDescriptor == NAMENODE) {
            return ["formatNamenode": ["bin/hdfs", "namenode", "-format"].toArray()]
        }
        return [:]
    }
}
