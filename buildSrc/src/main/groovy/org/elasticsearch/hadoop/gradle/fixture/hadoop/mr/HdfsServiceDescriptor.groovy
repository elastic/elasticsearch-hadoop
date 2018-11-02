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

package org.elasticsearch.hadoop.gradle.fixture.hadoop.mr

import org.apache.tools.ant.taskdefs.condition.Os
import org.elasticsearch.gradle.Version
import org.elasticsearch.hadoop.gradle.fixture.hadoop.ConfigFormats
import org.elasticsearch.hadoop.gradle.fixture.hadoop.ServiceDescriptor
import org.elasticsearch.hadoop.gradle.fixture.hadoop.RoleDescriptor
import org.elasticsearch.hadoop.gradle.fixture.hadoop.ServiceIdentifier
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.InstanceConfiguration
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.ServiceConfiguration
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.SettingsContainer
import org.elasticsearch.hadoop.gradle.tasks.ApacheMirrorDownload

/**
 * Describes behavior, file/setting names, and commands for deploying a local HDFS cluster
 */
class HdfsServiceDescriptor implements ServiceDescriptor {

    static final RoleDescriptor NAMENODE_ROLE = RoleDescriptor.requiredProcess('namenode')
    static final RoleDescriptor DATANODE_ROLE = RoleDescriptor.requiredProcess('datanode', [NAMENODE_ROLE])

    @Override
    String id() {
        return serviceSubGroup()
    }

    @Override
    String serviceName() {
        return 'hadoop'
    }

    @Override
    String serviceSubGroup() {
        return 'hdfs'
    }

    @Override
    List<ServiceDescriptor> serviceDependencies() {
        return []
    }

    @Override
    List<RoleDescriptor> roles() {
        return [NAMENODE_ROLE, DATANODE_ROLE]
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
    String pidFileName(ServiceIdentifier instance) {
        return "${instance.roleName}.pid"
    }

    @Override
    String configPath(ServiceIdentifier instance) {
        if (instance.subGroup == "hdfs") {
            return "etc/hadoop"
        }
        throw new UnsupportedOperationException("Unknown instance [${instance}]")
    }

    @Override
    List<String> configFiles(ServiceIdentifier instance) {
        return ['core-site.xml', 'hdfs-site.xml']
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
        hdfsSite.putIfAbsent('dfs.datanode.name.dir', new File(defaultDataDir, "dfs/data/").toURI().toString())
        hdfsSite.putIfAbsent('dfs.namenode.checkpoint.dir', new File(defaultDataDir, "dfs/namesecondary/").toURI().toString())

        // namenode rpc-address
        hdfsSite.putIfAbsent('dfs.namenode.rpc-address', 'localhost:9000')

        // default permissions to disabled
        hdfsSite.putIfAbsent('dfs.permissions.enabled', 'false')

        files.put('hdfs-site.xml', hdfsSite)

        // core-site.xml:
        Map<String, String> coreSite = container.flattenFile('core-site.xml')

        // default FS settings
        coreSite.putIfAbsent('fs.defaultFS', "hdfs://${hdfsSite.get('dfs.namenode.rpc-address')}")

        files.put('core-site.xml', coreSite)

        return files
    }

    @Override
    Closure<String> configFormat(ServiceIdentifier instance) {
        return ConfigFormats.hadoopXML()
    }

    @Override
    List<String> startCommand(ServiceIdentifier serviceIdentifier) {
        String cmdName = serviceIdentifier.roleName
        if (Os.isFamily(Os.FAMILY_WINDOWS)) {
            return ['hdfs.cmd', cmdName]
        } else {
            return ['hdfs', cmdName]
        }
    }

    @Override
    String scriptDir(ServiceIdentifier serviceIdentifier) {
        return "bin"
    }

    /**
     * The name of the environment variable whose contents are used for the service's java system properties
     */
    @Override
    String javaOptsEnvSetting(ServiceIdentifier instance) {
        if (instance.roleName == "namenode") {
            return "HADOOP_NAMENODE_OPTS"
        } else if (instance.roleName == "datanode") {
            return "HADOOP_DATANODE_OPTS"
        }
        throw new UnsupportedOperationException("Unknown instance [${instance}]")
    }

    @Override
    void finalizeEnv(Map<String, String> env, InstanceConfiguration config, File baseDir) {
//        env.put('HADOOP_USER_NAME', 'hadoop')
//        env.put("HADOOP_IDENT_STRING", config.getClusterConf().getName())
//        env.put("HADOOP_PID_DIR", "${new File(baseDir, "run")}")
    }

    @Override
    Map<String, Object[]> defaultSetupCommands(ServiceIdentifier instance) {
        if (instance.roleName == "namenode") {
            return ["formatNamenode": ["bin/hdfs", "namenode", "-format"].toArray()]
        }
        return [:]
    }
}
