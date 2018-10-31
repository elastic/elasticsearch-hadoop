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

package org.elasticsearch.hadoop.gradle.fixture.hadoop.hdfs

import org.apache.tools.ant.taskdefs.condition.Os
import org.elasticsearch.gradle.Version
import org.elasticsearch.hadoop.gradle.fixture.hadoop.ServiceDescriptor
import org.elasticsearch.hadoop.gradle.fixture.hadoop.RoleDescriptor
import org.elasticsearch.hadoop.gradle.fixture.hadoop.ServiceIdentifier
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.InstanceConfiguration
import org.elasticsearch.hadoop.gradle.tasks.ApacheMirrorDownload

/**
 * Describes behavior, file/setting names, and commands for deploying a local HDFS cluster
 */
class HdfsServiceDescriptor implements ServiceDescriptor {

    static final RoleDescriptor NAMENODE_ROLE = new RoleDescriptor(true, 'namenode', 1, [])
    static final RoleDescriptor DATANODE_ROLE = new RoleDescriptor(true, 'datanode', 1, [NAMENODE_ROLE])

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
        return new Version(2, 7, 7, null, false)
    }

    @Override
    void configureDownload(ApacheMirrorDownload task, Version version) {
        task.packagePath = 'hadoop/common'
        task.packageName = 'hadoop'
        task.artifactFileName = "hadoop-${version}"
        task.version = "${version}"
        task.distribution = 'tar.gz'
    }

    @Override
    String packageName() {
        return 'hadoop'
    }

    @Override
    String artifactName(Version version) {
        return "hadoop-${version}"
    }

    @Override
    Map<String, String> packageHashVerification(Version version) {
        if (version.major == 2 && version.minor == 7 && version.revision == 7) {
            return ['SHA-512': '17c8917211dd4c25f78bf60130a390f9e273b0149737094e45f4ae5c917b1174b97eb90818c5df068e607835120126281bcc07514f38bd7fd3cb8e9d3db1bdde']
        }
    }

    @Override
    String homeDirName(Version version) {
        return artifactName(version)
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
    String configFile(ServiceIdentifier instance) {
        return "hdfs-site.xml"
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
