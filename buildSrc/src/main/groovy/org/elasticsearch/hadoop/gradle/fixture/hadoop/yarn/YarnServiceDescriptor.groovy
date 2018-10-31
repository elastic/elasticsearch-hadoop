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

package org.elasticsearch.hadoop.gradle.fixture.hadoop.yarn

import org.apache.tools.ant.taskdefs.condition.Os
import org.elasticsearch.gradle.Version
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.HadoopClusterConfiguration
import org.elasticsearch.hadoop.gradle.fixture.hadoop.RoleDescriptor
import org.elasticsearch.hadoop.gradle.fixture.hadoop.ServiceDescriptor
import org.elasticsearch.hadoop.gradle.fixture.hadoop.ServiceIdentifier
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.InstanceConfiguration
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.ServiceConfiguration
import org.elasticsearch.hadoop.gradle.tasks.ApacheMirrorDownload

class YarnServiceDescriptor implements ServiceDescriptor {

    static RoleDescriptor RESOURCEMANAGER = new RoleDescriptor(true, 'resourcemanager', 1, [])
    static RoleDescriptor NODEMANAGER = new RoleDescriptor(true, 'nodemanager', 1, [RESOURCEMANAGER])

    @Override
    String id() {
        return serviceSubGroup()
    }

    @Override
    String serviceName() {
        return "hadoop"
    }

    @Override
    String serviceSubGroup() {
        return "yarn"
    }

    @Override
    List<ServiceDescriptor> serviceDependencies() {
        return [HadoopClusterConfiguration.HDFS]
    }

    @Override
    List<RoleDescriptor> roles() {
        return [RESOURCEMANAGER, NODEMANAGER]
    }

    @Override
    Version defaultVersion() {
        return new Version(2, 7, 7, null, false)
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
    String homeDirName(Version version) {
        return artifactName(version)
    }

    @Override
    String pidFileName(ServiceIdentifier serviceIdentifier) {
        return "${serviceIdentifier.roleName}.pid"
    }

    @Override
    String configPath(ServiceIdentifier instance) {
        if (instance.subGroup == "yarn") {
            return "etc/hadoop"
        }
        throw new UnsupportedOperationException("Unknown instance [${instance}]")
    }

    @Override
    String configFile(ServiceIdentifier serviceIdentifier) {
        return 'yarn-site.xml'
    }

    @Override
    List<String> startCommand(ServiceIdentifier serviceIdentifier) {
        String cmdName = serviceIdentifier.roleName
        if (Os.isFamily(Os.FAMILY_WINDOWS)) {
            return ['yarn.cmd', cmdName]
        } else {
            return ['yarn', cmdName]
        }
    }

    @Override
    String scriptDir(ServiceIdentifier serviceIdentifier) {
        return 'bin'
    }

    @Override
    String javaOptsEnvSetting(ServiceIdentifier serviceIdentifier) {
        if (serviceIdentifier.roleName == "resourcemanager") {
            return "YARN_RESOURCEMANAGER_OPTS"
        } else if (serviceIdentifier.roleName == "nodemanager") {
            return "YARN_NODEMANAGER_OPTS"
        }
        throw new UnsupportedOperationException("Unknown instance [${serviceIdentifier}]")
    }

    @Override
    void finalizeEnv(Map<String, String> env, InstanceConfiguration config, File baseDir) {
//        env.put("YARN_IDENT_STRING", config.getClusterConf().getName())
//        env.put("YARN_PID_DIR", "${new File(baseDir, "run")}")
    }

    @Override
    Map<String, Object[]> defaultSetupCommands(ServiceIdentifier serviceIdentifier) {
        return [:]
    }
}
