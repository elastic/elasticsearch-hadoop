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
import org.elasticsearch.hadoop.gradle.fixture.hadoop.HadoopClusterConfiguration
import org.elasticsearch.hadoop.gradle.fixture.hadoop.RoleDescriptor
import org.elasticsearch.hadoop.gradle.fixture.hadoop.ServiceDescriptor
import org.elasticsearch.hadoop.gradle.fixture.hadoop.ServiceIdentifier

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
    String packageName() {
        return 'hadoop'
    }

    @Override
    String packagePath() {
        return 'hadoop/common'
    }

    @Override
    String packageDistro() {
        return 'tar.gz'
    }

    @Override
    String packageSha512(Version version) {
        if (version.major == 2 && version.minor == 7 && version.revision == 7) {
            return '17c8917211dd4c25f78bf60130a390f9e273b0149737094e45f4ae5c917b1174b97eb90818c5df068e607835120126281bcc07514f38bd7fd3cb8e9d3db1bdde'
        }
    }

    @Override
    String pidFileName(ServiceIdentifier serviceIdentifier) {
        return "hadoop-james.baiera-${serviceIdentifier.roleName}.pid"
    }

    @Override
    String configPath(ServiceIdentifier serviceIdentifier) {
        if (serviceIdentifier.subGroup == "yarn") {
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
    List<String> daemonStartCommand(ServiceIdentifier serviceIdentifier) {
        String cmdName = serviceIdentifier.roleName
        if (Os.isFamily(Os.FAMILY_WINDOWS)) {
            return ['yarn.cmd', cmdName]
        } else {
            return ['yarn-daemon.sh', 'start', cmdName]
        }
    }

    @Override
    List<String> daemonStopCommand(ServiceIdentifier serviceIdentifier) {
        String cmdName = serviceIdentifier.roleName
        if (Os.isFamily(Os.FAMILY_WINDOWS)) {
            return null
        } else {
            return ['yarn-daemon.sh', 'stop', cmdName]
        }
    }

    @Override
    String daemonScriptDir(ServiceIdentifier serviceIdentifier) {
        return 'sbin'
    }

    @Override
    boolean wrapScript(ServiceIdentifier serviceIdentifier) {
        return false
    }

    @Override
    String envIdentString(ServiceIdentifier serviceIdentifier) {
        return 'YARN_IDENT_STRING'
    }

    @Override
    String scriptDir(ServiceIdentifier serviceIdentifier) {
        return 'bin'
    }

    @Override
    String pidFileEnvSetting(ServiceIdentifier serviceIdentifier) {
        return 'YARN_PID_DIR'
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
    Map<String, Object[]> defaultSetupCommands(ServiceIdentifier serviceIdentifier) {
        return [:]
    }
}
