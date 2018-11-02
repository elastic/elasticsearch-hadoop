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
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.HadoopClusterConfiguration
import org.elasticsearch.hadoop.gradle.fixture.hadoop.RoleDescriptor
import org.elasticsearch.hadoop.gradle.fixture.hadoop.ServiceDescriptor
import org.elasticsearch.hadoop.gradle.fixture.hadoop.ServiceIdentifier
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.InstanceConfiguration
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.ServiceConfiguration
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.SettingsContainer
import org.elasticsearch.hadoop.gradle.tasks.ApacheMirrorDownload

class YarnServiceDescriptor implements ServiceDescriptor {

    static RoleDescriptor RESOURCEMANAGER = RoleDescriptor.requiredProcess('resourcemanager')
    static RoleDescriptor NODEMANAGER = RoleDescriptor.requiredProcess('nodemanager', [RESOURCEMANAGER])

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
    List<String> configFiles(ServiceIdentifier serviceIdentifier) {
        return ['core-site.xml', 'yarn-site.xml']
    }

    @Override
    Map<String, Map<String, String>> collectConfigFilesContents(InstanceConfiguration configuration) {
        SettingsContainer container = configuration.getSettingsContainer()

        Map<String, Map<String, String>> files = [:]

        // yarn-site.xml:
        Map<String, String> yarnSite = container.flattenFile('yarn-site.xml')

        files.put('yarn-site.xml', yarnSite)

        // TODO: This setting traditionally lives in core-site.xml, but we only have one config per integration right now
        // core-site.xml:
        Map<String, String> coreSite = container.flattenFile('core-site.xml')

        // default FS settings
        if (!coreSite.containsKey('fs.defaultFS')) {
            // Pick up default FS from namenode settings
            ServiceConfiguration hdfsConf = configuration.getClusterConf().service(HadoopClusterConfiguration.HDFS.id())
            InstanceConfiguration namenodeConf = hdfsConf.role(HdfsServiceDescriptor.NAMENODE_ROLE.roleName()).instance(0)
            ServiceDescriptor hdfs = hdfsConf.getServiceDescriptor()

            Map<String, Map<String, String>> namenodeConfigs = hdfs.collectConfigFilesContents(namenodeConf)

            Map<String, String> namenodeCoreSite = namenodeConfigs.get('core-site.xml')

            coreSite.putIfAbsent('fs.defaultFS', namenodeCoreSite.get('fs.defaultFS'))
        }

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
//        env.put('HADOOP_USER_NAME', 'hadoop')
//        env.put("YARN_IDENT_STRING", config.getClusterConf().getName())
//        env.put("YARN_PID_DIR", "${new File(baseDir, "run")}")
    }

    @Override
    Map<String, Object[]> defaultSetupCommands(ServiceIdentifier serviceIdentifier) {
        return [:]
    }
}
