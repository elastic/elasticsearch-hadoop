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

package org.elasticsearch.hadoop.gradle.fixture.hadoop.conf

import org.elasticsearch.hadoop.gradle.fixture.hadoop.RoleDescriptor
import org.elasticsearch.hadoop.gradle.fixture.hadoop.ServiceDescriptor
import org.gradle.api.Project

/**
 * Configurations for a specific running process, that is of a given role type, belonging
 * to a service within a cluster.
 *
 * End -> Cluster -> Service -> Role -> Instance
 */
class InstanceConfiguration extends ProcessConfiguration {

    private final Project project
    private final RoleConfiguration roleConfiguration
    private final String prefix
    private int instance

    InstanceConfiguration(Project project, RoleConfiguration serviceConfiguration, String prefix, int instance) {
        super(project)
        this.project = project
        this.roleConfiguration = serviceConfiguration
        this.prefix = prefix
        this.instance = instance
    }

    File getBaseDir() {
        String serviceName = getServiceDescriptor().serviceName()
        String roleName = getRoleDescriptor().roleName()
        return new File(project.buildDir, "fixtures/${serviceName}/${prefix}-${roleName}${instance}")
    }

    String getPrefix() {
        return prefix
    }

    int getInstance() {
        return instance
    }

    HadoopClusterConfiguration getClusterConf() {
        return roleConfiguration.getServiceConf().getClusterConf()
    }

    ServiceConfiguration getServiceConf() {
        return roleConfiguration.getServiceConf()
    }

    ServiceDescriptor getServiceDescriptor() {
        return getServiceConf().getServiceDescriptor()
    }

    RoleConfiguration getRoleConf() {
        return roleConfiguration
    }

    RoleDescriptor getRoleDescriptor() {
        return getRoleConf().getRoleDescriptor()
    }

    @Override
    protected ProcessConfiguration parent() {
        return roleConfiguration
    }
}
