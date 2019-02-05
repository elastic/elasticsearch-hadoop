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

import org.elasticsearch.gradle.Version
import org.elasticsearch.hadoop.gradle.fixture.hadoop.RoleDescriptor
import org.elasticsearch.hadoop.gradle.fixture.hadoop.ServiceDescriptor
import org.gradle.api.GradleException
import org.gradle.api.Project
import org.gradle.util.ConfigureUtil

/**
 * Handles configurations for a sub-cluster of services within the larger Hadoop cluster.
 *
 * A service in this context is the collection of programs that make up a self contained
 * system. HDFS and YARN can be considered services.
 *
 * End -> Cluster -> Service -> Role -> Instance
 */
class ServiceConfiguration extends ProcessConfiguration {

    private final Project project
    private final HadoopClusterConfiguration clusterConfiguration
    private final String prefix
    private final ServiceDescriptor serviceDescriptor
    private final List<ServiceConfiguration> dependentServices

    private final Map<String, RoleDescriptor> supportedRoles
    private final Map<String, RoleConfiguration> roleConfigurations
    private final List<RoleConfiguration> roleDeploymentOrder

    private Version version

    ServiceConfiguration(Project project, HadoopClusterConfiguration clusterConfiguration, String prefix, ServiceDescriptor serviceDescriptor, List<ServiceConfiguration> dependentServices) {
        super(project)
        this.project = project
        this.clusterConfiguration = clusterConfiguration
        this.prefix = prefix
        this.serviceDescriptor = serviceDescriptor
        this.dependentServices = dependentServices
        this.supportedRoles = serviceDescriptor.roles().collectEntries { RoleDescriptor role ->
            [(role.roleName()): role]
        }
        this.roleConfigurations = [:]
        this.roleDeploymentOrder = []
        this.version = serviceDescriptor.defaultVersion()
        initializeRequiredRoles()
    }

    private void initializeRequiredRoles() {
        for (RoleDescriptor roleDescriptor : serviceDescriptor.roles()) {
            if (roleDescriptor.required()) {
                role(roleDescriptor)
            }
        }
    }

    RoleConfiguration role(String roleName) {
        return role(roleName, null)
    }

    RoleConfiguration role(String roleName, Closure<?> configure) {
        RoleDescriptor roleDescriptor = supportedRoles.get(roleName)

        if (roleDescriptor == null) {
            throw new GradleException("${serviceDescriptor.serviceName()} does not support unknown role [${roleName}]")
        }

        return doGetRoleConf(roleDescriptor, configure)
    }

    RoleConfiguration role(RoleDescriptor roleDescriptor) {
        if (!supportedRoles.containsKey(roleDescriptor.roleName())) {
            throw new GradleException("${serviceDescriptor.serviceName()} does not support unknown role [${roleDescriptor.roleName()}]")
        }
        return doGetRoleConf(roleDescriptor, null)
    }

    private RoleConfiguration doGetRoleConf(RoleDescriptor roleDescriptor, Closure<?> configure) {
        String roleName = roleDescriptor.roleName()
        RoleConfiguration roleConf = roleConfigurations.get(roleName)
        if (roleConf != null) {
            if (configure != null) {
                ConfigureUtil.configure(configure, roleConf)
            }
            return roleConf
        }

        // Create the service conf and its dependent service confs

        // Ensure that the dependent services already exist
        List<RoleConfiguration> dependentRoles = []
        for (RoleDescriptor dependentRole : roleDescriptor.dependentRoles()) {
            RoleConfiguration conf = role(dependentRole)
            dependentRoles.add(conf)
        }

        roleConf = new RoleConfiguration(project, this, prefix, roleDescriptor, dependentRoles)
        roleConfigurations.put(roleName, roleConf)
        roleDeploymentOrder.add(roleConf)

        if (configure != null) {
            ConfigureUtil.configure(configure, roleConf)
        }

        return roleConf
    }

    List<ServiceConfiguration> getDependentServices() {
        return dependentServices
    }

    List<RoleConfiguration> getRoles() {
        return roleDeploymentOrder
    }

    ServiceDescriptor getServiceDescriptor() {
        return serviceDescriptor
    }

    HadoopClusterConfiguration getClusterConf() {
        return clusterConfiguration
    }

    Version getVersion() {
        return version
    }

    void setVersion(Version version) {
        this.version = version
    }

    @Override
    protected ProcessConfiguration parent() {
        return clusterConfiguration
    }
}
