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
import org.gradle.api.GradleException
import org.gradle.api.GradleScriptException
import org.gradle.api.Project
import org.gradle.util.ConfigureUtil

/**
 * Shared configurations for all instances of a role within a Hadoop service.
 *
 * Roles in this context are the different types of programs that run as part
 * of a service. Datanodes and Namenodes are examples of roles in the HDFS service.
 *
 * End -> Cluster -> Service -> Role -> Instance
 */
class RoleConfiguration extends ProcessConfiguration {

    private final Project project
    private final ServiceConfiguration serviceConfiguration
    private final String prefix
    private final RoleDescriptor roleDescriptor
    private final List<RoleConfiguration> dependentRoles
    private final List<InstanceConfiguration> instances

    RoleConfiguration(Project project, ServiceConfiguration serviceConfiguration, String prefix, RoleDescriptor roleDescriptor, List<RoleConfiguration> dependentRoles) {
        super(project)
        this.project = project
        this.serviceConfiguration = serviceConfiguration
        this.prefix = prefix
        this.roleDescriptor = roleDescriptor
        this.dependentRoles = dependentRoles
        this.instances = []
        initializeDefaultInstances()
    }

    void initializeDefaultInstances() {
        int defaultInstances = roleDescriptor.defaultInstances()
        for (int instanceNumber = 0; instanceNumber < defaultInstances; instanceNumber++) {
            instance(instanceNumber)
        }
    }

    InstanceConfiguration instance(int instanceNumber) {
        return instance(instanceNumber, null)
    }

    InstanceConfiguration instance(int instanceNumber, Closure<?> configure) {
        if (instances.size() > instanceNumber) {
            InstanceConfiguration instanceConf = instances.get(instanceNumber)
            if (configure != null) {
                ConfigureUtil.configure(configure, instanceConf)
            }
            return instanceConf
        }

        // Grow the list of instances up to the given instance number
        int oldInstanceListSize = instances.size()
        for (int newInstanceNumber = oldInstanceListSize; newInstanceNumber <= instanceNumber; newInstanceNumber++) {
            InstanceConfiguration instanceConfig = new InstanceConfiguration(project, this, prefix, newInstanceNumber)
            instances.add(instanceConfig)
        }

        InstanceConfiguration instanceConf = instances.get(instanceNumber)

        if (configure != null) {
            ConfigureUtil.configure(configure, instanceConf)
        }

        return instanceConf
    }

    List<RoleConfiguration> getDependentRoles() {
        return dependentRoles
    }

    void setInstances(int desiredInstances) {
        if (desiredInstances < instances.size()) {
            throw new GradleException("Cannot lower number of instances once they are allocated. " +
                    "Asking for [${desiredInstances}] but have already allocated [${instances.size()}]")
        }

        instance(desiredInstances - 1)
    }

    List<InstanceConfiguration> getInstances() {
        return instances
    }

    RoleDescriptor getRoleDescriptor() {
        return roleDescriptor
    }

    ServiceConfiguration getServiceConf() {
        return serviceConfiguration
    }

    @Override
    protected ProcessConfiguration parent() {
        return serviceConfiguration
    }
}
