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

import org.elasticsearch.hadoop.gradle.fixture.hadoop.ServiceDescriptor
import org.elasticsearch.hadoop.gradle.fixture.hadoop.services.HiveServiceDescriptor
import org.elasticsearch.hadoop.gradle.fixture.hadoop.services.HadoopServiceDescriptor
import org.elasticsearch.hadoop.gradle.fixture.hadoop.services.PigServiceDescriptor
import org.elasticsearch.hadoop.gradle.fixture.hadoop.services.SparkYarnServiceDescriptor
import org.gradle.api.GradleException
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.util.ConfigureUtil

/**
 * Configuration for a Hadoop cluster, used for integration tests.
 *
 * The master configuration for the entire cluster. Each service, role, and instance
 * inherits config values from this config.
 *
 * End -> Cluster -> Service -> Role -> Instance
 */
class HadoopClusterConfiguration extends ProcessConfiguration {

    static final ServiceDescriptor HADOOP = new HadoopServiceDescriptor()
    static final ServiceDescriptor HIVE = new HiveServiceDescriptor()
    static final ServiceDescriptor SPARK = new SparkYarnServiceDescriptor()
    static final ServiceDescriptor PIG = new PigServiceDescriptor()

    private static final Map<String, ServiceDescriptor> SUPPORTED_SERVICES = [HADOOP, HIVE, PIG, SPARK]
                    .collectEntries { [(it.id()): it] }

    private final Project project
    private final String name
    private final List<Task> clusterTasks
    private final ProcessConfiguration defaultConfiguration
    private final Map<String, ServiceConfiguration> serviceConfigurations
    private final List<ServiceConfiguration> serviceCreationOrder

    HadoopClusterConfiguration(Project project, String name) {
        super(project)
        this.project = project
        this.name = name
        this.clusterTasks = []
        this.defaultConfiguration = new EndProcessConfiguration(project)
        this.serviceConfigurations = [:]
        this.serviceCreationOrder = []
    }

    ServiceConfiguration service(String serviceName) {
        return service(serviceName, null)
    }

    ServiceConfiguration service(String serviceName, Closure<?> configure) {
        ServiceDescriptor serviceDescriptor = SUPPORTED_SERVICES.get(serviceName)
        if (serviceDescriptor == null) {
            throw new GradleException("HadoopClusterConfiguration does not support unknown service [${serviceName}]")
        }
        return doGetService(serviceDescriptor, configure)
    }

    ServiceConfiguration service(ServiceDescriptor serviceDescriptor) {
        if (!SUPPORTED_SERVICES.containsKey(serviceDescriptor.id())) {
            throw new GradleException("HadoopClusterConfiguration does not support unknown service [${serviceDescriptor.id()}]")
        }
        return doGetService(serviceDescriptor, null)
    }

    private ServiceConfiguration doGetService(ServiceDescriptor descriptor) {
        return doGetService(descriptor, null)
    }

    private ServiceConfiguration doGetService(ServiceDescriptor serviceDescriptor, Closure<?> configure) {
        ServiceConfiguration serviceConf = serviceConfigurations.get(serviceDescriptor.id())
        if (serviceConf != null) {
            if (configure != null) {
                ConfigureUtil.configure(configure, serviceConf)
            }
            return serviceConf
        }

        // Create the service conf and its dependent service confs

        // Ensure that the dependent services already exist
        List<ServiceConfiguration> dependentServices = []
        for (ServiceDescriptor dependentServiceName : serviceDescriptor.serviceDependencies()) {
            ServiceConfiguration conf = doGetService(dependentServiceName)
            dependentServices.add(conf)
        }

        serviceConf = new ServiceConfiguration(project, this, name, serviceDescriptor, dependentServices)
        serviceConfigurations.put(serviceDescriptor.id(), serviceConf)
        serviceCreationOrder.add(serviceConf)

        if (configure != null) {
            ConfigureUtil.configure(configure, serviceConf)
        }

        return serviceConf
    }

    String getName() {
        return name
    }

    List<ServiceConfiguration> getServices() {
        return serviceCreationOrder
    }

    @Override
    protected ProcessConfiguration parent() {
        return defaultConfiguration
    }
}
