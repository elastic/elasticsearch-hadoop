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
import org.elasticsearch.hadoop.gradle.fixture.hadoop.hdfs.HdfsServiceDescriptor
import org.elasticsearch.hadoop.gradle.fixture.hadoop.hive.HiveServiceDescriptor
import org.elasticsearch.hadoop.gradle.fixture.hadoop.yarn.YarnServiceDescriptor
import org.gradle.api.GradleException
import org.gradle.api.Project
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

    static final HdfsServiceDescriptor HDFS = new HdfsServiceDescriptor()
    static final YarnServiceDescriptor YARN = new YarnServiceDescriptor()
    static final HiveServiceDescriptor HIVE = new HiveServiceDescriptor()

    private static final Map<String, ServiceDescriptor> SUPPORTED_SERVICES = [HDFS, YARN, HIVE]
                    .collectEntries { [(it.id()): it] }

    private static final ProcessConfiguration END = new EndProcessConfiguration()

    private final Project project
    private final String name
    private final Map<String, ServiceConfiguration> serviceConfigurations
    private final List<ServiceConfiguration> serviceCreationOrder

    HadoopClusterConfiguration(Project project, String name) {
        this.project = project
        this.name = name
        this.serviceConfigurations = [:]
        this.serviceCreationOrder = []
    }

    ServiceConfiguration service(String serviceName) {
        return service(serviceName, null)
    }

    ServiceConfiguration service(String serviceName, Closure<ServiceConfiguration> configure) {
        ServiceDescriptor serviceDescriptor = SUPPORTED_SERVICES.get(serviceName)
        if (serviceDescriptor == null) {
            throw new GradleException("HadoopClusterConfiguration does not support unknown service [${serviceName}]")
        }
        return doGetService(serviceDescriptor, configure)
    }

    private ServiceConfiguration doGetService(ServiceDescriptor descriptor) {
        return doGetService(descriptor, null)
    }

    private ServiceConfiguration doGetService(ServiceDescriptor serviceDescriptor, Closure<ServiceConfiguration> configure) {
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
        return END
    }
}
