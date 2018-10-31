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

package org.elasticsearch.hadoop.gradle.fixture.hadoop

/**
 * Describes deployment characteristics for the roles within a service.
 *
 * Used to define the names of roles in a service, the roles they depend on,
 * and their default instance counts.
 */
class RoleDescriptor {

    static RoleDescriptor requiredProcess(String name) {
        return requiredProcess(name, [])
    }

    static RoleDescriptor requiredProcess(String name, List<RoleDescriptor> dependentRoles) {
        return new RoleDescriptor(true, name, 1, dependentRoles, true)
    }

    static RoleDescriptor optionalProcess(String name, int defaultInstances, List<RoleDescriptor> dependentRoles) {
        return new RoleDescriptor(false, name, defaultInstances, dependentRoles, true)
    }

    static RoleDescriptor requiredGateway(String serviceName, List<RoleDescriptor> dependentRoles) {
        return new RoleDescriptor(true, "${serviceName}.gateway", 1, dependentRoles, false)
    }

    static RoleDescriptor optionalGateway(String serviceName, List<RoleDescriptor> dependentRoles) {
        return new RoleDescriptor(false, "${serviceName}.gateway", 1, dependentRoles, false)
    }

    private final boolean required
    private final String name
    private final int defaultInstances
    private final List<RoleDescriptor> dependentRoles
    private final boolean executableProcess

    RoleDescriptor(boolean required, String name, int defaultInstances, List<RoleDescriptor> dependentRoles, boolean executableProcess) {
        this.required = required
        this.name = name
        this.defaultInstances = defaultInstances
        this.dependentRoles = dependentRoles
        this.executableProcess = executableProcess
    }

    boolean required() {
        return required
    }

    String roleName() {
        return name
    }

    int defaultInstances() {
        return defaultInstances
    }

    List<RoleDescriptor> dependentRoles() {
        return dependentRoles
    }

    boolean isExecutableProcess() {
        return executableProcess
    }
}