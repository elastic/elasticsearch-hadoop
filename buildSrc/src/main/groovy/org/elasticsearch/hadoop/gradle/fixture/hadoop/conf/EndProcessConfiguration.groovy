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

import org.elasticsearch.gradle.testclusters.ElasticsearchCluster
import org.gradle.api.Project

/**
 * Provides defaults and can be slotted in as the last parent configuration in a chain.
 *
 * ProcessConfiguration is a chain of configurations. This is meant to be the head of the
 * chain, in that it provides the empty defaults.
 *
 * End -> Cluster -> Service -> Role -> Instance
 */
class EndProcessConfiguration extends ProcessConfiguration {

    EndProcessConfiguration(Project project) {
        super(project)
    }

    @Override
    protected ProcessConfiguration parent() {
        return null
    }

    @Override
    Map<String, String> getSystemProperties() {
        return Collections.emptyMap()
    }

    @Override
    Map<String, String> getEnvironmentVariables() {
        return Collections.emptyMap()
    }

    @Override
    SettingsContainer getSettingsContainer() {
        return new SettingsContainer()
    }

    @Override
    Map<String, Object> getExtraConfigFiles() {
        return Collections.emptyMap()
    }

    @Override
    Map<String, Object[]> getSetupCommands() {
        return Collections.emptyMap()
    }

    @Override
    List<Object> getDependencies() {
        return Collections.emptyList()
    }

    @Override
    String getJavaHome() {
        return project.runtimeJavaHome
    }

    @Override
    String getJvmArgs() {
        return ""
    }

    @Override
    boolean getDebug() {
        return false
    }

    @Override
    ElasticsearchCluster getElasticsearchCluster() {
        return null
    }
}
