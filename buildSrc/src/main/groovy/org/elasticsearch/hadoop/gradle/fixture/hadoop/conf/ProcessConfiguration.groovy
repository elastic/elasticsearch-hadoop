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
import org.gradle.api.InvalidUserDataException
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.util.ConfigureUtil

/**
 * All the configurations that can be set hierarchically for a cluster.
 *
 * Each configuration class extends this, and optionally returns its parent config via the abstract method.
 * This class takes care of combining all settings from the root cluster config down to the instance level config.
 */
abstract class ProcessConfiguration {

    protected abstract ProcessConfiguration parent()

    ProcessConfiguration(Project project) {
        this.project = project
    }

    protected final Project project
    private Map<String, String> systemProperties = new HashMap<>()
    private Map<String, String> environmentVariables = new HashMap<>()
    private SettingsContainer settingsContainer = new SettingsContainer()
    private Map<String, Object> extraConfigFiles = new HashMap<>()
    private LinkedHashMap<String, Object[]> setupCommands = new LinkedHashMap<>()
    private List<Object> dependencies = new ArrayList<>()
    private List<Task> clusterTasks = new ArrayList<>()
    private String javaHome = null
    private String jvmArgs = ''
    private boolean debug = false
    private ElasticsearchCluster elasticsearchCluster = null

    void addSystemProperty(String key, String value) {
        systemProperties.put(key, value)
    }

    Map<String, String> getSystemProperties() {
        Map<String, String> combined = new HashMap<>()
        ProcessConfiguration parent = parent()
        if (parent != null) {
            combined.putAll(parent.getSystemProperties())
        }
        combined.putAll(systemProperties)
        return combined
    }

    void addEnvironmentVariable(String key, String value) {
        environmentVariables.put(key, value)
    }

    Map<String, String> getEnvironmentVariables() {
        Map<String, String> combined = new HashMap<>()
        ProcessConfiguration parent = parent()
        if (parent != null) {
            combined.putAll(parent.getEnvironmentVariables())
        }
        combined.putAll(environmentVariables)
        return combined
    }

    void addSetting(String key, Object value) {
        settingsContainer.addSetting(key, value)
    }

    SettingsContainer.FileSettings settingsFile(String filename) {
        return settingsFile(filename, null)
    }

    SettingsContainer.FileSettings settingsFile(String filename, Closure<?> config) {
        SettingsContainer.FileSettings file = settingsContainer.getFile(filename)
        if (config != null) {
            ConfigureUtil.configure(config, file)
        }
        return file
    }

    SettingsContainer getSettingsContainer() {
        SettingsContainer combined = new SettingsContainer()
        ProcessConfiguration parent = parent()
        if (parent != null) {
            combined.combine(parent.getSettingsContainer())
        }
        combined.combine(settingsContainer)
        return combined
    }

    void addExtraConfigFile(String key, Object file) {
        extraConfigFiles.put(key, file)
    }

    Map<String, Object> getExtraConfigFiles() {
        Map<String, Object> combined = new HashMap<>()
        ProcessConfiguration parent = parent()
        if (parent != null) {
            combined.putAll(parent.getExtraConfigFiles())
        }
        combined.putAll(extraConfigFiles)
        return combined
    }

    void addSetupCommand(String commandName, Object[] command) {
        setupCommands.put(commandName, command)
    }

    Map<String, Object[]> getSetupCommands() {
        LinkedHashMap<String, Object[]> combined = new LinkedHashMap<>()
        ProcessConfiguration parent = parent()
        if (parent != null) {
            combined.putAll(parent.getSetupCommands())
        }
        combined.putAll(setupCommands)
        return combined
    }

    void addDependency(Object dependency) {
        dependencies.add(dependency)
    }

    List<Object> getDependencies() {
        List<Object> combined = new ArrayList<>()
        ProcessConfiguration parent = parent()
        if (parent != null) {
            combined.addAll(parent.getDependencies())
        }
        combined.addAll(dependencies)
        return combined
    }

    void setJavaHome(String javaHome) {
        this.javaHome = javaHome
    }

    String getJavaHome() {
        if (this.javaHome != null) {
            return this.javaHome
        } else {
            ProcessConfiguration parent = parent()
            if (parent != null) {
                return parent.getJavaHome()
            } else {
                return null
            }
        }
    }

    void setJvmArgs(String jvmArgs) {
        this.jvmArgs = jvmArgs
    }

    String getJvmArgs() {
        String combined = ""
        ProcessConfiguration parent = parent()
        if (parent != null) {
            combined = combined + parent.getJvmArgs() + " "
        }
        combined = combined + jvmArgs
        return combined
    }

    void setDebug(boolean debug) {
        this.debug = debug
    }

    boolean getDebug() {
        if (debug) {
            // Prefer local value if true
            return debug
        } else {
            // if the local value is false, fall back to parent value if has one
            ProcessConfiguration parent = parent()
            if (parent != null) {
                return parent.getDebug()
            }
        }
        // No parent, return value (which should be false)
        return debug
    }

    void useElasticsearchCluster(ElasticsearchCluster elasticsearchCluster) {
        this.elasticsearchCluster = elasticsearchCluster
    }

    ElasticsearchCluster getElasticsearchCluster() {
        if (this.elasticsearchCluster != null) {
            return this.elasticsearchCluster
        } else {
            ProcessConfiguration parent = parent()
            if (parent != null) {
                return parent.getElasticsearchCluster()
            } else {
                return null
            }
        }
    }

    Task createClusterTask(Map<String, ?> options) throws InvalidUserDataException {
        Task task = project.tasks.create(options)
        addClusterTask(task)
        return task
    }

    Task createClusterTask(Map<String, ?> options, Closure configureClosure) throws InvalidUserDataException {
        Task task = project.tasks.create(options, configureClosure)
        addClusterTask(task)
        return task
    }

    Task createClusterTask(String name) throws InvalidUserDataException {
        Task task = project.tasks.create(name)
        addClusterTask(task)
        return task
    }

    Task createClusterTask(String name, Closure configureClosure) throws InvalidUserDataException {
        Task task = project.tasks.create(name, configureClosure)
        addClusterTask(task)
        return task
    }

    public <T extends Task> T createClusterTask(String name, Class<T> type) throws InvalidUserDataException {
        T task = project.tasks.create(name, type)
        addClusterTask(task)
        return task
    }

    public <T extends Task> T createClusterTask(String name, Class<T> type, Closure configureClosure)
            throws InvalidUserDataException {
        T task = project.tasks.create(name, type)
        ConfigureUtil.configure(configureClosure, task)
        addClusterTask(task)
        return task
    }

    void addClusterTask(Task task) {
        clusterTasks.add(task)
    }

    /**
     * Get all tasks that expect the processes and resources created and maintained by this fixture to exist before
     * running, and that should be concluded with this fixture's eventual halting.
     * @return
     */
    List<Task> getClusterTasks() {
        return clusterTasks
    }
}
