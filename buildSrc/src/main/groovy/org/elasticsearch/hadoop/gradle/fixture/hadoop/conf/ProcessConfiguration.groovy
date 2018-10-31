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

import org.elasticsearch.hadoop.gradle.fixture.hadoop.ServiceIdentifier

/**
 * All the configurations that can be set hierarchically for a cluster.
 *
 * Each configuration class extends this, and optionally returns its parent config via the abstract method.
 * This class takes care of combining all settings from the root cluster config down to the instance level config.
 */
abstract class ProcessConfiguration {

    protected abstract ProcessConfiguration parent()

    /*
     * FIXHERE: Pick properties to add here
     * String distribution? Nope, only Tar supported
     * int num nodes? set this at the role level?
     * int num bwc nodes? no, hadoop should be homogeneous
     * Version bwc version? Not set yet. Set this at the service level?
     * int httpPort? Nah
     * int transportPort? Nah
     * Closure<String> dataDirectory? This might be good to set
     * String clusterName? Defaults to prefix at the moment
     * boolean daemonize? Should always be true - not sure what we get for it being false
     * boolean debug? Yeah, add it, super useful if it is ever needed
     * mimimumMasterNodes? Not needed
     * String jvmArgs? Yes
     * boolean cleanShared? Sure, but at the service level
     * Closure waitCondition? Yes
     *      takes NodeInfo and AntBuilder (We'll probably have InstanceInfo for each item)
     *      hits wait URL and writes url contents to wait.success
     *      returns that wait.success exists
     * nodeStartupWaitSeconds, yes 30
     */

    private Map<String, String> systemProperties = new HashMap<>()
    private Map<String, String> environmentVariables = new HashMap<>()
    private Map<String, String> settings = new HashMap<>()
    private Map<String, Object> extraConfigFiles = new HashMap<>()
    private LinkedHashMap<String, Object[]> setupCommands = new LinkedHashMap<>()
    private List<Object> dependencies = new ArrayList<>()
    // outputs the path for the data dir
    private Closure<ServiceIdentifier> dataDir
    // Not sure this ever makes sense to be false
//    private boolean daemonize = true
    private String jvmArgs = ''
    private boolean debug = false

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

    void addSetting(String key, String value) {
        settings.put(key, value)
    }

    Map<String, String> getSettings() {
        Map<String, String> combined = new HashMap<>()
        ProcessConfiguration parent = parent()
        if (parent != null) {
            combined.putAll(parent.getSettings())
        }
        combined.putAll(settings)
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

    void setDataDir(Closure<ServiceIdentifier> dataDir) {
        this.dataDir = dataDir
    }

    Closure<ServiceIdentifier> getDataDir() {
        if (dataDir != null) {
            return dataDir
        }
        ProcessConfiguration parent = parent()
        if (parent != null) {
            return parent.getDataDir()
        }
        return null
    }

//    void setDaemonize(boolean daemonize) {
//        this.daemonize = daemonize
//    }

//    boolean isDaemonized() {
//        if (daemonize) {
//            // Prefer local value if true
//            return daemonize
//        } else {
//            // if the local value is false, fall back to parent value if has one
//            ProcessConfiguration parent = parent()
//            if (parent != null) {
//                return parent.isDaemonized()
//            }
//        }
//        // No parent, return value (which should be false)
//        return daemonize
//    }

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
}
