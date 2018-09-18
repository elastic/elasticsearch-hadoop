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

import org.apache.tools.ant.taskdefs.condition.Os
import org.gradle.api.Project

/**
 * Configuration for a Hadoop cluster, used for integration tests
 */
class HadoopClusterConfiguration {

    enum Service {
        HDFS_NAMENODE,
        HDFS_DATANODE,
        YARN_RESOURCEMANAGER,
        YARN_NODEMANAGER
    }

    private final Project project

    public String instances(ServiceIdentifier instance) {
        return 1 // Only one service at a time for now.
    }

    public String packageName(ServiceIdentifier instance) {
        if (instance.serviceGroup == "hadoop") {
            return "hadoop"
        }
    }

    public String configPath(ServiceIdentifier instance) {
        if (instance.serviceGroup == "hadoop") {
            return "etc/hadoop"
        }
    }

    public String configFile(ServiceIdentifier instance) {
        if (instance.serviceGroup == "hadoop") {
            if (instance.serviceName == "namenode") {
                return "hdfs-site.xml"
            }
        }
    }

    public List<String> startCommand(ServiceIdentifier instance) {
        if (instance.serviceName == "namenode") {
            if (Os.isFamily(Os.FAMILY_WINDOWS)) {
                return ['hdfs.cmd', 'namenode']
            } else {
                return ['hdfs', 'namenode']
            }
        }
    }

    /**
     * If the service is run as a daemon, should the script be wrapped or run as is?
     */
    public boolean wrapScript(ServiceIdentifier instance) {
        return instance.serviceGroup != "hadoop"
    }

    public List<String> daemonStartCommand(ServiceIdentifier serviceIdentifier) {
        if (serviceIdentifier.serviceName == "namenode") {
            if (Os.isFamily(Os.FAMILY_WINDOWS)) {
                return ['hdfs.cmd', 'namenode']
            } else {
                return ['hadoop-daemon.sh', 'start', 'namenode']
            }
        }
    }

    public List<String> daemonStopCommand(ServiceIdentifier serviceIdentifier) {
        if (serviceIdentifier.serviceName == "namenode") {
            if (Os.isFamily(Os.FAMILY_WINDOWS)) {
                return null
            } else {
                return ['hadoop-daemon.sh', 'stop', 'namenode']
            }
        }
    }

    public String pidFileName(ServiceIdentifier instance) {
        return "hadoop-james.baiera-${instance.serviceName}.pid"
    }

    public String envIdentString(ServiceIdentifier instance) {
        return "HADOOP_IDENT_STRING"
    }

    public String scriptDir(ServiceIdentifier serviceIdentifier) {
        String dirName
        if (daemonize) {
            if (wrapScript(serviceIdentifier)) {
                dirName = "bin"
            } else {
                dirName = "sbin"
            }
        } else {
            dirName = "bin"
        }
    }

    public String pidFileEnvSetting(ServiceIdentifier instance) {
        if (instance.serviceName == "namenode") {
            return "HADOOP_PID_DIR"
        } else if (instance.serviceName == "datanode") {
            return "HADOOP_PID_DIR"
        } else if (instance.serviceName == "resourcemanager") {
            return "YARN_PID_DIR"
        } else if (instance.serviceName == "nodemanager") {
            return "YARN_PID_DIR"
        } else {
            return null
        }
    }

    /**
     * The name of the environment variable whose contents are used for the service's java system properties
     */
    public String envSysPropOption(ServiceIdentifier instance) {
        if (instance.serviceName == "namenode") {
            return "HADOOP_NAMENODE_OPTS"
        } else if (instance.serviceName == "datanode") {
            return "HADOOP_DATANODE_OPTS"
        } else if (instance.serviceName == "resourcemanager") {
            return "YARN_OPTS"
        } else if (instance.serviceName == "nodemanager") {
            return "YARN_OPTS"
        }
    }

    /*
     * String distribution? Nope, only Tar supported
     * int num nodes? always 1
     * int num bwc nodes? always 0?
     * Version bwc version? Not set yet.
     * int httpPort? NA?
     * int transportPort? defaults
     * Closure<String> dataDirectory? This might be good to set
     * String clusterName? Probably not important
     * boolean daemonize? Probably not applicable
     * boolean debug? Not sure
     * mimimumMasterNodes? No
     * String jvmArgs? Yes - per service
     * boolean cleanShared? Do we need a shared environment?
     * Closure waitCondition? Yes
     *      takes NodeInfo and AntBuilder
     *      We'll probably have ServiceInfo for each item
     *      hits wait URL and writes url contents to wait.success
     *      returns that wait.success exists
     * nodeStartupWaitSeconds, yes 30
     */

    Map<String, String> systemProperties = new HashMap<>()

    Map<String, String> environmentVariables = new HashMap<>()

    Map<String, String> settings = new HashMap<>()

    // Don't need keystore settings or files do we?

    Map<String, Object> extraConfigFiles = new HashMap<>()

    // don't need plugins or modules

    HashMap<String, LinkedHashMap<String, Object[]>> setupCommands = new LinkedHashMap<>()

    List<Object> dependencies = new ArrayList<>()

    // outputs the path for the data dir
    Closure<ServiceIdentifier> dataDir

    // Not sure this ever makes sense to be false
    boolean daemonize = true

    String jvmArgs = ''

    boolean debug

    public void addSetupCommand(ServiceIdentifier instance, String commandName, Object[] command) {
        LinkedHashMap<String, Object[]> commandMap = setupCommands.get(instance.serviceName)
        if (commandMap == null) {
            commandMap = new LinkedHashMap<>()
            setupCommands.put(instance.serviceName, commandMap)
        }
        commandMap.put(commandName, command)
    }

    public Map<String, Object[]> setupCommands(ServiceIdentifier instance) {
        return setupCommands.getOrDefault(instance.serviceName, Collections.emptyMap())
    }
}
