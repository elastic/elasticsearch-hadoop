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
package org.elasticsearch.hadoop.yarn.util;

import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.elasticsearch.hadoop.yarn.EsYarnConstants;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.*;

public abstract class YarnUtils {

    // missing in Hadoop 2.2
    public static final String[] DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH = {
            ApplicationConstants.Environment.HADOOP_CONF_DIR.$$(),
            ApplicationConstants.Environment.HADOOP_COMMON_HOME.$$() + "/share/hadoop/common/*",
            ApplicationConstants.Environment.HADOOP_COMMON_HOME.$$() + "/share/hadoop/common/lib/*",
            ApplicationConstants.Environment.HADOOP_HDFS_HOME.$$() + "/share/hadoop/hdfs/*",
            ApplicationConstants.Environment.HADOOP_HDFS_HOME.$$() + "/share/hadoop/hdfs/lib/*",
            ApplicationConstants.Environment.HADOOP_YARN_HOME.$$() + "/share/hadoop/yarn/*",
            ApplicationConstants.Environment.HADOOP_YARN_HOME.$$() + "/share/hadoop/yarn/lib/*" };

    public static ApplicationAttemptId getApplicationAttemptId(Map<String, String> env) {
        if (env == null) {
            return null;
        }
        String amContainerId = env.get(ApplicationConstants.Environment.CONTAINER_ID.name());
        if (amContainerId == null) {
            return null;
        }
        ContainerId containerId = ConverterUtils.toContainerId(amContainerId);
        return containerId.getApplicationAttemptId();
    }

    public static InetSocketAddress getResourceManagerAddr(Configuration cfg) {
        return cfg.getSocketAddr(RM_SCHEDULER_ADDRESS, DEFAULT_RM_SCHEDULER_ADDRESS, DEFAULT_RM_SCHEDULER_PORT);
    }

    public static long getAmHeartBeatRate(Configuration cfg) {
        return cfg.getLong(RM_AM_EXPIRY_INTERVAL_MS, DEFAULT_RM_AM_EXPIRY_INTERVAL_MS);
    }

    public static Map<String, String> setupEnv(Configuration cfg) {
        Map<String, String> env = new LinkedHashMap<String, String>(); // System.getenv()
        // add Hadoop Classpath
        for (String c : cfg.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            Apps.addToEnvironment(env, Environment.CLASSPATH.name(), c.trim(), ApplicationConstants.CLASS_PATH_SEPARATOR);
        }
        // add es-hadoop jar / current folder jars
        Apps.addToEnvironment(env, Environment.CLASSPATH.name(), "./*", ApplicationConstants.CLASS_PATH_SEPARATOR);

        //
        // some es-yarn constants
        //
        Apps.addToEnvironment(env, EsYarnConstants.FS_URI, cfg.get(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS));

        return env;
    }

    public static Object minVCores(Configuration cfg, int vCores) {
        return yarnAcceptableMin(cfg, RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES, vCores);
    }

    public static int minMemory(Configuration cfg, int memory) {
        return yarnAcceptableMin(cfg, RM_SCHEDULER_MINIMUM_ALLOCATION_MB, memory);
    }

    private static int yarnAcceptableMin(Configuration cfg, String property, int value) {
        int acceptedVal = Integer.parseInt(cfg.get(property));
        if (acceptedVal >= value) {
            return acceptedVal;
        }

        if (value % acceptedVal != 0) {
            return acceptedVal * (value % acceptedVal);
        }
        return value;
    }

    public static ApplicationId createAppIdFrom(String appId) {
        appId = appId.substring(ApplicationId.appIdStrPrefix.length());
        int delimiter = appId.indexOf("-");
        return ApplicationId.newInstance(Long.parseLong(appId.substring(0, delimiter)), Integer.parseInt(appId.substring(delimiter + 1)));
    }
}