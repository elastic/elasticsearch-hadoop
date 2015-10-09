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
package org.elasticsearch.hadoop;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.elasticsearch.hadoop.mr.HadoopCfgUtils;
import org.elasticsearch.hadoop.util.ReflectionUtils;
import org.elasticsearch.hadoop.util.TestSettings;
import org.elasticsearch.hadoop.util.TestUtils;

public class HdpBootstrap {

    private static boolean hackVerified = false;

    /**
     * Hack to allow Hadoop client to run on windows (which otherwise fails due to some permission problem).
     */
    public static void hackHadoopStagingOnWin() {
        // do the assignment only on Windows systems
        if (TestUtils.isWindows()) {
            // 0655 = -rwxr-xr-x , 0650 = -rwxr-x---
            JobSubmissionFiles.JOB_DIR_PERMISSION.fromShort((short) 0650);
            JobSubmissionFiles.JOB_FILE_PERMISSION.fromShort((short) 0650);

            Field field = null;

            // handle distributed cache permissions on Hadoop < 2.4
            try {
                Class<?> jl = Class.forName("org.apache.hadoop.mapred.JobLocalizer");
                field = ReflectionUtils.findField(jl, "privateCachePerms");

                if (field != null) {
                    ReflectionUtils.makeAccessible(field);
                    FsPermission perm = (FsPermission) ReflectionUtils.getField(field, null);
                    perm.fromShort((short) 0650);
                }
            } catch (ClassNotFoundException cnfe) {
                // ignore
            }

            // handle jar permissions as well - temporarily disable for CDH 4 / YARN
            try {
                Class<?> tdcm = Class.forName("org.apache.hadoop.filecache.TrackerDistributedCacheManager");
                field = ReflectionUtils.findField(tdcm, "PUBLIC_CACHE_OBJECT_PERM");
                ReflectionUtils.makeAccessible(field);
                FsPermission perm = (FsPermission) ReflectionUtils.getField(field, null);
                perm.fromShort((short) 0650);
            } catch (ClassNotFoundException cnfe) {
                //ignore
                return;
            } catch (Exception ex) {
                LogFactory.getLog(TestUtils.class).warn("Cannot set permission for TrackerDistributedCacheManager", ex);
            }
        }
    }

    public static <T extends Configuration> T addProperties(T conf, Properties props, boolean override) {
        for (Entry<Object, Object> entry : props.entrySet()) {
            if (override || conf.get(entry.getKey().toString()) == null) {
                conf.set(entry.getKey().toString(), entry.getValue().toString());
            }
        }
        return conf;
    }

    public static JobConf hadoopConfig() {
        if (!hackVerified) {
            hackVerified = true;
            // check local execution
            if ("local".equals(TestSettings.TESTING_PROPS.get("mapred.job.tracker"))) {
                hackHadoopStagingOnWin();
            }
            // damn HADOOP-9123
            System.setProperty("path.separator", ":");
        }

        JobConf conf = addProperties(new JobConf(), TestSettings.TESTING_PROPS, true);
        HadoopCfgUtils.setGenericOptions(conf);

        // provision if not local
        if (!HadoopCfgUtils.isLocal(conf)) {
            Provisioner.provision(conf);
            HdfsUtils.rmr(conf, ".staging");
        }


        return conf;
    }

    public static Properties asProperties(Configuration cfg) {
        Properties props = new Properties();

        if (cfg != null) {
            for (Map.Entry<String, String> entry : cfg) {
                props.setProperty(entry.getKey(), entry.getValue());
            }
        }

        return props;
    }
}