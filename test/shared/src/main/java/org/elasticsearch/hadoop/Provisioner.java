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

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.GenericOptionsParser;
import org.elasticsearch.hadoop.util.Assert;

/**
 * Builder class for adding jars to the Hadoop environment.
 */
public abstract class Provisioner {

    public static final String ESHADOOP_TESTING_JAR;
    public static final String HDFS_ES_HDP_LIB = "/eshdp/libs/es-hadoop.jar";
    public static final String SYS_PROP_JOB_JAR = "es.hadoop.job.jar";

    static {
        // init ES-Hadoop JAR
        // expect the jar under build\libs
        try {
            String jobJarLocation = System.getProperty(SYS_PROP_JOB_JAR);
            if (jobJarLocation == null) {
                throw new RuntimeException("Cannot find elasticsearch hadoop jar. System Property [" + SYS_PROP_JOB_JAR + "] was not set.");
            }

            File testJar = new File(jobJarLocation).getCanonicalFile();
            Assert.isTrue(testJar.exists(),
                    String.format("Cannot find elasticsearch hadoop jar. File not found [%s]", testJar));
            ESHADOOP_TESTING_JAR = testJar.getAbsolutePath();
        } catch (IOException ex) {
            throw new RuntimeException("Cannot find required files", ex);
        }
    }


    public static JobConf provision(JobConf conf) {
        // set job jar
        conf.set("mapred.jar", ESHADOOP_TESTING_JAR);
        return conf;
    }

    protected static void addLibs(Configuration configuration, String... libs) {
        addResource(configuration, libs, "-libjars");
    }

    private static void addResource(Configuration cfg, String[] locations, String param) {
        Assert.notNull(cfg, "a non-null configuration is required");

        List<String> list = new ArrayList<String>();

        try {
            if (locations != null) {
                int count = locations.length;
                list.add(param);

                StringBuilder sb = new StringBuilder();
                for (String location : locations) {
                    if (location != null) {
                        sb.append(location);
                        if (--count > 0) {
                            sb.append(",");
                        }
                    }
                }
                list.add(sb.toString());
            }

            new GenericOptionsParser(cfg, list.toArray(new String[list.size()]));
        } catch (Exception ex) {
            throw new EsHadoopIllegalStateException(ex);
        }
    }
}