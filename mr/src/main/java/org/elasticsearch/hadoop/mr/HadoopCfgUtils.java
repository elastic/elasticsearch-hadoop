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
package org.elasticsearch.hadoop.mr;

import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.elasticsearch.hadoop.util.unit.TimeValue;

/**
 * Class which handles the various Hadoop properties, aware of the changes between YARN (Hadoop 2) and 1.
 */
public abstract class HadoopCfgUtils {


    public static boolean isLocal(Configuration cfg) {
        return "local".equals(cfg.get("mapreduce.framework.name")) || "local".equals(getJobTracker(cfg));
    }

    public static String getFileSystem(Configuration cfg) {
        return get(cfg, "fs.defaultFS", "fs.default.name");
    }

    public static void setFileSystem(Configuration cfg, String value) {
        set(cfg, value, "fs.defaultFS", "fs.default.name");
    }

    public static String getJobTracker(Configuration cfg) {
        return get(cfg, "mapreduce.jobtracker.address", "mapred.job.tracker");
    }

    public static void setJobTracker(Configuration cfg, String value) {
        set(cfg, value, "mapreduce.jobtracker.address", "mapred.job.tracker");
    }

    public static String getFileOutputFormatDir(Configuration cfg) {
        return get(cfg, "mapreduce.output.fileoutputformat.outputdir", "mapred.output.dir");
    }

    public static void setFileOutputFormatDir(Configuration cfg, String value) {
        set(cfg, value, "mapreduce.output.fileoutputformat.outputdir", "mapred.output.dir");
    }

    public static String getOutputCommitterClass(Configuration cfg) {
        return get(cfg, "mapred.output.committer.class", null);
    }

    public static void setOutputCommitterClass(Configuration cfg, String value) {
        set(cfg, value, "mapred.output.committer.class", null);
    }

    public static String getTaskAttemptId(Configuration cfg) {
        return get(cfg, "mapreduce.task.attempt.id", "mapred.task.id");
    }

    public static String getTaskId(Configuration cfg) {
        return get(cfg, "mapreduce.task.id", "mapred.tip.id");
    }

    public static String getReduceTasks(Configuration cfg) {
        return get(cfg, "mapreduce.job.reduces", "mapred.reduce.tasks");
    }

    public static boolean getSpeculativeReduce(Configuration cfg) {
        return get(cfg, "mapreduce.reduce.speculative", "mapred.reduce.tasks.speculative.execution", true);
    }

    public static boolean getSpeculativeMap(Configuration cfg) {
        return get(cfg, "mapreduce.map.speculative", "mapred.map.tasks.speculative.execution", true);
    }

    public static void setGenericOptions(Configuration cfg) {
        set(cfg, "true", "mapreduce.client.genericoptionsparser.used", "mapred.used.genericoptionsparser");
    }

    public static TimeValue getTaskTimeout(Configuration cfg) {
        return TimeValue.parseTimeValue(get(cfg, "mapreduce.task.timeout", "mapred.task.timeout"));
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

    private static String get(Configuration cfg, String hadoop2, String hadoop1) {
        String prop = cfg.get(hadoop2);
        return (prop != null ? prop : (hadoop1 != null ? cfg.get(hadoop1) : null));
    }

    private static boolean get(Configuration cfg, String hadoop2, String hadoop1, boolean defaultValue) {
        String result = get(cfg, hadoop2, hadoop1);
        if ("true".equals(result))
            return true;
        else if ("false".equals(result))
            return false;
        else
            return defaultValue;
    }


    private static void set(Configuration cfg, String value, String hadoop2, String hadoop1) {
        cfg.set(hadoop2, value);
        if (hadoop1 != null) {
            cfg.set(hadoop1, value);
        }
    }

    public static JobConf asJobConf(Configuration cfg) {
        return (cfg instanceof JobConf ? (JobConf) cfg : new JobConf(cfg));
    }
}