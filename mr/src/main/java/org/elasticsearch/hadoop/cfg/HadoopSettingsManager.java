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
package org.elasticsearch.hadoop.cfg;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;

/**
 * Factory for loading settings based on various configuration objects, such as Properties or Hadoop configuration.
 * The factory main role is to minimize the number of dependencies required at compilation time in the event that ES-Hadoop is running
 * in a non-Hadoop oriented environment.
 */
public class HadoopSettingsManager implements SettingsManager<Object> {

    private final static Class<?> HADOOP_CONFIGURATION;

    static {
        Class<?> cfgClass = null;
        try {
            cfgClass = Class.forName("org.apache.hadoop.conf.Configuration", false, HadoopSettingsManager.class.getClassLoader());
        } catch (Exception ex) {
            // ignore
        }
        HADOOP_CONFIGURATION = cfgClass;
    }

    private abstract static class FromHadoopConfiguration {
        public static Settings create(Object cfg) {
            return new HadoopSettings((Configuration) cfg);
        }
    }

    public static Settings loadFrom(Object configuration) {
        if (configuration instanceof Properties) {
            return new PropertiesSettings((Properties) configuration);
        }
        if (HADOOP_CONFIGURATION != null && HADOOP_CONFIGURATION.isInstance(configuration)) {
            return FromHadoopConfiguration.create(configuration);
        }
        throw new EsHadoopIllegalArgumentException("Don't know how to create Settings from configuration " + configuration);
    }

    public Settings load(Object configuration) {
        return loadFrom(configuration);
    }
}