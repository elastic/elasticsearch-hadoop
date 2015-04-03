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
package org.apache.hadoop.hive.serde2;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;

// mock class used for Hive < 0.11 compatibility
// the class was introduced in Hive 0.11 and has evolved ever since
public abstract class AbstractSerDe implements SerDe {

    protected String configErrors;

    public void initialize(Configuration configuration, Properties tableProperties, Properties partitionProperties)
            throws SerDeException {
        initialize(configuration, createOverlayedProperties(tableProperties, partitionProperties));
    }

    @Override
    public abstract void initialize(Configuration conf, Properties tbl) throws SerDeException;

    @Override
    public abstract Class<? extends Writable> getSerializedClass();

    @Override
    public abstract Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException;

    @Override
    public abstract SerDeStats getSerDeStats();

    @Override
    public abstract Object deserialize(Writable blob) throws SerDeException;

    @Override
    public abstract ObjectInspector getObjectInspector() throws SerDeException;

    // introduced in Hive 1.1.0
    /**
     * Get the error messages during the Serde configuration
     *
     * @return The error messages in the configuration which are empty if no error occurred
     */
    public String getConfigurationErrors() {
        return configErrors == null ? "" : configErrors;
    }

    // used here to preserve source(not just binary)-compatibility with Hive <0.14
    private static Properties createOverlayedProperties(Properties tblProps, Properties partProps) {
        Properties props = new Properties();
        props.putAll(tblProps);
        if (partProps != null) {
            props.putAll(partProps);
        }
        return props;
    }
}
