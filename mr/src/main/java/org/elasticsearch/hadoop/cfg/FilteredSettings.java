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

import java.io.InputStream;
import java.util.Properties;

/**
 * Filtered settings are used for masking and hiding configuration values by key prefix. This is useful when you want
 * to ignore a namespace of a configuration, but still reference values from its state.
 */
public class FilteredSettings extends Settings {

    private final Settings parent;
    private final String excludePrefix;

    FilteredSettings(Settings parent, String excludePrefix) {
        this.parent = parent;
        if (excludePrefix.endsWith(".")) {
           this.excludePrefix = excludePrefix;
        } else {
            this.excludePrefix = excludePrefix + ".";
        }
    }

    private boolean validKey(String key) {
        return !key.startsWith(excludePrefix);
    }

    @Override
    public InputStream loadResource(String location) {
        return parent.loadResource(location);
    }

    @Override
    public Settings copy() {
        return new FilteredSettings(parent.copy(), excludePrefix);
    }

    @Override
    public String getProperty(String name) {
        if (validKey(name)) {
            return parent.getProperty(name);
        } else {
            return null;
        }
    }

    @Override
    public void setProperty(String name, String value) {
        // Ignore the update if it is an excluded key as it will not be readable after setting anyway
        if (validKey(name)) {
            parent.setProperty(name, value);
        }
    }

    @Override
    public Properties asProperties() {
        Properties parentSettings = parent.asProperties();
        Properties filteredProperties = new Properties();
        for (Object keyObject : parentSettings.keySet()) {
            String key = keyObject.toString();
            if (validKey(key)) {
                filteredProperties.put(key, parentSettings.getProperty(key));
            }
        }
        return filteredProperties;
    }
}
