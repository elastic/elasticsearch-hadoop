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

import org.junit.Test;

import static org.junit.Assert.*;

public class FilteredSettingsTest {

    @Test
    public void copy() {
        Properties baseProperties = new Properties();
        baseProperties.put("test.key.one", "value1");
        baseProperties.put("test.key.two", "value2");
        baseProperties.put("test.config.one", "confValue1");

        Settings base = new PropertiesSettings(baseProperties);
        Settings filtered = base.excludeFilter("test.config");
        Settings copied = filtered.copy();

        filtered.setProperty("test.key.three", "value3");

        assertNull("Copy operation was not a deep copy", copied.getProperty("test.key.three"));
    }

    @Test
    public void getProperty() {
        Properties baseProperties = new Properties();
        baseProperties.put("test.key.one", "value1");
        baseProperties.put("test.key.two", "value2");
        baseProperties.put("test.config.one", "confValue1");

        Settings base = new PropertiesSettings(baseProperties);
        Settings filtered = base.excludeFilter("test.config");

        assertNull("Property should be filtered out", filtered.getProperty("test.config.one"));
        assertEquals("Could not read unfiltered property", "value1", filtered.getProperty("test.key.one"));
    }

    @Test
    public void setProperty() {
        Properties baseProperties = new Properties();
        baseProperties.put("test.key.one", "value1");
        baseProperties.put("test.key.two", "value2");
        baseProperties.put("test.config.one", "confValue1");

        Settings base = new PropertiesSettings(baseProperties);
        Settings filtered = base.excludeFilter("test.config");

        filtered.setProperty("test.config.two", "confValue2");

        assertNull("Property should be filtered out", filtered.getProperty("test.config.one"));
        assertNull("Property should be filtered out", filtered.getProperty("test.config.two"));
        assertEquals("Could not read unfiltered property", "value1", filtered.getProperty("test.key.one"));
    }

    @Test
    public void asProperties() {
        Properties baseProperties = new Properties();
        baseProperties.put("test.key.one", "value1");
        baseProperties.put("test.key.two", "value2");
        baseProperties.put("test.config.one", "confValue1");

        Settings base = new PropertiesSettings(baseProperties);
        Settings filtered = base.excludeFilter("test.config");

        Properties filteredProperties = filtered.asProperties();

        assertNull("Property should be filtered out", filteredProperties.getProperty("test.config.one"));
        assertNull("Property should be filtered out", filteredProperties.getProperty("test.config.two"));
        assertEquals("Could not read unfiltered property", "value1", filteredProperties.getProperty("test.key.one"));
    }
}