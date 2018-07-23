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
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;

/**
 * Provides a composite view of configurations, using a hierarchical model for property lookup as well as immutable
 * treatment of the underlying configurations. Each Composite has its own write-settings to accumulate mutations that
 * is checked for values first. After checking the write-settings, each settings object is checked in order that they
 * were specified.
 */
public class CompositeSettings extends Settings {

    private Settings writeSettings;
    private Deque<Settings> settingsList;

    /**
     * @param settings ordered input of settings, which will be checked for properties in the order that they are given.
     */
    public CompositeSettings(Collection<Settings> settings) {
        this.writeSettings = new PropertiesSettings();
        this.settingsList = new LinkedList<Settings>(settings);
    }

    private CompositeSettings(Settings writeSettings, LinkedList<Settings> settingsList) {
        this.writeSettings = writeSettings;
        this.settingsList = settingsList;
    }

    @Override
    public InputStream loadResource(String location) {
        return writeSettings.loadResource(location);
    }

    @Override
    public Settings copy() {
        Settings copiedWriteSettings = writeSettings.copy();
        LinkedList<Settings> copiedSettingsList = new LinkedList<Settings>();
        for (Settings settings : settingsList) {
            copiedSettingsList.add(settings.copy());
        }
        return new CompositeSettings(copiedWriteSettings, copiedSettingsList);
    }

    @Override
    public String getProperty(String name) {
        String value = writeSettings.getProperty(name);
        if (value == null) {
            Iterator<Settings> toCheck = settingsList.iterator();
            while (value == null && toCheck.hasNext()) {
                Settings next = toCheck.next();
                value = next.getProperty(name);
            }
        }
        return value;
    }

    @Override
    public void setProperty(String name, String value) {
        writeSettings.setProperty(name, value);
    }

    @Override
    public Properties asProperties() {
        Properties merged = new Properties();
        Iterator<Settings> settingsIterator = settingsList.descendingIterator();
        while (settingsIterator.hasNext()) {
            Settings next = settingsIterator.next();
            Properties props = next.asProperties();
            for (Object keyObject : props.keySet()) {
                String key = keyObject.toString();
                merged.setProperty(key, props.getProperty(key));
            }
        }
        Properties writeProperties = writeSettings.asProperties();
        for (Object keyObject : writeProperties.keySet()) {
            String key = keyObject.toString();
            merged.setProperty(key, writeProperties.getProperty(key));
        }
        return merged;
    }
}
