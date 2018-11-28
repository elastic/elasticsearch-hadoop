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

package org.elasticsearch.hadoop.gradle.fixture.hadoop.conf

/**
 * Performs organization and addition of settings for a collection of settings files
 */
class SettingsContainer {

    static class FileSettings {
        private Map<String, String> settings = [:]

        void addSetting(String key, String value) {
            settings.put(key, value)
        }

        void settings(Map<String, String> values) {
            settings.putAll(values)
        }

        Map<String, String> getSettings() {
            return settings
        }
    }

    private Map<String, String> globalSettings
    private Map<String, FileSettings> settingsFiles

    SettingsContainer() {
        this.globalSettings = [:]
        this.settingsFiles = [:]
    }

    void addSetting(String key, String value) {
        globalSettings.put(key, value)
    }

    Map<String, String> getSettings() {
        return globalSettings
    }

    FileSettings getFile(String filename) {
        return settingsFiles.computeIfAbsent(filename, {k -> new FileSettings()})
    }

    void combine(SettingsContainer other) {
        this.globalSettings.putAll(other.globalSettings)
        other.settingsFiles.forEach { String filename, FileSettings settings ->
            FileSettings existingContent = this.settingsFiles.putIfAbsent(filename, settings)
            if (existingContent != null) {
                existingContent.settings.putAll(settings.settings)
            }
        }
    }

    Map<String, String> flattenFile(String filename) {
        Map<String, String> flattened = [:]
        flattened.putAll(globalSettings)
        FileSettings fileSettings = settingsFiles.get(filename)
        if (fileSettings != null) {
            flattened.putAll(fileSettings.getSettings())
        }
        return flattened
    }
}
