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
package org.elasticsearch.hadoop.util;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.elasticsearch.hadoop.cfg.InternalConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;

public abstract class SettingsUtils {

    private static List<String> qualifyHosts(String hosts, int defaultPort) {
        List<String> list = StringUtils.tokenize(hosts);
        for (int i = 0; i < list.size(); i++) {
            String host = list.get(i);
            list.set(i, qualifyHost(host, defaultPort));
        }
        return list;
    }

    private static String qualifyHost(String host, int defaultPort) {
        return (host.contains(":") ? host : host + ":" + defaultPort);
    }

    public static List<String> nodes(Settings settings) {
        return qualifyHosts(settings.getNodes(), settings.getPort());
    }

    public static Map<String, String> aliases(String definition) {
        List<String> aliases = StringUtils.tokenize(definition, ",");

        Map<String, String> aliasMap = new LinkedHashMap<String, String>();

        if (aliases != null) {
            for (String string : aliases) {
                // split alias
                string = string.trim();
                int index = string.indexOf(":");
                if (index > 0) {
                    String key = string.substring(0, index);
                    // save the lower case version as well since Hive does that for top-level keys
                    aliasMap.put(key, string.substring(index + 1));
                    aliasMap.put(key.toLowerCase(Locale.ENGLISH), string.substring(index + 1));
                }
            }
        }

        return aliasMap;
    }

    /**
     * Whether the settings indicate a ES 1.0RC1 (which introduces breaking changes) or lower (1.0.0.Beta2)
     *
     * @param settings
     * @return
     */
    public static boolean isEs10(Settings settings) {
        String version = settings.getProperty(InternalConfigurationOptions.INTERNAL_ES_VERSION);
        // assume ES 1.0 by default
        if (!StringUtils.hasText(version)) {
            return true;
        }

        return ("1.0.0.RC".compareTo(version) <= 0 || "1.0.0".equals(version));
    }
}
