/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticsearch.hadoop.pig;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.util.StringUtils;

/**
 * Basic class for field aliasing since Pig column names are restricted to: [0-9a-z_] and cannot start with numbers. Without any mapping, the alias will convert all Pig columns to lower case.
 * Note that Pig is case sensitive.
 */
class FieldAlias {

    private static final String COLUMN_ALIASES = "es.column.aliases";

    private final Map<String, String> pigToES;

    public FieldAlias() {
        this.pigToES = new LinkedHashMap<String, String>();
    }

    public FieldAlias(Map<String, String> alias) {
        this.pigToES = alias;
    }

    String toES(String string) {
        String alias = pigToES.get(string);
        if (alias == null) {
            // ES fields are all lowercase
            alias = string.toLowerCase();
            pigToES.put(string, alias);
        }
        return alias;
    }

    static FieldAlias load(Settings settings) {
        List<String> aliases = StringUtils.tokenize(settings.getProperty(COLUMN_ALIASES), ",");

        Map<String, String> aliasMap = new LinkedHashMap<String, String>();

        if (aliases != null) {
            for (String string : aliases) {
                // split alias
                string = string.trim();
                int index = string.indexOf(":");
                if (index > 0) {
                    String key = string.substring(0, index);
                    // save the lower case version as well to speed, lookup
                    aliasMap.put(key, string.substring(index + 1));
                    aliasMap.put(key.toLowerCase(), string.substring(index + 1));
                }
            }
        }

        return new FieldAlias(aliasMap);
    }
}