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
package org.elasticsearch.hadoop.hive;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Basic class for field aliasing since Hive column names are restricted to: [0-9a-z_] and cannot start with numbers. Without any mapping, the alias will convert all hive columns to lower case.
 * Note that hive does this automatically for top-level fields but not for nested ones.
 */
class FieldAlias {

    private final Map<String, String> hiveToES;

    public FieldAlias() {
        this.hiveToES = new LinkedHashMap<String, String>();
    }

    public FieldAlias(Map<String, String> alias) {
        this.hiveToES = alias;
    }

    String toES(String string) {
        String alias = hiveToES.get(string);
        if (alias == null) {
            // ES fields are all lowercase
            alias = string.toLowerCase();
            hiveToES.put(string, alias);
        }
        return alias;
    }
}
