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
import java.util.Locale;
import java.util.Map;

/**
 * Simple class for providing basic aliases for fields.
 */
public class FieldAlias {

    private final Map<String, String> fieldToAlias;
    private final boolean caseInsensitive;

    public FieldAlias(boolean caseInsensitive) {
        this(new LinkedHashMap<String, String>(), caseInsensitive);
    }

    public FieldAlias(Map<String, String> alias, boolean caseInsensitive) {
        this.fieldToAlias = alias;
        this.caseInsensitive = caseInsensitive;
    }

    public String toES(String string) {
        String alias = fieldToAlias.get(string);
        if (alias == null) {
            alias = (caseInsensitive ? string.toLowerCase(Locale.ROOT) : string);
            fieldToAlias.put(string, alias);
        }
        return alias;
    }

    @Override
    public String toString() {
        return fieldToAlias.toString();
    }
}