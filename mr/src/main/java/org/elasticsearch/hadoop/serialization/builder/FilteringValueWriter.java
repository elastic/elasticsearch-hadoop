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
package org.elasticsearch.hadoop.serialization.builder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.serialization.SettingsAware;
import org.elasticsearch.hadoop.serialization.field.FieldFilter;
import org.elasticsearch.hadoop.serialization.field.FieldFilter.NumberedInclude;
import org.elasticsearch.hadoop.util.StringUtils;

public abstract class FilteringValueWriter<T> implements ValueWriter<T>, SettingsAware {

    private List<NumberedInclude> includes;
    private List<String> excludes;

    @Override
    public void setSettings(Settings settings) {
        List<String> includeAsStrings = StringUtils.tokenize(settings.getMappingIncludes());
        includes = (includeAsStrings.isEmpty() ? Collections.<NumberedInclude> emptyList() : new ArrayList<NumberedInclude>(includeAsStrings.size()));
        for (String include : includeAsStrings) {
            includes.add(new NumberedInclude(include));
        }
        excludes = StringUtils.tokenize(settings.getMappingExcludes());
    }

    protected boolean shouldKeep(String parentField, String name) {
        name = StringUtils.hasText(parentField) ? parentField + "." + name : name;
        return FieldFilter.filter(name, includes, excludes).matched;
    }
}
