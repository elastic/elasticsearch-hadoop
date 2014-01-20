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
package org.elasticsearch.hadoop.serialization.field;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.serialization.ParsingUtils;
import org.elasticsearch.hadoop.serialization.json.JacksonJsonParser;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.ObjectUtils;

public class JsonFieldExtractors {

    private static Log log = LogFactory.getLog(JsonFieldExtractors.class);

    private final List<String> results = new ArrayList<String>(6);
    private int usedSlots = 0;
    private String[] paths;

    private FieldExtractor id, parent, routing, ttl, version, timestamp;

    private class PrecomputedFieldExtractor implements FieldExtractor {

        private final int slot;

        public PrecomputedFieldExtractor(int slot) {
            this.slot = slot;
        }

        @Override
        public String field(Object target) {
            return results.get(slot);
        }
    }

    private static class FixedFieldExtractor implements FieldExtractor {
        private final String value;

        public FixedFieldExtractor(String value) {
            this.value = value;
        }

        @Override
        public String field(Object target) {
            return value;
        }
    }

    public JsonFieldExtractors(Settings settings) {
        List<String> jsonPaths = new ArrayList<String>(6);

        id = init(settings.getMappingId(), jsonPaths);
        parent = init(settings.getMappingParent(), jsonPaths);
        routing = init(settings.getMappingRouting(), jsonPaths);
        ttl = init(settings.getMappingTtl(), jsonPaths);
        version = init(settings.getMappingVersion(), jsonPaths);
        timestamp = init(settings.getMappingTimestamp(), jsonPaths);

        paths = jsonPaths.toArray(new String[jsonPaths.size()]);
    }

    private FieldExtractor init(String setting, List<String> pathList) {
        if (setting != null) {
            String constant = initConstant(setting);
            if (constant != null) {
                return new FixedFieldExtractor(constant);
            }
            else {
                pathList.add(setting);
                return new PrecomputedFieldExtractor(usedSlots++);
            }
        }
        return null;
    }

    private String initConstant(String field) {
        if (field != null && field.startsWith("<") && field.endsWith(">")) {
            return field.substring(1, field.length() - 1);
        }
        return null;
    }

    public FieldExtractor id() {
        return id;
    }

    public FieldExtractor parent() {
        return parent;
    }

    public FieldExtractor routing() {
        return routing;
    }

    public FieldExtractor ttl() {
        return ttl;
    }

    public FieldExtractor version() {
        return version;
    }

    public FieldExtractor timestamp() {
        return timestamp;
    }

    public void process(BytesArray storage) {
        // no extractors, no lookups
        if (ObjectUtils.isEmpty(paths)) {
            return;
        }

        results.clear();

        if (log.isTraceEnabled()) {
            log.trace(String.format("About to look for paths [%s] in doc [%s]", Arrays.toString(paths), storage));
        }
        results.addAll(ParsingUtils.values(new JacksonJsonParser(storage.bytes(), 0, storage.length()), paths));
    }
}