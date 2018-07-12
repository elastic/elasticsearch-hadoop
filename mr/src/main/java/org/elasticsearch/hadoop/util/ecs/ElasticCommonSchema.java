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

package org.elasticsearch.hadoop.util.ecs;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class ElasticCommonSchema {

    public static final String V0_992 = "0.992";

    private final String version;

    public ElasticCommonSchema() {
        this(V0_992);
    }

    public ElasticCommonSchema(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }

    public TemplateBuilder buildTemplate() {
        return new TemplateBuilder(this);
    }

    public static final class TemplateBuilder {
        private ElasticCommonSchema schema;
        private Map<String, String> labels;
        private Set<String> tags;
        private HostData host;
        private String eventCategory;
        private String eventType;

        private TemplateBuilder(ElasticCommonSchema schema) {
            this.schema = schema;
            this.labels = new LinkedHashMap<String, String>();
            this.tags = new LinkedHashSet<String>();
            this.host = null;
            this.eventCategory = null;
            this.eventType = null;
        }

        public TemplateBuilder addLabel(String label, String value) {
            this.labels.put(label, value);
            return this;
        }

        public TemplateBuilder addLabels(Map<String, String> labels) {
            this.labels.putAll(labels);
            return this;
        }

        public TemplateBuilder addTag(String tag) {
            this.tags.add(tag);
            return this;
        }

        public TemplateBuilder addTags(Iterable<String> tags) {
            for (String tag : tags) {
                addTag(tag);
            }
            return this;
        }

        public TemplateBuilder setEventCategory(String eventCategory) {
            this.eventCategory = eventCategory;
            return this;
        }

        public TemplateBuilder setEventType(String eventType) {
            this.eventType = eventType;
            return this;
        }

        public MessageTemplate build() {
            if (host == null) {
                host = HostData.getInstance();
            }

            return new MessageTemplate(schema, labels, tags, host, eventCategory, eventType);
        }
    }
}
