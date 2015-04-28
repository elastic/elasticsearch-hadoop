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
package org.elasticsearch.hadoop.serialization.bulk;

import java.util.EnumMap;

import org.elasticsearch.hadoop.serialization.field.FieldExtractor;

// specific implementation that relies on basic field extractors that are computed
// lazy per entity. Both the pool and extractors are meant to be reused.
public abstract class PerEntityPoolingMetadataExtractor implements MetadataExtractor {

    protected Object entity;

    private static class StaticFieldExtractor implements FieldExtractor {
        private Object field;
        private boolean needsInit = true;

        @Override
        public Object field(Object target) {
            return field;
        }

        public void setField(Object field) {
            this.field = field;
            this.needsInit = true;
        }

        public boolean needsInit() {
            return needsInit;
        }
    }

    private final EnumMap<Metadata, StaticFieldExtractor> pool = new EnumMap<Metadata, StaticFieldExtractor>(Metadata.class);

    public void reset() {
        entity = null;
    }

    @Override
    public FieldExtractor get(Metadata metadata) {
        StaticFieldExtractor fieldExtractor = pool.get(metadata);

        if (fieldExtractor == null || fieldExtractor.needsInit()) {
            Object value = getValue(metadata);
            if (value == null) {
                return null;
            }
            if (fieldExtractor == null) {
                fieldExtractor = new StaticFieldExtractor();
            }
            if (fieldExtractor.needsInit()) {
                fieldExtractor.setField(value);
            }
            pool.put(metadata, fieldExtractor);
        }
        return fieldExtractor;
    }

    public abstract Object getValue(Metadata metadata);

    public void setObject(Object entity) {
        this.entity = entity;
    }
}
