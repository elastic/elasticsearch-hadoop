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

import org.elasticsearch.hadoop.EsHadoopUnsupportedOperationException;
import org.elasticsearch.hadoop.serialization.field.FieldExtractor;
import org.elasticsearch.hadoop.util.EsMajorVersion;

// specific implementation that relies on basic field extractors that are computed
// lazy per entity. Both the pool and extractors are meant to be reused.
public abstract class PerEntityPoolingMetadataExtractor implements MetadataExtractor {

    protected EsMajorVersion version;
    protected Object entity;

    public PerEntityPoolingMetadataExtractor(EsMajorVersion version) {
        this.version = version;
    }

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

    /**
     * A special field extractor meant to be used for metadata fields that are supported in
     * some versions of Elasticsearch, but not others. In the case that a metadata field is
     * unsupported for the configured version of Elasticsearch, this extractor which throws
     * exceptions for using unsupported metadata tags is returned instead of the regular one.
     */
    private static class UnsupportedMetadataFieldExtractor extends StaticFieldExtractor {
        private Metadata unsupportedMetadata;
        private EsMajorVersion version;

        public UnsupportedMetadataFieldExtractor(Metadata unsupportedMetadata, EsMajorVersion version) {
            this.unsupportedMetadata = unsupportedMetadata;
            this.version = version;
        }

        @Override
        public Object field(Object target) {
            throw new EsHadoopUnsupportedOperationException("Unsupported metadata tag [" + unsupportedMetadata.getName()
                    + "] for Elasticsearch version [" + version.toString() + "]. Bailing out...");
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
                fieldExtractor = createExtractorFor(metadata);
            }
            if (fieldExtractor.needsInit()) {
                fieldExtractor.setField(value);
            }
            pool.put(metadata, fieldExtractor);
        }
        return fieldExtractor;
    }

    /**
     * If a metadata tag is unsupported for this version of Elasticsearch then a
     */
    private StaticFieldExtractor createExtractorFor(Metadata metadata) {
        // Boot metadata tags that are not supported in this version of Elasticsearch
        if (version.onOrAfter(EsMajorVersion.V_6_X)) {
            // 6.0 Removed support for TTL and Timestamp metadata on index and update requests.
            switch (metadata) {
                case TTL: // Fall through
                case TIMESTAMP:
                    return new UnsupportedMetadataFieldExtractor(metadata, version);
            }
        }

        return new StaticFieldExtractor();
    }

    public abstract Object getValue(Metadata metadata);

    public void setObject(Object entity) {
        this.entity = entity;
    }
}
