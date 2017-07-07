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

import java.util.ArrayList;
import java.util.List;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.Resource;
import org.elasticsearch.hadoop.serialization.builder.ValueWriter;
import org.elasticsearch.hadoop.serialization.bulk.MetadataExtractor.Metadata;
import org.elasticsearch.hadoop.serialization.dto.mapping.MappingUtils;
import org.elasticsearch.hadoop.serialization.field.ChainedFieldExtractor;
import org.elasticsearch.hadoop.serialization.field.ConstantFieldExtractor;
import org.elasticsearch.hadoop.serialization.field.FieldExplainer;
import org.elasticsearch.hadoop.serialization.field.FieldExtractor;
import org.elasticsearch.hadoop.serialization.field.IndexExtractor;
import org.elasticsearch.hadoop.serialization.field.JsonFieldExtractors;
import org.elasticsearch.hadoop.serialization.json.JacksonJsonGenerator;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.BytesArrayPool;
import org.elasticsearch.hadoop.util.FastByteArrayOutputStream;
import org.elasticsearch.hadoop.util.ObjectUtils;
import org.elasticsearch.hadoop.util.StringUtils;

public abstract class AbstractBulkFactory implements BulkFactory {

    private static Log log = LogFactory.getLog(AbstractBulkFactory.class);

    protected Settings settings;

    private final boolean jsonInput;
    private final boolean isStatic;

    private final MetadataExtractor metaExtractor;
    // used when specifying an index pattern
    private IndexExtractor indexExtractor;
    private FieldExtractor idExtractor,
    typeExtractor,
    parentExtractor,
    routingExtractor,
    versionExtractor,
    ttlExtractor,
    timestampExtractor,
    paramsExtractor;

    private final FieldExtractor versionTypeExtractor = new FieldExtractor() {

        private Object value;

        @Override
        public Object field(Object target) {
            // lazy init to have the settings in place
            if (value == null) {
                value = new RawJson(StringUtils.toJsonString(settings.getMappingVersionType()));
            }
            return value;
        }
    };

    private JsonFieldExtractors jsonExtractors;

    private final ValueWriter valueWriter;

    class FieldWriter {
        final String headerValue;
        final FieldExtractor extractor;
        final BytesArrayPool pool = new BytesArrayPool();

        FieldWriter(FieldExtractor extractor) {
            this.headerValue = null;
            this.extractor = extractor;
        }

        FieldWriter(String header, FieldExtractor extractor) {
            this.headerValue = header;
            this.extractor = extractor;
        }

        BytesArrayPool write(Object object) {
            pool.reset();

            Object value = extractor.field(object);
            if (value == FieldExtractor.NOT_FOUND) {
                String obj = (extractor instanceof FieldExplainer ? ((FieldExplainer) extractor).toString(object) : object.toString());
                throw new EsHadoopIllegalArgumentException(String.format("[%s] cannot extract value from entity [%s] | instance [%s]", extractor, obj.getClass(), obj));
            } else if (value == FieldExtractor.SKIP) {
                // Skip it
                return pool;
            }

            if (headerValue != null) {
                pool.get().bytes(headerValue);
            }

            if (value instanceof List) {
                for (Object val : (List) value) {
                    doWrite(val);
                }
            }
            // if/else to save one collection/iterator instance
            else {
                doWrite(value);
            }

            return pool;
        }

        void doWrite(Object value) {
            // common-case - constants or JDK types
            if (value instanceof String || jsonInput || value instanceof Number || value instanceof Boolean || value == null) {
                String valueString = (value == null ? "null" : value.toString());
                if (value instanceof String && !jsonInput) {
                    valueString = StringUtils.toJsonString(valueString);
                }

                pool.get().bytes(valueString);
            }
            
            else if (value instanceof Date) {
                String valueString = (value == null ? "null": Long.toString(((Date) value).getTime()));
                pool.get().bytes(valueString);
            }
            
            else if (value instanceof RawJson) {
                pool.get().bytes(((RawJson) value).json());
            }
            // library specific type - use the value writer (a bit overkill but handles collections/arrays properly)
            else {
                BytesArray ba = pool.get();
                JacksonJsonGenerator generator = new JacksonJsonGenerator(new FastByteArrayOutputStream(ba));
                valueWriter.write(value, generator);
                generator.flush();
                generator.close();
            }
        }

        @Override
        public String toString() {
            return "FieldWriter for " + extractor;
        }
    }

    interface DynamicContentRef {
        List<Object> getDynamicContent();
    }

    public class DynamicHeaderRef implements DynamicContentRef {
        final List<Object> header = new ArrayList<Object>();

        public List<Object> getDynamicContent() {
            header.clear();
            writeObjectHeader(header);
            return compact(header);
        }
    }

    public class DynamicEndRef implements DynamicContentRef {
        final List<Object> end = new ArrayList<Object>();

        public List<Object> getDynamicContent() {
            end.clear();
            writeObjectEnd(end);
            return compact(end);
        }
    }


    AbstractBulkFactory(Settings settings, MetadataExtractor metaExtractor) {
        this.settings = settings;
        this.valueWriter = ObjectUtils.instantiate(settings.getSerializerValueWriterClassName(), settings);
        this.metaExtractor = metaExtractor;

        jsonInput = settings.getInputAsJson();
        isStatic = metaExtractor == null;
        initExtractorsFromSettings(settings);
    }

    private void initExtractorsFromSettings(final Settings settings) {
        if (jsonInput) {
            if (log.isDebugEnabled()) {
                log.debug("JSON input; using internal field extractor for efficient parsing...");
            }

            jsonExtractors = new JsonFieldExtractors(settings);
            indexExtractor = jsonExtractors.indexAndType();

            idExtractor = jsonExtractors.id();
            parentExtractor = jsonExtractors.parent();
            routingExtractor = jsonExtractors.routing();
            versionExtractor = jsonExtractors.version();
            ttlExtractor = jsonExtractors.ttl();
            timestampExtractor = jsonExtractors.timestamp();
            paramsExtractor = jsonExtractors.params();
        }
        else {
            // init extractors (if needed)
            if (settings.getMappingId() != null) {
                settings.setProperty(ConstantFieldExtractor.PROPERTY, settings.getMappingId());
                idExtractor = ObjectUtils.<FieldExtractor> instantiate(settings.getMappingIdExtractorClassName(),
                        settings);
            }
            if (settings.getMappingParent() != null) {
                settings.setProperty(ConstantFieldExtractor.PROPERTY, settings.getMappingParent());
                parentExtractor = ObjectUtils.<FieldExtractor> instantiate(
                        settings.getMappingParentExtractorClassName(), settings);
            }
            // Two different properties can satisfy the routing field extraction
            ChainedFieldExtractor.NoValueHandler routingResponse = ChainedFieldExtractor.NoValueHandler.SKIP;
            List<FieldExtractor> routings = new ArrayList<FieldExtractor>(2);
            if (settings.getMappingRouting() != null) {
                settings.setProperty(ConstantFieldExtractor.PROPERTY, settings.getMappingRouting());
                FieldExtractor extractor = ObjectUtils.<FieldExtractor> instantiate(
                        settings.getMappingRoutingExtractorClassName(), settings);
                // If we specify a routing field, return NOT_FOUND if we ultimately cannot find one instead of skipping
                routingResponse = ChainedFieldExtractor.NoValueHandler.NOT_FOUND;
                routings.add(extractor);
            }
            if (settings.getMappingJoin() != null) {
                // make sure to append the parent sub-field
                settings.setProperty(ConstantFieldExtractor.PROPERTY, MappingUtils.joinParentField(settings));
                FieldExtractor extractor = ObjectUtils.<FieldExtractor>instantiate(
                        settings.getMappingJoinExtractorClassName(), settings);
                routings.add(extractor);
            }
            if (routings.size() != 0) {
                routingExtractor = new ChainedFieldExtractor(routings, routingResponse);
            }

            if (settings.getMappingTtl() != null) {
                settings.setProperty(ConstantFieldExtractor.PROPERTY, settings.getMappingTtl());
                ttlExtractor = ObjectUtils.<FieldExtractor> instantiate(settings.getMappingTtlExtractorClassName(),
                        settings);
            }
            if (settings.getMappingVersion() != null) {
                settings.setProperty(ConstantFieldExtractor.PROPERTY, settings.getMappingVersion());
                versionExtractor = ObjectUtils.<FieldExtractor> instantiate(
                        settings.getMappingVersionExtractorClassName(), settings);
            }
            if (settings.getMappingTimestamp() != null) {
                settings.setProperty(ConstantFieldExtractor.PROPERTY, settings.getMappingTimestamp());
                timestampExtractor = ObjectUtils.<FieldExtractor> instantiate(
                        settings.getMappingTimestampExtractorClassName(), settings);
            }

            // create adapter
            IndexExtractor iformat = ObjectUtils.<IndexExtractor> instantiate(settings.getMappingIndexExtractorClassName(), settings);
            iformat.compile(new Resource(settings, false).toString());

            if (iformat.hasPattern()) {
                indexExtractor = iformat;
            }

            if (settings.hasUpdateScriptParams()) {
                settings.setProperty(ConstantFieldExtractor.PROPERTY, settings.getUpdateScriptParams());
                paramsExtractor = ObjectUtils.instantiate(settings.getMappingParamsExtractorClassName(), settings);
            }

            if (log.isTraceEnabled()) {
                log.trace(String.format("Instantiated value writer [%s]", valueWriter));
                if (idExtractor != null) {
                    log.trace(String.format("Instantiated id extractor [%s]", idExtractor));
                }
                if (parentExtractor != null) {
                    log.trace(String.format("Instantiated parent extractor [%s]", parentExtractor));
                }
                if (routingExtractor != null) {
                    log.trace(String.format("Instantiated routing extractor [%s]", routingExtractor));
                }
                if (ttlExtractor != null) {
                    log.trace(String.format("Instantiated ttl extractor [%s]", ttlExtractor));
                }
                if (versionExtractor != null) {
                    log.trace(String.format("Instantiated version extractor [%s]", versionExtractor));
                }
                if (timestampExtractor != null) {
                    log.trace(String.format("Instantiated timestamp extractor [%s]", timestampExtractor));
                }
                if (paramsExtractor != null) {
                    log.trace(String.format("Instantiated params extractor [%s]", paramsExtractor));
                }
            }
        }

        // json params override other extractors
        if (settings.hasUpdateScriptParamsJson()) {
            paramsExtractor = new FieldExtractor() {
                @Override
                public Object field(Object target) {
                    return new RawJson(settings.getUpdateScriptParamsJson().trim());
                }
            };
        }
    }


    class DynamicFieldExtractor implements FieldExtractor {

        private final List<Object> before = new ArrayList<Object>();

        @Override
        public Object field(Object target) {
            before.clear();
            writeObjectHeader(before);
            return compact(before);
        }
    }

    @Override
    public BulkCommand createBulk() {
        List<Object> before = new ArrayList<Object>();
        List<Object> after = new ArrayList<Object>();

        if (!isStatic) {
            before.add(new DynamicHeaderRef());
            after.add(new DynamicEndRef());
        }
        else {
            writeObjectHeader(before);
            before = compact(before);
            writeObjectEnd(after);
            after = compact(after);
        }

        boolean isScriptUpdate = settings.hasUpdateScript();
        // compress pieces
        if (jsonInput) {
            if (isScriptUpdate) {
                return new JsonScriptTemplateBulk(before, after, jsonExtractors, settings);
            }
            return new JsonTemplatedBulk(before, after, jsonExtractors, settings);
        }
        if (isScriptUpdate) {
            return new ScriptTemplateBulk(settings, before, after, valueWriter);
        }
        return new TemplatedBulk(before, after, valueWriter);
    }

    // write action & metadata header
    protected void writeObjectHeader(List<Object> list) {
        // action
        list.add("{\"" + getOperation() + "\":{");

        // flag indicating whether a comma needs to be added between fields
        boolean commaMightBeNeeded = false;

        commaMightBeNeeded = addExtractorOrDynamicValue(list, getMetadataExtractorOrFallback(Metadata.INDEX, indexExtractor), "", commaMightBeNeeded);
        commaMightBeNeeded = addExtractorOrDynamicValue(list, getMetadataExtractorOrFallback(Metadata.TYPE, typeExtractor), "\"_type\":", commaMightBeNeeded);
        commaMightBeNeeded = id(list, commaMightBeNeeded);
        commaMightBeNeeded = addExtractorOrDynamicValue(list, getMetadataExtractorOrFallback(Metadata.PARENT, parentExtractor), "\"_parent\":", commaMightBeNeeded);
        commaMightBeNeeded = addExtractorOrDynamicValueAsFieldWriter(list, getMetadataExtractorOrFallback(Metadata.ROUTING, routingExtractor), "\"_routing\":", commaMightBeNeeded);
        commaMightBeNeeded = addExtractorOrDynamicValue(list, getMetadataExtractorOrFallback(Metadata.TTL, ttlExtractor), "\"_ttl\":", commaMightBeNeeded);
        commaMightBeNeeded = addExtractorOrDynamicValue(list, getMetadataExtractorOrFallback(Metadata.TIMESTAMP, timestampExtractor), "\"_timestamp\":", commaMightBeNeeded);

        // version & version_type fields
        Object versionField = getMetadataExtractorOrFallback(Metadata.VERSION, versionExtractor);
        if (versionField != null) {
            if (commaMightBeNeeded) {
                list.add(",");
                commaMightBeNeeded = false;
            }
            commaMightBeNeeded = true;
            list.add("\"_version\":");
            list.add(versionField);

            // version_type - only needed when a version is specified
            Object versionTypeField = getMetadataExtractorOrFallback(Metadata.VERSION_TYPE, versionTypeExtractor);
            if (versionTypeField != null) {
                if (commaMightBeNeeded) {
                    list.add(",");
                    commaMightBeNeeded = false;
                }
                commaMightBeNeeded = true;
                list.add("\"_version_type\":");
                list.add(versionTypeField);
            }
        }

        // useful for update command
        otherHeader(list, commaMightBeNeeded);
        list.add("}}\n");
    }

    protected boolean id(List<Object> list, boolean commaMightBeNeeded) {
        return addExtractorOrDynamicValue(list, getMetadataExtractorOrFallback(Metadata.ID, idExtractor), "\"_id\":", commaMightBeNeeded);
    }

    /**
     * If extractor is present, this will add the header to the template, followed by the extractor.
     * If a comma is needed, the comma will be inserted before the header.
     *
     * @return true if a comma may be needed on the next call.
     */
    private boolean addExtractorOrDynamicValue(List<Object> list, Object extractor, String header, boolean commaMightBeNeeded) {
        if (extractor != null) {
            if (commaMightBeNeeded) {
                list.add(",");
            }
            list.add(header);
            list.add(extractor);
            return true;
        }
        return commaMightBeNeeded;
    }

    /**
     * If extractor is present, this will combine the header and extractor into a FieldWriter,
     * allowing the FieldWriter to determine when and if to write the header value based on the
     * given document's data. If a comma is needed, it is appended to the header string before
     * being passed to the FieldWriter.
     *
     * @return true if a comma may be needed on the next call
     */
    private boolean addExtractorOrDynamicValueAsFieldWriter(List<Object> list, FieldExtractor extractor, String header, boolean commaMightBeNeeded) {
        if (extractor != null) {
            String head = header;
            if (commaMightBeNeeded) {
                head = "," + head;
            }
            list.add(new FieldWriter(head, extractor));
            return true;
        }
        return commaMightBeNeeded;
    }

    protected void otherHeader(List<Object> list, boolean commaMightBeNeeded) {
        // no-op
    }

    /**
     * Get the extractor for a given field, trying first one from a MetadataExtractor, and failing that,
     * falling back to the provided 'static' one
     */
    protected FieldExtractor getMetadataExtractorOrFallback(Metadata meta, FieldExtractor fallbackExtractor) {
        if (metaExtractor != null) {
            FieldExtractor metaFE = metaExtractor.get(meta);
            if (metaFE != null) {
                return metaFE;
            }
        }
        return fallbackExtractor;
    }

    protected abstract String getOperation();

    protected void writeObjectEnd(List<Object> list) {
        list.add("\n");
    }

    // optimization method used when dealing with 'static' extractors
    // concatenates all the strings to minimize the amount of data needed for construction
    // TODO This does not compact a list of objects - it is COMPILLING it, and changing the structure. Make this more explicit
    private List<Object> compact(List<Object> list) {
        if (list == null || list.isEmpty()) {
            return null;
        }

        List<Object> compacted = new ArrayList<Object>();
        StringBuilder stringAccumulator = new StringBuilder();
        for (Object object : list) {
            if (object instanceof FieldWriter) {
                // If a field writer is in the stream, then something may have specific writing logic tied to it.
                // pass it along same as field extractor
                if (stringAccumulator.length() > 0) {
                    compacted.add(new BytesArray(stringAccumulator.toString()));
                    stringAccumulator.setLength(0);
                }
                compacted.add(object);
            }
            else if (object instanceof FieldExtractor) {
                if (stringAccumulator.length() > 0) {
                    compacted.add(new BytesArray(stringAccumulator.toString()));
                    stringAccumulator.setLength(0);
                }
                compacted.add(new FieldWriter((FieldExtractor) object));
            }
            else {
                stringAccumulator.append(object.toString());
            }
        }

        if (stringAccumulator.length() > 0) {
            compacted.add(new BytesArray(stringAccumulator.toString()));
        }
        return compacted;
    }

    protected FieldExtractor getParamExtractor() {
        return paramsExtractor;
    }
}
