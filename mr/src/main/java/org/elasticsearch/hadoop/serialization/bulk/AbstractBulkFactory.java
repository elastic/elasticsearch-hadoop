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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.Resource;
import org.elasticsearch.hadoop.serialization.builder.ValueWriter;
import org.elasticsearch.hadoop.serialization.field.ConstantFieldExtractor;
import org.elasticsearch.hadoop.serialization.field.FieldExplainer;
import org.elasticsearch.hadoop.serialization.field.FieldExtractor;
import org.elasticsearch.hadoop.serialization.field.IndexExtractor;
import org.elasticsearch.hadoop.serialization.field.JsonFieldExtractors;
import org.elasticsearch.hadoop.serialization.field.WithoutQuotes;
import org.elasticsearch.hadoop.serialization.json.JacksonJsonGenerator;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.BytesArrayPool;
import org.elasticsearch.hadoop.util.FastByteArrayOutputStream;
import org.elasticsearch.hadoop.util.ObjectUtils;
import org.elasticsearch.hadoop.util.StringUtils;


abstract class AbstractBulkFactory implements BulkFactory {

    private static Log log = LogFactory.getLog(AbstractBulkFactory.class);

    private boolean jsonInput;
    private JsonFieldExtractors jsonExtractors;

    protected Settings settings;
    private ValueWriter valueWriter;
    // used when specifying an index pattern
    private IndexExtractor indexExtractor;
    private FieldExtractor idExtractor, parentExtractor, routingExtractor, versionExtractor, ttlExtractor,
            timestampExtractor, paramsExtractor;

    static final BytesArray QUOTE = new BytesArray("\"");

    class FieldWriter {
        final FieldExtractor extractor;
        final boolean addQuotesIfNecessary;
        final BytesArrayPool pool = new BytesArrayPool();

        FieldWriter(FieldExtractor extractor) {
            this.extractor = extractor;
            addQuotesIfNecessary = (extractor instanceof WithoutQuotes);
        }

        BytesArrayPool write(Object object) {
            pool.reset();

            Object value = extractor.field(object);
            if (value == FieldExtractor.NOT_FOUND) {
                String obj = (extractor instanceof FieldExplainer ? ((FieldExplainer) extractor).toString(object) : object.toString());
                throw new EsHadoopIllegalArgumentException(String.format("[%s] cannot extract value from object [%s]", extractor, obj));
            }

            if (value instanceof List) {
                List list = (List) value;
                for (int i = 0; i < list.size() - 1; i++) {
                    doWrite(list.get(i), false);
                }
                //
                doWrite(list.get(list.size() - 1), true);
            }
            // weird if/else to save one collection/iterator instance
            else {
                doWrite(value, true);
            }

            return pool;
        }

        void doWrite(Object value, boolean lookForQuotes) {
            // common-case - constants
            if (value instanceof String || jsonInput) {
                String val = value.toString();
                if (lookForQuotes && addQuotesIfNecessary) {
                    if (val.startsWith("[") || val.startsWith("{")) {
                        pool.get().bytes(val);
                    }
                    else {
                        pool.get().bytes(QUOTE);
                        pool.get().bytes(val);
                        pool.get().bytes(QUOTE);
                    }
                }
                else {
                    pool.get().bytes(val);
                }
            }
            else {
                BytesArray ba = pool.get();
                JacksonJsonGenerator generator = new JacksonJsonGenerator(new FastByteArrayOutputStream(ba));
                valueWriter.write(value, generator);
                generator.flush();
                generator.close();

                // jackson likely will add leading/trailing "" which are added down the pipeline so remove them
                // however that's not mandatory in case the source is a number (instead of a string)
                if ((lookForQuotes && !addQuotesIfNecessary) && ba.bytes()[ba.offset()] == '"') {
                    ba.size(Math.max(0, ba.length() - 2));
                    ba.offset(1);
                }
            }
        }
    }


    AbstractBulkFactory(Settings settings) {
        this.settings = settings;
        this.valueWriter = ObjectUtils.instantiate(settings.getSerializerValueWriterClassName(), settings);
        initFieldExtractors(settings);
    }

    private void initFieldExtractors(Settings settings) {
        jsonInput = settings.getInputAsJson();

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
                idExtractor = ObjectUtils.<FieldExtractor> instantiate(settings.getMappingIdExtractorClassName(), settings);
            }
            if (settings.getMappingParent() != null) {
                settings.setProperty(ConstantFieldExtractor.PROPERTY, settings.getMappingParent());
                parentExtractor = ObjectUtils.<FieldExtractor> instantiate(settings.getMappingParentExtractorClassName(), settings);
            }
            if (settings.getMappingRouting() != null) {
                settings.setProperty(ConstantFieldExtractor.PROPERTY, settings.getMappingRouting());
                routingExtractor = ObjectUtils.<FieldExtractor> instantiate(settings.getMappingRoutingExtractorClassName(), settings);
            }
            if (settings.getMappingTtl() != null) {
                settings.setProperty(ConstantFieldExtractor.PROPERTY, settings.getMappingTtl());
                ttlExtractor = ObjectUtils.<FieldExtractor> instantiate(settings.getMappingTtlExtractorClassName(), settings);
            }
            if (settings.getMappingVersion() != null) {
                settings.setProperty(ConstantFieldExtractor.PROPERTY, settings.getMappingVersion());
                versionExtractor = ObjectUtils.<FieldExtractor> instantiate(settings.getMappingVersionExtractorClassName(), settings);
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

            // param extractor
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
    }

    protected IndexExtractor index() {
        return indexExtractor;
    }

    protected FieldExtractor id() {
        return idExtractor;
    }

    protected FieldExtractor parent() {
        return parentExtractor;
    }

    protected FieldExtractor routing() {
        return routingExtractor;
    }

    protected FieldExtractor ttl() {
        return ttlExtractor;
    }

    protected FieldExtractor version() {
        return versionExtractor;
    }

    protected FieldExtractor timestamp() {
        return timestampExtractor;
    }

    protected FieldExtractor params() {
        return paramsExtractor;
    }

    @Override
    public BulkCommand createBulk() {
        List<Object> before = new ArrayList<Object>();
        writeBeforeObject(before);

        List<Object> after = new ArrayList<Object>();
        writeAfterObject(after);

        before = compact(before);
        after = compact(after);

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

    protected void writeAfterObject(List<Object> after) {
        after.add("\n");
    }

    private List<Object> compact(List<Object> list) {
        if (list == null || list.isEmpty()) {
            return null;
        }

        List<Object> compacted = new ArrayList<Object>();
        StringBuilder stringAccumulator = new StringBuilder();
        String lastString = null;
        boolean hasSeenIndexExtractor = false;
        for (Object object : list) {
            if (object instanceof FieldExtractor) {
                hasSeenIndexExtractor = object instanceof IndexExtractor;
                if (stringAccumulator.length() > 0) {
                    compacted.add(stringAccumulator.toString().getBytes(StringUtils.UTF_8));
                    stringAccumulator.setLength(0);
                    lastString = null;
                }
                compacted.add(createFieldWriter((FieldExtractor) object));
            }
            else {
                String str = object.toString();
                if (("\"".equals(lastString) || (lastString == null && hasSeenIndexExtractor)) && str.startsWith("\"")) {
                    stringAccumulator.append(",");
                }
                hasSeenIndexExtractor = false;
                lastString = str;
                stringAccumulator.append(str);
            }
        }

        if (stringAccumulator.length() > 0) {
            compacted.add(stringAccumulator.toString().getBytes(StringUtils.UTF_8));
        }
        return compacted;
    }

    protected Object createFieldWriter(FieldExtractor extractor) {
        return new FieldWriter(extractor);
    }

    protected void writeBeforeObject(List<Object> pieces) {
        startHeader(pieces);

        index(pieces);

        id(pieces);
        parent(pieces);
        routing(pieces);
        ttl(pieces);
        version(pieces);
        timestamp(pieces);

        otherHeader(pieces);
        endHeader(pieces);

        scriptParams(pieces);
    }

    private void startHeader(List<Object> pieces) {
        pieces.add("{\"" + getOperation() + "\":{");
    }

    private void endHeader(List<Object> pieces) {
        pieces.add("}}\n");
    }

    protected boolean index(List<Object> pieces) {
        if (index() != null) {
            pieces.add(index());
            return true;
        }
        return false;
    }

    protected boolean id(List<Object> pieces) {
        if (id() != null) {
            pieces.add("\"_id\":\"");
            pieces.add(id());
            pieces.add("\"");
            return true;
        }
        return false;
    }

    protected abstract String getOperation();

    protected boolean parent(List<Object> pieces) {
        if (parent() != null) {
            pieces.add("\"_parent\":\"");
            pieces.add(parent());
            pieces.add("\"");
            return true;
        }
        return false;
    }

    protected boolean routing(List<Object> pieces) {
        if (routing() != null) {
            pieces.add("\"_routing\":\"");
            pieces.add(routing());
            pieces.add("\"");
            return true;
        }
        return false;
    }

    protected boolean ttl(List<Object> pieces) {
        if (ttl() != null) {
            pieces.add("\"_ttl\":\"");
            pieces.add(ttl());
            pieces.add("\"");
            return true;
        }
        return false;
    }

    protected boolean version(List<Object> pieces) {
        if (version() != null) {
			if (settings.hasMappingVersionType()) {
				pieces.add("\"_version_type\":\"");
				pieces.add(settings.getMappingVersionType());
				pieces.add("\"");
			}
            pieces.add("\"_version\":\"");
            pieces.add(version());
            pieces.add("\"");
            return true;
        }
        return false;
    }

    protected boolean timestamp(List<Object> pieces) {
        if (timestamp() != null) {
            pieces.add("\"_timestamp\":\"");
            pieces.add(timestamp());
            pieces.add("\"");
            return true;
        }
        return false;
    }

    protected void otherHeader(List<Object> pieces) {
        // no-op
    }

    private boolean scriptParams(List<Object> pieces) {
        // handle json params first
        if (settings.hasUpdateScriptParamsJson()) {
            pieces.add("{\"params\":");
            pieces.add(settings.getUpdateScriptParamsJson().trim());
            pieces.add(",");
            return true;
        }
        if (params() != null) {
            pieces.add("{\"params\":{");
            pieces.add(params());
            pieces.add("},");
            return true;
        }
        return false;
    }
}