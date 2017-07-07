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

import java.util.List;

import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.serialization.bulk.MetadataExtractor.Metadata;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.EsMajorVersion;
import org.elasticsearch.hadoop.util.StringUtils;

class UpdateBulkFactory extends AbstractBulkFactory {

    private final int RETRY_ON_FAILURE;
    private final String RETRY_HEADER;

    private final String SCRIPT_2X;

    private final String SCRIPT_5X;
    private final String SCRIPT_LANG_5X;

    private final String SCRIPT_1X;
    private final String SCRIPT_LANG_1X;

    private final boolean HAS_SCRIPT, HAS_LANG;
    private final boolean UPSERT;

    private final EsMajorVersion esMajorVersion;

    public UpdateBulkFactory(Settings settings, MetadataExtractor metaExtractor, EsMajorVersion esMajorVersion) {
        this(settings, false, metaExtractor, esMajorVersion);
    }

    public UpdateBulkFactory(Settings settings, boolean upsert, MetadataExtractor metaExtractor, EsMajorVersion esMajorVersion) {
        super(settings, metaExtractor);

        this.esMajorVersion = esMajorVersion;

        UPSERT = upsert;
        RETRY_ON_FAILURE = settings.getUpdateRetryOnConflict();
        RETRY_HEADER = "\"_retry_on_conflict\":" + RETRY_ON_FAILURE + "";

        HAS_SCRIPT = settings.hasUpdateScript();
        HAS_LANG = StringUtils.hasText(settings.getUpdateScriptLang());

        SCRIPT_LANG_5X = ",\"lang\":\"" + settings.getUpdateScriptLang() + "\"";
        SCRIPT_LANG_1X = "\"lang\":\"" + settings.getUpdateScriptLang() + "\",";

        if (HAS_SCRIPT) {
            if (StringUtils.hasText(settings.getUpdateScriptInline())) {
                // INLINE
                String source = "inline";
                if (esMajorVersion.onOrAfter(EsMajorVersion.V_6_X)) {
                    source = "source";
                }
                SCRIPT_5X = "{\"script\":{\"" + source + "\":\"" + settings.getUpdateScriptInline() + "\"";
                SCRIPT_2X = SCRIPT_5X;
                SCRIPT_1X = "\"script\":\"" + settings.getUpdateScriptInline() + "\"";
            } else if (StringUtils.hasText(settings.getUpdateScriptFile())) {
                // FILE
                SCRIPT_5X = "{\"script\":{\"file\":\"" + settings.getUpdateScriptFile() + "\"";
                SCRIPT_2X = SCRIPT_5X;
                SCRIPT_1X = "\"script_file\":\"" + settings.getUpdateScriptFile() + "\"";
            } else if (StringUtils.hasText(settings.getUpdateScriptStored())) {
                // STORED
                SCRIPT_5X = "{\"script\":{\"stored\":\"" + settings.getUpdateScriptStored() + "\"";
                SCRIPT_2X = "{\"script\":{\"id\":\"" + settings.getUpdateScriptStored() + "\"";
                SCRIPT_1X = "\"script_id\":\"" + settings.getUpdateScriptStored() + "\"";
            } else {
                throw new EsHadoopIllegalStateException("No update script found...");
            }
        } else {
            SCRIPT_5X = null;
            SCRIPT_2X = null;
            SCRIPT_1X = null;
        }
    }

    @Override
    protected String getOperation() {
        return ConfigurationOptions.ES_OPERATION_UPDATE;
    }

    @Override
    protected void otherHeader(List<Object> list, boolean commaMightBeNeeded) {
        if (RETRY_ON_FAILURE > 0) {
            if (commaMightBeNeeded) {
                list.add(",");
            }
            list.add(RETRY_HEADER);
        }
    }

    @Override
    protected void writeObjectHeader(List<Object> list) {
        super.writeObjectHeader(list);

        Object paramExtractor = getMetadataExtractorOrFallback(Metadata.PARAMS, getParamExtractor());

        if (esMajorVersion.on(EsMajorVersion.V_1_X)) {
            writeLegacyFormatting(list, paramExtractor);
        } else if (esMajorVersion.on(EsMajorVersion.V_2_X)) {
            writeStrictFormatting(list, paramExtractor, SCRIPT_2X);
        } else {
            writeStrictFormatting(list, paramExtractor, SCRIPT_5X);
        }
    }

    /**
     * Script format meant for versions 1.x to 2.x. Required format for 1.x and below.
     * @param list Consumer of snippets
     * @param paramExtractor Extracts parameters from documents or constants
     */
    private void writeLegacyFormatting(List<Object> list, Object paramExtractor) {
        if (paramExtractor != null) {
            list.add("{\"params\":");
            list.add(paramExtractor);
            list.add(",");
        }
        else {
            list.add("{");
        }

        if (HAS_SCRIPT) {
            /*
             * {
             *   "params": ...,
             *   "lang": "...",
             *   "script": "...",
             *   "upsert": {...}
             * }
             */
            if (HAS_LANG) {
                list.add(SCRIPT_LANG_1X);
            }
            list.add(SCRIPT_1X);
            if (UPSERT) {
                list.add(",\"upsert\":");
            }
        }
        else {
            /*
             * {
             *   "doc_as_upsert": true,
             *   "doc": {...}
             * }
             */
            if (UPSERT) {
                list.add("\"doc_as_upsert\":true,");
            }
            list.add("\"doc\":");
        }
    }

    /**
     * Script format meant for versions 2.x to 5.x. Required format for 5.x and above.
     * @param list Consumer of snippets
     * @param paramExtractor Extracts parameters from documents or constants
     */
    private void writeStrictFormatting(List<Object> list, Object paramExtractor, String scriptToUse) {
        if (HAS_SCRIPT) {
            /*
             * {
             *   "script":{
             *     "inline": "...",
             *     "lang": "...",
             *     "params": ...,
             *   },
             *   "upsert": {...}
             * }
             */
            list.add(scriptToUse);
            if (HAS_LANG) {
                list.add(SCRIPT_LANG_5X);
            }
            if (paramExtractor != null) {
                list.add(",\"params\":");
                list.add(paramExtractor);
            }
            list.add("}");
            if (UPSERT) {
                list.add(",\"upsert\":");
            }
        } else {
            /*
             * {
             *   "doc_as_upsert": true,
             *   "doc": {...}
             * }
             */
            list.add("{");
            if (UPSERT) {
                list.add("\"doc_as_upsert\":true,");
            }
            list.add("\"doc\":");
        }
    }

    @Override
    protected void writeObjectEnd(List<Object> after) {
        after.add("}");
        super.writeObjectEnd(after);
    }

    @Override
    protected boolean id(List<Object> list, boolean commaMightBeNeeded) {
        boolean added = super.id(list, commaMightBeNeeded);
        Assert.isTrue(added, String.format("Operation [%s] requires an id but none was given/found", getOperation()));
        return added;
    }
}