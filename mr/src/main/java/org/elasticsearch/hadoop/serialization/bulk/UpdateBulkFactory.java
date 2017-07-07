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

import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.serialization.bulk.MetadataExtractor.Metadata;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.EsMajorVersion;
import org.elasticsearch.hadoop.util.StringUtils;

class UpdateBulkFactory extends AbstractBulkFactory {

    private final int RETRY_ON_FAILURE;
    private final String RETRY_HEADER;

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

        HAS_SCRIPT = StringUtils.hasText(settings.getUpdateScript());
        HAS_LANG = StringUtils.hasText(settings.getUpdateScriptLang());

        SCRIPT_LANG_5X = ",\"lang\":\"" + settings.getUpdateScriptLang() + "\"";
        SCRIPT_5X = "{\"script\":{\"inline\":\"" + settings.getUpdateScript() + "\"";

        SCRIPT_LANG_1X = "\"lang\":\"" + settings.getUpdateScriptLang() + "\",";
        SCRIPT_1X = "\"script\":\"" + settings.getUpdateScript() + "\"";
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

        if (esMajorVersion.after(EsMajorVersion.V_1_X)) {
            writeStrictFormatting(list, paramExtractor);
        } else {
            writeLegacyFormatting(list, paramExtractor);
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
            if (HAS_LANG) {
                list.add(SCRIPT_LANG_1X);
            }
            list.add(SCRIPT_1X);
            if (UPSERT) {
                list.add(",\"upsert\":");
            }
        }
        else {
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
    private void writeStrictFormatting(List<Object> list, Object paramExtractor) {
        if (HAS_SCRIPT) {
            list.add(SCRIPT_5X);
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
        }
        else {
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