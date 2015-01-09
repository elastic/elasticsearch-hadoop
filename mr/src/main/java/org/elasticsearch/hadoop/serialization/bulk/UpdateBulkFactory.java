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
import org.elasticsearch.hadoop.serialization.field.FieldExtractor;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.StringUtils;

class UpdateBulkFactory extends AbstractBulkFactory {

    private final int RETRY_ON_FAILURE;
    private final String RETRY_HEADER;
    private final String SCRIPT;
    private final String SCRIPT_LANG;

    private final boolean HAS_SCRIPT, HAS_LANG, HAS_PARAMS;
    private final boolean UPSERT;

    public UpdateBulkFactory(Settings settings) {
        this(settings, false);
    }

    public UpdateBulkFactory(Settings settings, boolean upsert) {
        super(settings);

        UPSERT = upsert;
        RETRY_ON_FAILURE = settings.getUpdateRetryOnConflict();
        RETRY_HEADER = "\"_retry_on_conflict\":" + RETRY_ON_FAILURE + "";

        HAS_SCRIPT = StringUtils.hasText(settings.getUpdateScript());
        HAS_LANG = StringUtils.hasText(settings.getUpdateScriptLang());
        HAS_PARAMS = StringUtils.hasText(settings.getUpdateScriptParams());

        SCRIPT_LANG = "\"lang\":\"" + settings.getUpdateScriptLang() + "\",";
        SCRIPT = "\"script\":\"" + settings.getUpdateScript() + "\"";
    }

    @Override
    protected String getOperation() {
        return ConfigurationOptions.ES_OPERATION_UPDATE;
    }

    @Override
    protected void otherHeader(List<Object> pieces) {
        if (RETRY_ON_FAILURE > 0) {
            pieces.add(RETRY_HEADER);
        }
    }

    @Override
    protected void writeBeforeObject(List<Object> pieces) {
        super.writeBeforeObject(pieces);

		// when params are specified, the { is already added for readability purpose
        if (!settings.hasUpdateScriptParams() && !settings.hasUpdateScriptParamsJson()) {
            pieces.add("{");
        }

        if (HAS_SCRIPT) {
            if (HAS_LANG) {
                pieces.add(SCRIPT_LANG);
            }
            pieces.add(SCRIPT);
            if (UPSERT) {
                pieces.add(",\"upsert\":");
            }
        }
        else {
            if (UPSERT) {
                pieces.add("\"doc_as_upsert\":true,");
            }
            pieces.add("\"doc\":");
        }
    }

    @Override
    protected void writeAfterObject(List<Object> after) {
        after.add("}");
        super.writeAfterObject(after);
    }

    @Override
    protected FieldExtractor id() {
        FieldExtractor id = super.id();
        Assert.notNull(id, String.format("Operation [%s] requires an id but none was given/found", getOperation()));
        return id;
    }
}
