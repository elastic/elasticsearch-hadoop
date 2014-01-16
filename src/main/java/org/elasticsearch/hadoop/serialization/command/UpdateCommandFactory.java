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
package org.elasticsearch.hadoop.serialization.command;

import java.util.List;

import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.serialization.field.FieldExtractor;
import org.elasticsearch.hadoop.util.Assert;

class UpdateCommandFactory extends AbstractCommandFactory {

    private final boolean UPSERT_DOC;


    public UpdateCommandFactory(Settings settings) {
        super(settings);
        UPSERT_DOC = settings.getUpsertDoc();
    }

    @Override
    protected String getOperation() {
        return ConfigurationOptions.ES_OPERATION_UPDATE;
    }

    @Override
    protected void writeBeforeObject(List<Object> pieces) {
        super.writeBeforeObject(pieces);
        pieces.add("{");
        if (UPSERT_DOC) {
            pieces.add("\"doc_as_upsert\":true,");
        }
        pieces.add("\"doc\":");
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
