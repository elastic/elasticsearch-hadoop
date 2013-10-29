/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticsearch.hadoop.serialization;

import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.StringUtils;

public class UpdateCommand extends AbstractCommand {

    private final boolean UPSERT_DOC;

    private final byte[] HEADER_PREFIX = ("{\"" + ConfigurationOptions.ES_OPERATION_UPDATE + "\":{\"_id\":\"").getBytes(StringUtils.UTF_8);
    private final byte[] HEADER_SUFFIX = ("\"}}\n{\"doc\":").getBytes(StringUtils.UTF_8);
    private final byte[] BODY_NO_DOC_UPSERT_SUFFIX = ("}").getBytes(StringUtils.UTF_8);
    private final byte[] BODY_DOC_UPSERT_SUFFIX = (",\"doc_as_upsert\":true}").getBytes(StringUtils.UTF_8);


    UpdateCommand(Settings settings) {
        super(settings);
        UPSERT_DOC = settings.getUpsertDoc();
    }

    @Override
    protected boolean isIdRequired() {
        return true;
    }

    @Override
    protected byte[] headerPrefix() {
        return HEADER_PREFIX;
    }

    @Override
    protected byte[] headerSuffix() {
        return HEADER_SUFFIX;
    }

    @Override
    protected byte[] header() {
        throw new UnsupportedOperationException("Id is required but none was given");
    }


    @Override
    public int prepare(Object object) {
        return super.prepare(object) + (UPSERT_DOC ? BODY_DOC_UPSERT_SUFFIX.length : BODY_NO_DOC_UPSERT_SUFFIX.length);
    }

    @Override
    protected void writeSource(Object object, BytesArray buffer) {
        // write document
        super.writeSource(object, buffer);
        // append body
        if (UPSERT_DOC) {
            copyIntoBuffer(BODY_DOC_UPSERT_SUFFIX, buffer);
        }
        else {
            copyIntoBuffer(BODY_NO_DOC_UPSERT_SUFFIX, buffer);
        }
    }
}