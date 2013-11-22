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
package org.elasticsearch.hadoop.rest;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.httpclient.methods.RequestEntity;
import org.elasticsearch.hadoop.util.BytesArray;

/**
 * Wrapper around byte arrays that are not fully filled up.
 */
class BytesArrayRequestEntity implements RequestEntity {

    private final BytesArray ba;

    public BytesArrayRequestEntity(BytesArray ba) {
        this.ba = ba;
    }

    @Override
    public long getContentLength() {
        return ba.size();
    }

    @Override
    public void writeRequest(OutputStream out) throws IOException {
        out.write(ba.bytes(), 0, ba.size());
    }

    @Override
    public String getContentType() {
        return "application/json; charset=UTF-8";
    }

    @Override
    public boolean isRepeatable() {
        return true;
    }
}
