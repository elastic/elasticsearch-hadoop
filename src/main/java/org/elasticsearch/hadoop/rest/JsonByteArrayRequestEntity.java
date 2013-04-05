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

/**
 * Wrapper around byte arrays that are not fully filled up.
 */
class JsonByteArrayRequestEntity implements RequestEntity {

    private byte[] content;
    private int length;

    public JsonByteArrayRequestEntity(byte[] content, int length) {
        this.content = content;
        this.length = length;
    }

    @Override
    public long getContentLength() {
        return length;
    }

    @Override
    public void writeRequest(OutputStream out) throws IOException {
        out.write(content, 0, length);
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
