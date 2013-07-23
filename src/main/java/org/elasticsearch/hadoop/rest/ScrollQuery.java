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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.elasticsearch.hadoop.serialization.ScrollReader;

/**
 * Result streaming data from a ElasticSearch query using the scan/scroll. Performs batching underneath to retrieve data in chunks.
 */
public class ScrollQuery implements Iterator<Object>, Closeable {

    private BufferedRestClient client;
    private String scrollId;
    private List<Object[]> batch = Collections.emptyList();
    private boolean finished = false;

    private int batchIndex = 0;
    private long read = 0;
    private long size;

    private final ScrollReader reader;

    ScrollQuery(BufferedRestClient client, String scrollId, long size, ScrollReader reader) {
        this.client = client;
        this.scrollId = scrollId;
        this.size = size;
        this.reader = reader;
    }

    @Override
    public void close() throws IOException {
        finished = true;
        batch = Collections.emptyList();
        client.close();
    }

    @Override
    public boolean hasNext() {
        if (finished)
            return false;

        if (batch.isEmpty() || batchIndex >= batch.size()) {
            if (read >= size) {
                finished = true;
                return false;
            }

            try {
                batch = client.scroll(scrollId, reader);
            } catch (IOException ex) {
                throw new IllegalStateException("Cannot retrieve scroll [" + scrollId + "]", ex);
            }
            read += batch.size();
            if (batch.isEmpty()) {
                finished = true;
                return false;
            }
            // reset index
            batchIndex = 0;
        }

        return true;
    }

    public long getSize() {
        return size;
    }

    public long getRead() {
        return read;
    }

    @Override
    public Object[] next() {
        return batch.get(batchIndex++);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("read-only operator");
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ScrollQuery [scrollId=").append(scrollId).append("]");
        return builder.toString();
    }
}