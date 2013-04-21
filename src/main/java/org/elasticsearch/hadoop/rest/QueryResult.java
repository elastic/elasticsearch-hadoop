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
import java.util.Map;

/**
 * Result streaming data from a ElasticSearch query. Performs batching underneath to retrieve data in chunks.
 */
public class QueryResult implements Iterator<Map<String, Object>>, Closeable {

    private RestClient client;
    private String query;
    private List<Map<String, Object>> batch = Collections.emptyList();
    private boolean finished = false;

    private int batchIndex = 0;
    private int offset = 0;

    // number of records to get in one call
    private final int BATCH_SIZE = 1;

    QueryResult(RestClient client, String query) {
        this.client = client;
        this.query = query;
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

        // check if a new batch is required
        if (batch.isEmpty() || batchIndex >= BATCH_SIZE) {
            offset += batchIndex;
            try {
                batch = client.query(query, offset, BATCH_SIZE);
            } catch (IOException ex) {
                throw new IllegalStateException("Error perfoming query [" + ex.getMessage() + "]", ex);
            }
            if (batch.isEmpty()) {
                finished = true;
                return false;
            }
            // reset index
            batchIndex = 0;
        }
        return true;
    }

    @Override
    public Map<String, Object> next() {
        return batch.get(batchIndex++);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("read-only operator");
    }
}