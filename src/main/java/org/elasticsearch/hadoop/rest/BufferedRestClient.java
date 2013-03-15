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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Rest client performing high-level operations using buffers to improve performance.
 */
public class BufferedRestClient implements Closeable {

    // TODO: needs to be changed
    private int WRITE_BULK_SIZE = 3;
    private final Map<String, List<Object>> writeBatch = new LinkedHashMap<String, List<Object>>(WRITE_BULK_SIZE);
    private int currentBatchSize = 0;

    private RestClient client;

    public BufferedRestClient(String targetUri) {
        this(new RestClient(targetUri));
    }

    public BufferedRestClient(RestClient client) {
        this.client = client;
    }

    /**
     * Returns a pageable result to the given query.
     *
     * @param uri
     * @return
     */
    public QueryResult query(String uri) {
        return new QueryResult(client, uri);
    }

    /**
     * Writes the objects to index.
     *
     * @param index
     * @param object
     */
    public void addToIndex(String index, Object... object) {
        List<Object> list = writeBatch.get(index);
        if (list == null) {
            list = new ArrayList<Object>();
            writeBatch.put(index, list);
        }

        for (Object obj : object) {
            list.add(obj);
            currentBatchSize++;
        }

        if (currentBatchSize >= WRITE_BULK_SIZE) {
            flushBatch();
        }
    }

    private void flushBatch() {
        try {
            for (Map.Entry<String, List<Object>> entry : writeBatch.entrySet()) {
                client.addToIndex(entry.getKey(), entry.getValue());
            }

        } catch (IOException ex) {
            throw new IllegalStateException("Cannot add to index", ex);
        }
        writeBatch.clear();
        currentBatchSize = 0;
    }

    @Override
    public void close() {
        if (currentBatchSize > 0) {
            flushBatch();
        }
        client.close();
    }
}