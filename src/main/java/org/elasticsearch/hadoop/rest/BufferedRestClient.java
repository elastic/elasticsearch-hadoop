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

import org.apache.commons.lang.Validate;
import org.apache.hadoop.io.Writable;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.util.WritableUtils;

/**
 * Rest client performing high-level operations using buffers to improve performance. Stateful in that once created, it is used to perform updates against the same index.
 */
public class BufferedRestClient implements Closeable {

    // TODO: make this configurable
    private final byte[] buffer;
    private final int bufferEntriesThreshold;

    private int bufferSize = 0;
    private int bufferEntries = 0;
    private boolean requiresRefreshAfterBulk = false;
    private boolean executedBulkWrite = false;

    private ObjectMapper mapper = new ObjectMapper();

    private RestClient client;
    private String index;

    public BufferedRestClient(Settings settings) {
        this.client = new RestClient(settings);
        this.index = settings.getTargetResource();

        buffer = new byte[settings.getBatchSizeInBytes()];
        bufferEntriesThreshold = settings.getBatchSizeInEntries();
        requiresRefreshAfterBulk = settings.getBatchRefreshAfterWrite();
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
    public void addToIndex(Object object) throws IOException {
        Validate.notEmpty(index, "no index given");

        Object d = (object instanceof Writable ? WritableUtils.fromWritable((Writable) object) : object);

        StringBuilder sb = new StringBuilder();

        sb.append("{\"index\":{}}\n");
        sb.append(mapper.writeValueAsString(d));
        sb.append("\n");

        byte[] data = sb.toString().getBytes("UTF-8");

        // make some space first
        if (data.length + bufferSize >= buffer.length) {
            flushBatch();
        }

        System.arraycopy(data, 0, buffer, bufferSize, data.length);
        bufferSize += data.length;
        bufferEntries++;

        if (bufferEntriesThreshold > 0 && bufferEntries >= bufferEntriesThreshold) {
            flushBatch();
        }
    }

    private void flushBatch() throws IOException {
        client.bulk(index, buffer, bufferSize);
        bufferSize = 0;
        bufferEntries = 0;
        executedBulkWrite = true;
    }

    @Override
    public void close() throws IOException {
        if (bufferSize > 0) {
            flushBatch();
        }
        if (requiresRefreshAfterBulk && executedBulkWrite) {
            // refresh batch
            client.refresh(index);
        }
        client.close();
    }
}