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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.hadoop.util.WritableUtils;

/**
 * Rest client performing high-level operations using buffers to improve performance. Stateful in that once created, it is used to perform updates against the same index.
 */
public class BufferedRestClient implements Closeable {

    private static Log log = LogFactory.getLog(BufferedRestClient.class);

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
    private Resource resource;
    private final boolean trace;

    public BufferedRestClient(Settings settings) {
        this.client = new RestClient(settings);
        String tempIndex = settings.getTargetResource();
        if (tempIndex == null) {
            tempIndex = "";
        }
        this.index = tempIndex;
        this.resource = new Resource(index);

        buffer = new byte[settings.getBatchSizeInBytes()];
        bufferEntriesThreshold = settings.getBatchSizeInEntries();
        requiresRefreshAfterBulk = settings.getBatchRefreshAfterWrite();
        trace = log.isTraceEnabled();
    }

    /**
     * Returns a pageable (scan based) result to the given query.
     *
     * @param uri
     * @return
     */
    ScrollQuery scan(String query) throws IOException {
        String[] scrollInfo = client.scan(query);
        String scrollId = scrollInfo[0];
        long totalSize = Long.parseLong(scrollInfo[1]);
        return new ScrollQuery(client, scrollId, totalSize);
    }

    /**
     * Writes the objects to index.
     *
     * @param index
     * @param object
     */
    public void addToIndex(Object object) throws IOException {
        Assert.hasText(index, "no index given");

        Object d = (object instanceof Writable ? WritableUtils.fromWritable((Writable) object) : object);

        StringBuilder sb = new StringBuilder("{\"index\":{}}\n");
        sb.append(mapper.writeValueAsString(d));
        sb.append("\n");

        String str = sb.toString();

        if (trace) {
            log.trace(String.format("Indexing object [%s]", str));
        }

        byte[] data = str.getBytes(StringUtils.UTF_8);

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
        if (log.isDebugEnabled()) {
            log.debug(String.format("Flushing batch of [%d]", bufferSize));
        }

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

            if (log.isDebugEnabled()) {
                log.debug(String.format("Refreshing index [%s]", index));
            }
        }
        client.close();
    }

    public RestClient getRestClient() {
        return client;
    }

    public Map<Shard, Node> getTargetShards() throws IOException {
        Map<String, Node> nodes = client.getNodes();

        List<List<Map<String, Object>>> info = client.targetShards(resource.targetShards());
        Map<Shard, Node> shards = new LinkedHashMap<Shard, Node>(info.size());
        for (List<Map<String, Object>> shardGroup : info) {
            for (Map<String, Object> shardData : shardGroup) {
                Shard shard = new Shard(shardData);
                if (shard.getState().isStarted()) {
                    Node node = nodes.get(shard.getNode());
                    Assert.notNull(node, "Cannot find node with id [" + shard.getNode() + "]");
                    shards.put(shard, node);
                }
            }
        }
        return shards;
    }
}