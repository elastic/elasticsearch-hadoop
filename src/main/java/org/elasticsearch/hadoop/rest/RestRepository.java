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
package org.elasticsearch.hadoop.rest;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.dto.Node;
import org.elasticsearch.hadoop.rest.dto.Shard;
import org.elasticsearch.hadoop.rest.dto.mapping.Field;
import org.elasticsearch.hadoop.rest.stats.Stats;
import org.elasticsearch.hadoop.rest.stats.StatsAware;
import org.elasticsearch.hadoop.serialization.ScrollReader;
import org.elasticsearch.hadoop.serialization.command.BulkCommands;
import org.elasticsearch.hadoop.serialization.command.Command;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.BytesRef;
import org.elasticsearch.hadoop.util.TrackingBytesArray;
import org.elasticsearch.hadoop.util.unit.TimeValue;

/**
 * Rest client performing high-level operations using buffers to improve performance. Stateful in that once created, it is used to perform updates against the same index.
 */
public class RestRepository implements Closeable, StatsAware {

    private static Log log = LogFactory.getLog(RestRepository.class);

    // serialization artifacts
    private int bufferEntriesThreshold;

    private final BytesArray ba = new BytesArray(0);
    private final TrackingBytesArray data = new TrackingBytesArray(ba);
    private int dataEntries = 0;
    private boolean requiresRefreshAfterBulk = false;
    private boolean executedBulkWrite = false;
    private BytesRef trivialBytesRef;
    private boolean writeInitialized = false;

    private RestClient client;
    private Resource resource;
    private Command command;
    private final Settings settings;
    private final Stats stats = new Stats();

    public RestRepository(Settings settings) {
        this.settings = settings;
        this.client = new RestClient(settings);
        this.resource = new Resource(settings);
    }

    /** postpone writing initialization since we can do only reading so there's no need to allocate buffers */
    private void lazyInitWriting() {
        if (!writeInitialized) {
            writeInitialized = true;

            ba.bytes(new byte[settings.getBatchSizeInBytes()], 0);
            trivialBytesRef = new BytesRef();
            bufferEntriesThreshold = settings.getBatchSizeInEntries();
            requiresRefreshAfterBulk = settings.getBatchRefreshAfterWrite();

            this.command = BulkCommands.create(settings);
        }
    }

    /**
     * Returns a pageable (scan based) result to the given query.
     *
     * @param query scan query
     * @param reader scroll reader
     * @return a scroll query
     */
    ScrollQuery scan(String query, BytesArray body, ScrollReader reader) throws IOException {
        String[] scrollInfo = client.scan(query, body);
        String scrollId = scrollInfo[0];
        long totalSize = Long.parseLong(scrollInfo[1]);
        return new ScrollQuery(this, scrollId, totalSize, reader);
    }

    /**
     * Writes the objects to index.
     *
     * @param object object to add to the index
     */
    public void writeToIndex(Object object) throws IOException {
        Assert.notNull(object, "no object data given");

        lazyInitWriting();
        doWriteToIndex(command.write(object));
    }

    /**
     * Writes the objects to index.
     *
     * @param data as a byte array
     * @param size the length to use from the given array
     */
    public void writeProcessedToIndex(BytesArray ba) throws IOException {
        Assert.notNull(ba, "no data given");
        Assert.isTrue(ba.length() > 0, "no data given");

        lazyInitWriting();
        trivialBytesRef.reset();
        trivialBytesRef.add(ba);
        doWriteToIndex(trivialBytesRef);
    }

    private void doWriteToIndex(BytesRef payload) throws IOException {
        // check space first
        if (payload.length() > ba.available()) {
            sendBatch();
        }

        data.copyFrom(payload);
        payload.reset();

        dataEntries++;
        if (bufferEntriesThreshold > 0 && dataEntries >= bufferEntriesThreshold) {
            sendBatch();
        }
    }

    private void sendBatch() throws IOException {
        if (log.isDebugEnabled()) {
            log.debug(String.format("Sending batch of [%d] bytes/[%s] entries", data.length(), dataEntries));
        }

        client.bulk(resource, data);
        data.reset();
        dataEntries = 0;
        executedBulkWrite = true;
    }

    @Override
    public void close() {
        try {
            if (log.isDebugEnabled()) {
                log.debug("Closing repository and connection to Elasticsearch ...");
            }
            if (data.length() > 0) {
                sendBatch();
            }
            if (requiresRefreshAfterBulk && executedBulkWrite) {
                // refresh batch
                client.refresh(resource);

                if (log.isDebugEnabled()) {
                    log.debug(String.format("Refreshing index [%s]", resource));
                }
            }
        } catch (IOException ex) {
            log.warn("Cannot flush data batch", ex);
        }

        client.close();
    }

    public RestClient getRestClient() {
        return client;
    }

    public Map<Shard, Node> getTargetShards() throws IOException {
        Map<String, Node> nodes = client.getNodes();

        List<List<Map<String, Object>>> info = client.targetShards(resource);
        Map<Shard, Node> shards = new LinkedHashMap<Shard, Node>(info.size());

        for (List<Map<String, Object>> shardGroup : info) {
            // find the first started shard in each group (round-robin)
            for (Map<String, Object> shardData : shardGroup) {
                Shard shard = new Shard(shardData);
                if (shard.getState().isStarted()) {
                    Node node = nodes.get(shard.getNode());
                    Assert.notNull(node, "Cannot find node with id [" + shard.getNode() + "]");
                    shards.put(shard, node);
                    break;
                }
            }
        }
        return shards;
    }

    public Map<Shard, Node> getTargetPrimaryShards() throws IOException {
        Map<String, Node> nodes = client.getNodes();

        List<List<Map<String, Object>>> info = client.targetShards(resource);
        Map<Shard, Node> shards = new LinkedHashMap<Shard, Node>(info.size());

        for (List<Map<String, Object>> shardGroup : info) {
            // consider only primary shards
            for (Map<String, Object> shardData : shardGroup) {
                Shard shard = new Shard(shardData);
                if (shard.isPrimary()) {
                    Node node = nodes.get(shard.getNode());
                    Assert.notNull(node, "Cannot find node with id [" + shard.getNode() + "]");
                    shards.put(shard, node);
                    break;
                }
            }
        }
        return shards;
    }

    public Field getMapping() throws IOException {
        return Field.parseField((Map<String, Object>) client.getMapping(resource.mapping()));
    }

    public List<Object[]> scroll(String scrollId, ScrollReader reader) throws IOException {
        InputStream scroll = client.scroll(scrollId);
        try {
            return reader.read(scroll);
        } finally {
            if (scroll instanceof StatsAware) {
                stats.aggregate(((StatsAware) scroll).stats());
            }
        }
    }

    public boolean indexExists() throws IOException {
        return client.exists(resource.indexAndType());
    }

    public void putMapping(BytesArray mapping) throws IOException {
        client.putMapping(resource.index(), resource.mapping(), mapping.bytes());
    }

    public boolean touch() throws IOException {
        return client.touch(resource.index());
    }

    public boolean waitForYellow() throws IOException {
        return client.health(resource.index(), RestClient.HEALTH.YELLOW, TimeValue.timeValueSeconds(10));
    }

    @Override
    public Stats stats() {
        return stats.aggregate(client.stats());
    }
}