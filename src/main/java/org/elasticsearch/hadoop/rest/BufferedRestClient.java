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
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.dto.Node;
import org.elasticsearch.hadoop.rest.dto.Shard;
import org.elasticsearch.hadoop.rest.dto.mapping.Field;
import org.elasticsearch.hadoop.serialization.ContentBuilder;
import org.elasticsearch.hadoop.serialization.ScrollReader;
import org.elasticsearch.hadoop.serialization.SerializationUtils;
import org.elasticsearch.hadoop.serialization.SettingsAware;
import org.elasticsearch.hadoop.serialization.ValueWriter;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.FastByteArrayOutputStream;
import org.elasticsearch.hadoop.util.ObjectUtils;
import org.elasticsearch.hadoop.util.StringUtils;

/**
 * Rest client performing high-level operations using buffers to improve performance. Stateful in that once created, it is used to perform updates against the same index.
 */
public class BufferedRestClient implements Closeable {

    private static Log log = LogFactory.getLog(BufferedRestClient.class);

    private RestClientBuffer bufferWriter;
    // serialization artifacts
    private int bufferEntriesThreshold;
    private boolean requiresRefreshAfterBulk = false;
    private boolean executedBulkWrite = false;

    private BytesArray scratchPad;
    private ValueWriter<?> valueWriter;

    private boolean writeInitialized = false;

    private RestClient client;
    private String index;
    private Resource resource;
    private final boolean trace;

    private final Settings settings;



    public BufferedRestClient(Settings settings) {
        this.settings = settings;
        this.client = new RestClient(settings);
        String tempIndex = settings.getTargetResource();
        if (tempIndex == null) {
            tempIndex = "";
        }
        this.index = tempIndex;
        this.resource = new Resource(index);
        trace = log.isTraceEnabled();
    }

    /** postpone writing initialization since we can do only reading so there's no need to allocate buffers */

    private void lazyInitWriting() {
        if (!writeInitialized) {
            writeInitialized = true;

            bufferWriter = ConfigurationOptions.ES_INDEX_WRITE_STRATEGY_UPSERT.equals(settings.getWriteStrategy()) ?
                    new UpsertRestClientBuffer(settings.getBatchSizeInBytes(), settings.getIdPath()):
                    new IndexRestClientBuffer(settings.getBatchSizeInBytes());

            bufferEntriesThreshold = settings.getBatchSizeInEntries();
            requiresRefreshAfterBulk = settings.getBatchRefreshAfterWrite();

            valueWriter = SerializationUtils.instantiateValueWriter(settings);

            if (scratchPad == null) {
                scratchPad = new BytesArray(1024);
            }

            if (trace) {
                log.trace(String.format("Instantied value writer [%s]", valueWriter));
            }
        }
    }

    /**
     * Returns a pageable (scan based) result to the given query.
     *
     * @return
     */
    ScrollQuery scan(String query, ScrollReader reader) throws IOException {
        String[] scrollInfo = client.scan(query);
        String scrollId = scrollInfo[0];
        long totalSize = Long.parseLong(scrollInfo[1]);
        return new ScrollQuery(this, scrollId, totalSize, reader);
    }

    /**
     * Writes the objects to index.
     *
     * @param object
     */
    public void addToIndex(Object object) throws IOException {
        Assert.hasText(index, "no index given");
        Assert.notNull(object, "no object data given");

        lazyInitWriting();
        scratchPad.reset();
        FastByteArrayOutputStream bos = new FastByteArrayOutputStream(scratchPad);
        ContentBuilder.generate(bos, valueWriter).value(object).flush().close();

        doAddToIndex();
    }

    /**
     * Writes the objects to index.
     *
     */
    public void addToIndex(byte[] data, int size) throws IOException {
        Assert.hasText(index, "no index given");
        Assert.notNull(data, "no data given");

        if (scratchPad == null) {
            // minor optimization to avoid allocating data when used by Hive (which already has its own scratchPad)
            scratchPad = new BytesArray(0);
        }
        lazyInitWriting();
        scratchPad.setBytes(data, size);
        doAddToIndex();
    }

    private void doAddToIndex() throws IOException {
        if (trace) {
            log.trace(String.format("Indexing object [%s]", scratchPad));
        }

        if(!bufferWriter.hasCapacityFor(scratchPad)){
            flushBatch();
        }

        bufferWriter.write(scratchPad);

        if (bufferEntriesThreshold > 0 && bufferWriter.numEntries() >= bufferEntriesThreshold) {
            flushBatch();
        }
    }

    private void flushBatch() throws IOException {
        if (log.isDebugEnabled()) {
            log.debug(String.format("Flushing batch of [%d] bytes", bufferWriter.size()));
        }

        client.bulk(index, bufferWriter.getBuffer(), bufferWriter.size());
        bufferWriter.reset();
        executedBulkWrite = true;
    }

    @Override
    public void close() {
        try {
            if (writeInitialized && bufferWriter.size() > 0) {
                flushBatch();
            }
            if (requiresRefreshAfterBulk && executedBulkWrite) {
                // refresh batch
                client.refresh(index);

                if (log.isDebugEnabled()) {
                    log.debug(String.format("Refreshing index [%s]", index));
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

        List<List<Map<String, Object>>> info = client.targetShards(resource.targetShards());
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

    public Field getMapping() throws IOException {
        return Field.parseField((Map<String, Object>) client.getMapping(resource.mapping()));
    }

    public List<Object[]> scroll(String scrollId, ScrollReader reader) throws IOException {
        return reader.read(client.scroll(scrollId));
    }

    public boolean indexExists() {
        return client.exists(resource.indexAndType());
    }

    public void putMapping(BytesArray mapping) {
        client.putMapping(resource.index(), resource.mapping(), mapping.bytes());
    }
}