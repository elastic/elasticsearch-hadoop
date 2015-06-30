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
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.EsHadoopException;
import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.stats.Stats;
import org.elasticsearch.hadoop.rest.stats.StatsAware;
import org.elasticsearch.hadoop.serialization.ScrollReader;
import org.elasticsearch.hadoop.serialization.bulk.BulkCommand;
import org.elasticsearch.hadoop.serialization.bulk.BulkCommands;
import org.elasticsearch.hadoop.serialization.bulk.MetadataExtractor;
import org.elasticsearch.hadoop.serialization.dto.Node;
import org.elasticsearch.hadoop.serialization.dto.Shard;
import org.elasticsearch.hadoop.serialization.dto.mapping.Field;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.BytesRef;
import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.hadoop.util.TrackingBytesArray;
import org.elasticsearch.hadoop.util.unit.TimeValue;

/**
 * Rest client performing high-level operations using buffers to improve performance. Stateful in that once created, it is used to perform updates against the same index.
 */
public class RestRepository implements Closeable, StatsAware {

    private static Log log = LogFactory.getLog(RestRepository.class);
    private static final BitSet EMPTY = new BitSet();

    // serialization artifacts
    private int bufferEntriesThreshold;

    // raw data
    private final BytesArray ba = new BytesArray(0);
    // tracking array (backed by the BA above)
    private final TrackingBytesArray data = new TrackingBytesArray(ba);
    private int dataEntries = 0;
    private boolean requiresRefreshAfterBulk = false;
    private boolean executedBulkWrite = false;
    // wrapper around existing BA (for cases where the serialization already occurred)
    private BytesRef trivialBytesRef;
    private boolean writeInitialized = false;
    private boolean autoFlush = true;

    // indicates whether there were writes errorrs or not
    // flag indicating whether to flush the batch at close-time or not
    private boolean hadWriteErrors = false;

    private RestClient client;
    private Resource resourceR;
    private Resource resourceW;
    private BulkCommand command;
    // optional extractor passed lazily to BulkCommand
    private MetadataExtractor metaExtractor;

    private final Settings settings;
    private final Stats stats = new Stats();

    public RestRepository(Settings settings) {
        this.settings = settings;

        if (StringUtils.hasText(settings.getResourceRead())) {
            this.resourceR = new Resource(settings, true);
        }

        if (StringUtils.hasText(settings.getResourceWrite())) {
            this.resourceW = new Resource(settings, false);
        }

        Assert.isTrue(resourceR != null || resourceW != null, "Invalid configuration - No read or write resource specified");

        this.client = new RestClient(settings);
    }

    /** postpone writing initialization since we can do only reading so there's no need to allocate buffers */
    private void lazyInitWriting() {
        if (!writeInitialized) {
            writeInitialized = true;

            autoFlush = !settings.getBatchFlushManual();
            ba.bytes(new byte[settings.getBatchSizeInBytes()], 0);
            trivialBytesRef = new BytesRef();
            bufferEntriesThreshold = settings.getBatchSizeInEntries();
            requiresRefreshAfterBulk = settings.getBatchRefreshAfterWrite();

            this.command = BulkCommands.create(settings, metaExtractor);
        }
    }

    /**
     * Returns a pageable (scan based) result to the given query.
     *
     * @param query scan query
     * @param reader scroll reader
     * @return a scroll query
     */
    ScrollQuery scan(String query, BytesArray body, ScrollReader reader) {
        String[] scrollInfo = client.scan(query, body);
        String scrollId = scrollInfo[0];
        long totalSize = Long.parseLong(scrollInfo[1]);
        return new ScrollQuery(this, scrollId, totalSize, reader);
    }

    public void addRuntimeFieldExtractor(MetadataExtractor metaExtractor) {
        this.metaExtractor = metaExtractor;
    }

    /**
     * Writes the objects to index.
     *
     * @param object object to add to the index
     */
    public void writeToIndex(Object object) {
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
    public void writeProcessedToIndex(BytesArray ba) {
        Assert.notNull(ba, "no data given");
        Assert.isTrue(ba.length() > 0, "no data given");

        lazyInitWriting();
        trivialBytesRef.reset();
        trivialBytesRef.add(ba);
        doWriteToIndex(trivialBytesRef);
    }

    private void doWriteToIndex(BytesRef payload) {
        // check space first
        if (payload.length() > ba.available()) {
            if (autoFlush) {
                flush();
            }
            else {
                throw new EsHadoopIllegalStateException(
                        String.format("Auto-flush disabled and bulk buffer full; disable manual flush or increase capacity [current size %s]; bailing out", ba.capacity()));
            }
        }

        data.copyFrom(payload);
        payload.reset();

        dataEntries++;
        if (bufferEntriesThreshold > 0 && dataEntries >= bufferEntriesThreshold) {
            if (autoFlush) {
                flush();
            }
            else {
                // handle the corner case of manual flush that occurs only after the buffer is completely full (think size of 1)
                if (dataEntries > bufferEntriesThreshold) {
                    throw new EsHadoopIllegalStateException(
                            String.format(
                                    "Auto-flush disabled and maximum number of entries surpassed; disable manual flush or increase capacity [current size %s]; bailing out",
                                    bufferEntriesThreshold));
                }
            }
        }
    }

    public BitSet tryFlush() {
        if (log.isDebugEnabled()) {
            log.debug(String.format("Sending batch of [%d] bytes/[%s] entries", data.length(), dataEntries));
        }

        BitSet bulkResult = EMPTY;

        try {
            // double check data - it might be a false flush (called on clean-up)
            if (data.length() > 0) {
                bulkResult = client.bulk(resourceW, data);
                executedBulkWrite = true;
            }
        } catch (EsHadoopException ex) {
            hadWriteErrors = true;
            throw ex;
        }

        // discard the data buffer, only if it was properly sent/processed
        //if (bulkResult.isEmpty()) {
        // always discard data since there's no code path that uses the in flight data
        discard();
        //}

        return bulkResult;
    }

    public void discard() {
        data.reset();
        dataEntries = 0;
    }

    public void flush() {
        BitSet bulk = tryFlush();
        if (!bulk.isEmpty()) {
            throw new EsHadoopException(String.format("Could not write all entries [%s/%s] (maybe ES was overloaded?). Bailing out...", bulk.cardinality(), bulk.size()));
        }
    }

    @Override
    public void close() {
        if (log.isDebugEnabled()) {
            log.debug("Closing repository and connection to Elasticsearch ...");
        }

        if (!hadWriteErrors) {
            flush();
        }
        else {
            if (log.isDebugEnabled()) {
                log.debug("Dirty close; ignoring last existing write batch...");
            }
        }

        if (client != null) {
            if (requiresRefreshAfterBulk && executedBulkWrite) {
                // refresh batch
                client.refresh(resourceW);

                if (log.isDebugEnabled()) {
                    log.debug(String.format("Refreshing index [%s]", resourceW));
                }
            }

            client.close();
            stats.aggregate(client.stats());
            client = null;
        }
    }

    public RestClient getRestClient() {
        return client;
    }


    public Object[] getReadTargetShards(boolean clientNodesOnly) {
        for (int retries = 0; retries < 3; retries++) {
            Object[] result = doGetReadTargetShards(clientNodesOnly);
            if (result != null) {
                return result;
            }
        }
        throw new EsHadoopIllegalStateException("Cluster state volatile; cannot find node backing shards - please check whether your cluster is stable");
    }

    protected Object[] doGetReadTargetShards(boolean clientNodesOnly) {
        List<List<Map<String, Object>>> info = client.targetShards(resourceR.index());

        // if client-nodes routing is used, allow non-http clients
        Map<String, Node> httpNodes = client.getHttpNodes(clientNodesOnly);

        if (httpNodes.isEmpty()) {
            String msg = "No HTTP-enabled data nodes found";
            if (!settings.getNodesClientOnly()) {
                msg += String.format("; if you are using client-only nodes make sure to configure es-hadoop as such through [%s] property", ConfigurationOptions.ES_NODES_CLIENT_ONLY);
            }
            throw new EsHadoopIllegalStateException(msg);
        }

        Map<Shard, Node> shards = new LinkedHashMap<Shard, Node>();

        boolean overlappingShards = false;

        Object[] result = new Object[2];

        // false by default
        result[0] = overlappingShards;
        result[1] = shards;

        // check if multiple indices are hit
        if (!isReadIndexConcrete()) {
            String message = String.format("Read resource [%s] includes multiple indices or/and aliases; to avoid duplicate results (caused by shard overlapping), parallelism ", resourceR);

            Map<Shard, Node> combination = ShardSorter.find(info, httpNodes, log);
            if (combination.isEmpty()) {
                message += "is minimized";
                log.warn(message);
                overlappingShards = true;
                result[0] = overlappingShards;
            }
            else {
                int initialParallelism = 0;
                for (List<Map<String, Object>> shardGroup : info) {
                    initialParallelism += shardGroup.size();
                }

                if (initialParallelism > combination.size()) {
                    message += String.format("is reduced from %s to %s", initialParallelism, combination.size());
                    log.warn(message);
                }
                result[0] = overlappingShards;
                result[1] = combination;

                return result;
            }
        }

        Set<Integer> seenShards = new LinkedHashSet<Integer>();

        for (List<Map<String, Object>> shardGroup : info) {
            for (Map<String, Object> shardData : shardGroup) {
                Shard shard = new Shard(shardData);
                if (shard.getState().isStarted()) {
                    Node node = httpNodes.get(shard.getNode());
                    if (node == null) {
                        log.warn(String.format("Cannot find node with id [%s] (is HTTP enabled?) from shard [%s] in nodes [%s]; layout [%s]", shard.getNode(), shard, httpNodes, info));
                        return null;
                    }
                    // when dealing with overlapping shards, simply keep a shard for each id/name (0, 1, ...)
                    if (overlappingShards) {
                        if (seenShards.add(shard.getName())) {
                            shards.put(shard, node);
                        }
                    }
                    else {
                        shards.put(shard, node);
                        break;
                    }
                }
            }
        }
        return result;
    }

    public Map<Shard, Node> getWriteTargetPrimaryShards(boolean clientNodesOnly) {
        for (int retries = 0; retries < 3; retries++) {
            Map<Shard, Node> map = doGetWriteTargetPrimaryShards(clientNodesOnly);
            if (map != null) {
                return map;
            }
        }
        throw new EsHadoopIllegalStateException("Cluster state volatile; cannot find node backing shards - please check whether your cluster is stable");
    }

    protected Map<Shard, Node> doGetWriteTargetPrimaryShards(boolean clientNodesOnly) {
        List<List<Map<String, Object>>> info = client.targetShards(resourceW.index());
        Map<Shard, Node> shards = new LinkedHashMap<Shard, Node>();
        Map<String, Node> nodes = client.getHttpNodes(clientNodesOnly);

        for (List<Map<String, Object>> shardGroup : info) {
            // consider only primary shards
            for (Map<String, Object> shardData : shardGroup) {
                Shard shard = new Shard(shardData);
                if (shard.isPrimary()) {
                    Node node = nodes.get(shard.getNode());
                    if (node == null) {
                        log.warn(String.format("Cannot find node with id [%s] (is HTTP enabled?) from shard [%s] in nodes [%s]; layout [%s]", shard.getNode(), shard, nodes, info));
                        return null;
                    }
                    shards.put(shard, node);
                    break;
                }
            }
        }
        return shards;
    }

    public Field getMapping() {
        return Field.parseField(client.getMapping(resourceR.mapping()));
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

    public boolean indexExists(boolean read) {
        Resource res = (read ? resourceR : resourceW);
        // cheap hit
        boolean exists = client.exists(res.indexAndType());
        // could be a _all or a pattern which is valid for read
        // try again by asking the mapping - could be expensive
        if (!exists && read) {
            try {
                // make sure the mapping is null since the index might exist but the type might be missing
                exists = !client.getMapping(res.mapping()).isEmpty();
            } catch (EsHadoopInvalidRequest ex) {
                exists = false;
            }
        }
        return exists;
    }

    private boolean isReadIndexConcrete() {
        String index = resourceR.index();
        return !(index.contains(",") || index.contains("*") || client.isAlias(resourceR.aliases()));
    }

    public void putMapping(BytesArray mapping) {
        client.putMapping(resourceW.index(), resourceW.mapping(), mapping.bytes());
    }

    public boolean touch() {
        return client.touch(resourceW.index());
    }

    public void delete() {
        client.delete(resourceW.indexAndType());
    }

    public boolean isEmpty(boolean read) {
        Resource res = (read ? resourceR : resourceW);
        boolean exists = client.exists(res.indexAndType());
        return (exists ? count(read) <= 0 : true);
    }

    public long count(boolean read) {
        Resource res = (read ? resourceR : resourceW);
        return client.count(res.indexAndType());
    }

    public boolean waitForYellow() {
        return client.health(resourceW.index(), RestClient.HEALTH.YELLOW, TimeValue.timeValueSeconds(10));
    }

    @Override
    public Stats stats() {
        Stats copy = new Stats(stats);
        if (client != null) {
            copy.aggregate(client.stats());
        }
        return copy;
    }

    public Settings getSettings() {
        return settings;
    }
}
