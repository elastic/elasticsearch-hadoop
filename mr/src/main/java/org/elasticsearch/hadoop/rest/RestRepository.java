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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.EsHadoopException;
import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.query.QueryUtils;
import org.elasticsearch.hadoop.rest.stats.Stats;
import org.elasticsearch.hadoop.rest.stats.StatsAware;
import org.elasticsearch.hadoop.serialization.ScrollReader;
import org.elasticsearch.hadoop.serialization.ScrollReader.Scroll;
import org.elasticsearch.hadoop.serialization.ScrollReader.ScrollReaderConfig;
import org.elasticsearch.hadoop.serialization.builder.JdkValueReader;
import org.elasticsearch.hadoop.serialization.bulk.BulkCommand;
import org.elasticsearch.hadoop.serialization.bulk.BulkCommands;
import org.elasticsearch.hadoop.serialization.bulk.MetadataExtractor;
import org.elasticsearch.hadoop.serialization.dto.NodeInfo;
import org.elasticsearch.hadoop.serialization.dto.ShardInfo;
import org.elasticsearch.hadoop.serialization.dto.mapping.Field;
import org.elasticsearch.hadoop.serialization.dto.mapping.GeoField;
import org.elasticsearch.hadoop.serialization.dto.mapping.GeoField.GeoType;
import org.elasticsearch.hadoop.serialization.dto.mapping.MappingUtils;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.BytesRef;
import org.elasticsearch.hadoop.util.EsMajorVersion;
import org.elasticsearch.hadoop.util.SettingsUtils;
import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.hadoop.util.TrackingBytesArray;
import org.elasticsearch.hadoop.util.unit.TimeValue;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.elasticsearch.hadoop.rest.Request.Method.POST;

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

    // indicates whether there were writes errors or not
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

            this.command = BulkCommands.create(settings, metaExtractor, client.internalVersion);
        }
    }

    ScrollQuery scanAll(String query, BytesArray body, ScrollReader reader) {
        return scanLimit(query, body, -1, reader);
    }

    /**
     * Returns a pageable (scan based) result to the given query.
     *
     * @param query scan query
     * @param reader scroll reader
     * @return a scroll query
     */
    ScrollQuery scanLimit(String query, BytesArray body, long limit, ScrollReader reader) {
        return new ScrollQuery(this, query, body, limit, reader);
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
     * @param ba The data as a bytes array
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
        // ba is the backing array for data
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

    public BulkResponse tryFlush() {
        BulkResponse bulkResult;

        try {
            // double check data - it might be a false flush (called on clean-up)
            if (data.length() > 0) {
                if (log.isDebugEnabled()) {
                    log.debug(String.format("Sending batch of [%d] bytes/[%s] entries", data.length(), dataEntries));
                }

                bulkResult = client.bulk(resourceW, data);
                executedBulkWrite = true;
            } else {
                bulkResult = BulkResponse.ok(0);
            }
        } catch (EsHadoopException ex) {
            hadWriteErrors = true;
            throw ex;
        }

        // always discard data since there's no code path that uses the in flight data
        discard();

        return bulkResult;
    }

    public void discard() {
        data.reset();
        dataEntries = 0;
    }

    public void flush() {
        BulkResponse bulk = tryFlush();
        if (!bulk.getLeftovers().isEmpty()) {
            String header = String.format("Could not write all entries [%s/%s] (Maybe ES was overloaded?). Error sample (first [%s] error messages):\n", bulk.getLeftovers().cardinality(), bulk.getTotalWrites(), bulk.getErrorExamples().size());
            StringBuilder message = new StringBuilder(header);
            for (String errors : bulk.getErrorExamples()) {
                message.append("\t").append(errors).append("\n");
            }
            message.append("Bailing out...");
            throw new EsHadoopException(message.toString());
        }
    }

    @Override
    public void close() {
        if (log.isDebugEnabled()) {
            log.debug("Closing repository and connection to Elasticsearch ...");
        }

        // bail out if closed before
        if (client == null) {
            return;
        }

        try {
            if (!hadWriteErrors) {
                flush();
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("Dirty close; ignoring last existing write batch...");
                }
            }

            if (requiresRefreshAfterBulk && executedBulkWrite) {
                // refresh batch
                client.refresh(resourceW);

                if (log.isDebugEnabled()) {
                    log.debug(String.format("Refreshing index [%s]", resourceW));
                }
            }
        } finally {
            client.close();
            stats.aggregate(client.stats());
            client = null;
        }
    }

    public RestClient getRestClient() {
        return client;
    }

    public List<List<Map<String, Object>>> getReadTargetShards() {
        for (int retries = 0; retries < 3; retries++) {
            List<List<Map<String, Object>>> result = doGetReadTargetShards();
            if (result != null) {
                return result;
            }
        }
        throw new EsHadoopIllegalStateException("Cluster state volatile; cannot find node backing shards - please check whether your cluster is stable");
    }

    protected List<List<Map<String, Object>>> doGetReadTargetShards() {
        return client.targetShards(resourceR.index(), SettingsUtils.getFixedRouting(settings));
    }

    public Map<ShardInfo, NodeInfo> getWriteTargetPrimaryShards(boolean clientNodesOnly) {
        for (int retries = 0; retries < 3; retries++) {
            Map<ShardInfo, NodeInfo> map = doGetWriteTargetPrimaryShards(clientNodesOnly);
            if (map != null) {
                return map;
            }
        }
        throw new EsHadoopIllegalStateException("Cluster state volatile; cannot find node backing shards - please check whether your cluster is stable");
    }

    protected Map<ShardInfo, NodeInfo> doGetWriteTargetPrimaryShards(boolean clientNodesOnly) {
        List<List<Map<String, Object>>> info = client.targetShards(resourceW.index(), SettingsUtils.getFixedRouting(settings));
        Map<ShardInfo, NodeInfo> shards = new LinkedHashMap<ShardInfo, NodeInfo>();
        List<NodeInfo> nodes = client.getHttpNodes(clientNodesOnly);
        Map<String, NodeInfo> nodeMap = new HashMap<String, NodeInfo>(nodes.size());
        for (NodeInfo node : nodes) {
            nodeMap.put(node.getId(), node);
        }

        for (List<Map<String, Object>> shardGroup : info) {
            // consider only primary shards
            for (Map<String, Object> shardData : shardGroup) {
                ShardInfo shard = new ShardInfo(shardData);
                if (shard.isPrimary()) {
                    NodeInfo node = nodeMap.get(shard.getNode());
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

    public Map<String, GeoField> sampleGeoFields(Field mapping) {
        Map<String, GeoType> fields = MappingUtils.geoFields(mapping);
        Map<String, Object> geoMapping = client.sampleForFields(resourceR.indexAndType(), fields.keySet());

        Map<String, GeoField> geoInfo = new LinkedHashMap<String, GeoField>();
        for (Entry<String, GeoType> geoEntry : fields.entrySet()) {
            String fieldName = geoEntry.getKey();
            geoInfo.put(fieldName, MappingUtils.parseGeoInfo(geoEntry.getValue(), geoMapping.get(fieldName)));
        }

        return geoInfo;
    }

    // used to initialize a scroll (based on a query)
    Scroll scroll(String query, BytesArray body, ScrollReader reader) throws IOException {
        InputStream scroll = client.execute(POST, query, body).body();
        try {
            return reader.read(scroll);
        } finally {
            if (scroll instanceof StatsAware) {
                stats.aggregate(((StatsAware) scroll).stats());
            }
        }
    }
    
    // consume the scroll
    Scroll scroll(String scrollId, ScrollReader reader) throws IOException {
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
        boolean exists = client.indexExists(res.index());
        if (exists && StringUtils.hasText(res.type())) {
            exists = client.typeExists(res.index(), res.type());
        }

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
        if (client.internalVersion.on(EsMajorVersion.V_1_X)) {
            // ES 1.x - delete as usual
            client.delete(resourceW.indexAndType());
        }
        else {
            // try first a blind delete by query (since the plugin might be installed)
            try {
                client.delete(resourceW.indexAndType() + "/_query?q=*");
            } catch (EsHadoopInvalidRequest ehir) {
                log.info("Skipping delete by query as the plugin is not installed...");
            }

            // in ES 2.0 and higher this means scrolling and deleting the docs by hand...
            // do a scroll-scan without source

            // as this is a delete, there's not much value in making this configurable so we just go for some sane/safe defaults
            // 10m scroll timeout
            // 250 results

            int batchSize = 500;
            StringBuilder sb = new StringBuilder(resourceW.indexAndType());
            sb.append("/_search?scroll=10m&_source=false&size=");
            sb.append(batchSize);
            if (client.internalVersion.onOrAfter(EsMajorVersion.V_5_X)) {
                sb.append("&sort=_doc");
            }
            else {
                sb.append("&search_type=scan");
            }
            String scanQuery = sb.toString();
            ScrollReader scrollReader = new ScrollReader(new ScrollReaderConfig(new JdkValueReader()));

            // start iterating
            ScrollQuery sq = scanAll(scanQuery, null, scrollReader);
            try {
                BytesArray entry = new BytesArray(0);

                // delete each retrieved batch
                String format = "{\"delete\":{\"_id\":\"%s\"}}\n";
                while (sq.hasNext()) {
                    entry.reset();
                    entry.add(StringUtils.toUTF(String.format(format, sq.next()[0])));
                    writeProcessedToIndex(entry);
                }

                flush();
                // once done force a refresh
                client.refresh(resourceW);
            } finally {
                stats.aggregate(sq.stats());
                sq.close();
            }
        }
    }

    public boolean isEmpty(boolean read) {
        Resource res = (read ? resourceR : resourceW);
        boolean exists = client.indexExists(res.index());
        return (exists ? count(read) <= 0 : true);
    }

    public long count(boolean read) {
        Resource res = (read ? resourceR : resourceW);
        return client.count(res.indexAndType(), QueryUtils.parseQuery(settings));
    }

    public boolean waitForYellow() {
        return client.waitForHealth(resourceW.index(), RestClient.Health.YELLOW, TimeValue.timeValueSeconds(10));
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