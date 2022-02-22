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
import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.bulk.BulkProcessor;
import org.elasticsearch.hadoop.rest.bulk.BulkResponse;
import org.elasticsearch.hadoop.rest.query.QueryUtils;
import org.elasticsearch.hadoop.rest.stats.Stats;
import org.elasticsearch.hadoop.rest.stats.StatsAware;
import org.elasticsearch.hadoop.serialization.ScrollReader;
import org.elasticsearch.hadoop.serialization.ScrollReader.Scroll;
import org.elasticsearch.hadoop.serialization.ScrollReaderConfigBuilder;
import org.elasticsearch.hadoop.serialization.builder.JdkValueReader;
import org.elasticsearch.hadoop.serialization.bulk.BulkCommands;
import org.elasticsearch.hadoop.serialization.bulk.BulkEntryWriter;
import org.elasticsearch.hadoop.serialization.bulk.MetadataExtractor;
import org.elasticsearch.hadoop.serialization.dto.NodeInfo;
import org.elasticsearch.hadoop.serialization.dto.ShardInfo;
import org.elasticsearch.hadoop.serialization.dto.mapping.FieldParser;
import org.elasticsearch.hadoop.serialization.dto.mapping.GeoField;
import org.elasticsearch.hadoop.serialization.dto.mapping.GeoField.GeoType;
import org.elasticsearch.hadoop.serialization.dto.mapping.Mapping;
import org.elasticsearch.hadoop.serialization.dto.mapping.MappingSet;
import org.elasticsearch.hadoop.serialization.dto.mapping.MappingUtils;
import org.elasticsearch.hadoop.serialization.handler.read.impl.AbortOnlyHandlerLoader;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.BytesRef;
import org.elasticsearch.hadoop.util.EsMajorVersion;
import org.elasticsearch.hadoop.util.SettingsUtils;
import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.hadoop.util.unit.TimeValue;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.elasticsearch.hadoop.rest.Request.Method.POST;

/**
 * Rest client performing high-level operations using buffers to improve performance. Stateful in that once created, it
 * is used to perform updates against the same index.
 */
public class RestRepository implements Closeable, StatsAware {

    private static Log log = LogFactory.getLog(RestRepository.class);

    // wrapper around existing BA (for cases where the serialization already occurred)
    private BytesRef trivialBytesRef;
    private boolean writeInitialized = false;

    private RestClient client;
    // optional extractor passed lazily to BulkCommand
    private MetadataExtractor metaExtractor;

    private BulkEntryWriter bulkEntryWriter;
    private BulkProcessor bulkProcessor;

    // Internal
    private static class Resources {
        private final Settings resourceSettings;
        private Resource resourceRead;
        private Resource resourceWrite;

        public Resources(Settings resourceSettings) {
            this.resourceSettings = resourceSettings;
        }

        public Resource getResourceRead() {
            if (resourceRead == null) {
                if (StringUtils.hasText(resourceSettings.getResourceRead())) {
                    resourceRead = new Resource(resourceSettings, true);
                }
            }
            return resourceRead;
        }

        public Resource getResourceWrite() {
            if (resourceWrite == null) {
                if (StringUtils.hasText(resourceSettings.getResourceWrite())) {
                    resourceWrite = new Resource(resourceSettings, false);
                }
            }
            return resourceWrite;
        }
    }

    private final Settings settings;
    private Resources resources;
    private final Stats stats = new Stats();

    public RestRepository(Settings settings) {
        this.settings = settings;
        this.resources = new Resources(settings);

        // Check if we have a read resource first, and if not, THEN check the write resource
        // The write resource has more strict parsing rules, and if the process is only reading
        // with a resource that isn't good for writing, then eagerly parsing the resource as a
        // write resource can erroneously throw an error. Instead, we should just get the write
        // resource lazily as needed.
        Assert.isTrue(resources.getResourceRead() != null || resources.getResourceWrite() != null, "Invalid configuration - No read or write resource specified");

        this.client = new RestClient(settings);
    }

    /** postpone writing initialization since we can do only reading so there's no need to allocate buffers */
    private void lazyInitWriting() {
        if (!writeInitialized) {
            this.writeInitialized = true;
            this.bulkProcessor = new BulkProcessor(client, resources.getResourceWrite(), settings);
            this.trivialBytesRef = new BytesRef();
            this.bulkEntryWriter = new BulkEntryWriter(settings, BulkCommands.create(settings, metaExtractor, client.clusterInfo.getMajorVersion()));
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
        BytesRef serialized = bulkEntryWriter.writeBulkEntry(object);
        if (serialized != null) {
            doWriteToIndex(serialized);
        }
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
        bulkProcessor.add(payload);
        payload.reset();
    }

    public BulkResponse tryFlush() {
        if (writeInitialized) {
            return bulkProcessor.tryFlush();
        } else {
            log.warn("Attempt to flush before any data had been written");
            return BulkResponse.complete();
        }
    }

    public void flush() {
        if (writeInitialized) {
            bulkProcessor.flush();
        } else {
            log.warn("Attempt to flush before any data had been written");
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
            if (bulkProcessor != null) {
                bulkProcessor.close();
                // Aggregate stats before discarding them.
                stats.aggregate(bulkProcessor.stats());
                bulkProcessor = null;
            }

            if (bulkEntryWriter != null) {
                bulkEntryWriter.close();
                bulkEntryWriter = null;
            }
        } finally {
            client.close();
            // Aggregate stats before discarding them.
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
        return client.targetShards(resources.getResourceRead().index(), SettingsUtils.getFixedRouting(settings));
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
        List<List<Map<String, Object>>> info = client.targetShards(resources.getResourceWrite().index(), SettingsUtils.getFixedRouting(settings));
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

    public MappingSet getMappings() {
        return client.getMappings(resources.getResourceRead());
    }

    public Map<String, GeoField> sampleGeoFields(Mapping mapping) {
        Map<String, GeoType> fields = MappingUtils.geoFields(mapping);
        Map<String, Object> geoMapping = client.sampleForFields(resources.getResourceRead(), fields.keySet());

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
            Scroll scrollResult = reader.read(scroll);
            if (scrollResult == null) {
                log.info(String.format("No scroll for query [%s/%s], likely because the index is frozen", query, body));
            } else if (settings.getInternalVersionOrThrow().onOrBefore(EsMajorVersion.V_2_X)) {
                // On ES 2.X and before, a scroll response does not contain any hits to start with.
                // Another request will be needed.
                scrollResult = new Scroll(scrollResult.getScrollId(), scrollResult.getTotalHits(), false);
            }
            return scrollResult;
        } finally {
            if (scroll != null && scroll instanceof StatsAware) {
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

    public boolean resourceExists(boolean read) {
        Resource res = (read ? resources.getResourceRead() : resources.getResourceWrite());
        // cheap hit - works for exact index names, index patterns, the `_all` resource, and alias names
        boolean exists = client.indexExists(res.index());
        // Do we really care if it's typed?
        // Yes! If the index exists and a type is given, the type should exist on the index as well.
        if (exists && res.isTyped()) {
            exists = client.typeExists(res.index(), res.type());
        }

        // could be a _all or a pattern which is valid for read
        // try again by asking the mapping - could be expensive
        if (!exists && read) {
            try {
                // make sure the mapping is null since the index might exist but the type might be missing
                MappingSet mappings = client.getMappings(res);
                exists = mappings != null && !mappings.isEmpty();
            } catch (EsHadoopInvalidRequest ex) {
                exists = false;
            }
        }
        return exists;
    }

    public boolean touch() {
        return client.touch(resources.getResourceWrite().index());
    }

    public void delete() {
        if (client.clusterInfo.getMajorVersion().on(EsMajorVersion.V_1_X)) {
            // ES 1.x - delete as usual
            // Delete just the mapping
            client.delete(resources.getResourceWrite().index() + "/" + resources.getResourceWrite().type());
        }
        else {
            // try first a blind delete by query (since the plugin might be installed)
            try {
                if (resources.getResourceWrite().isTyped()) {
                    client.delete(resources.getResourceWrite().index() + "/" + resources.getResourceWrite().type() + "/_query?q=*");
                } else {
                    client.delete(resources.getResourceWrite().index() + "/_query?q=*");
                }
            } catch (EsHadoopInvalidRequest ehir) {
                log.info("Skipping delete by query as the plugin is not installed...");
            }

            // in ES 2.0 and higher this means scrolling and deleting the docs by hand...
            // do a scroll-scan without source

            // as this is a delete, there's not much value in making this configurable so we just go for some sane/safe defaults
            // 10m scroll timeout
            // 250 results

            int batchSize = 500;
            StringBuilder sb = new StringBuilder(resources.getResourceWrite().index());
            if (resources.getResourceWrite().isTyped()) {
                sb.append('/').append(resources.getResourceWrite().type());
            }
            sb.append("/_search?scroll=10m&_source=false&size=");
            sb.append(batchSize);
            if (client.clusterInfo.getMajorVersion().onOrAfter(EsMajorVersion.V_5_X)) {
                sb.append("&sort=_doc");
            }
            else {
                sb.append("&search_type=scan");
            }
            String scanQuery = sb.toString();
            ScrollReader scrollReader = new ScrollReader(
                    ScrollReaderConfigBuilder.builder(new JdkValueReader(), settings)
                            .setReadMetadata(true)
                            .setMetadataName("_metadata")
                            .setReturnRawJson(false)
                            .setIgnoreUnmappedFields(false)
                            .setIncludeFields(Collections.<String>emptyList())
                            .setExcludeFields(Collections.<String>emptyList())
                            .setIncludeArrayFields(Collections.<String>emptyList())
                            .setErrorHandlerLoader(new AbortOnlyHandlerLoader()) // Only abort since this is internal
            );

            // start iterating
            ScrollQuery sq = scanAll(scanQuery, null, scrollReader);
            try {
                BytesArray entry = new BytesArray(0);

                // delete each retrieved batch, keep routing in mind:
                String baseFormat = "{\"delete\":{\"_id\":\"%s\"}}\n";
                String routedFormat;
                if (client.clusterInfo.getMajorVersion().onOrAfter(EsMajorVersion.V_7_X)) {
                    routedFormat = "{\"delete\":{\"_id\":\"%s\", \"routing\":\"%s\"}}\n";
                } else {
                    routedFormat = "{\"delete\":{\"_id\":\"%s\", \"_routing\":\"%s\"}}\n";
                }

                boolean hasData = false;
                while (sq.hasNext()) {
                    hasData = true;
                    entry.reset();
                    Object[] kv = sq.next();
                    @SuppressWarnings("unchecked")
                    Map<String, Object> value = (Map<String, Object>) kv[1];
                    @SuppressWarnings("unchecked")
                    Map<String, Object> metadata = (Map<String, Object>) value.get("_metadata");
                    String routing = (String) metadata.get("_routing");
                    String encodedId = StringUtils.jsonEncoding((String) kv[0]);
                    if (StringUtils.hasText(routing)) {
                        String encodedRouting = StringUtils.jsonEncoding(routing);
                        entry.add(StringUtils.toUTF(String.format(routedFormat, encodedId, encodedRouting)));
                    } else {
                        entry.add(StringUtils.toUTF(String.format(baseFormat, encodedId)));
                    }
                    writeProcessedToIndex(entry);
                }

                if (hasData) {
                    flush();
                    // once done force a refresh
                    client.refresh(resources.getResourceWrite());
                }
            } finally {
                stats.aggregate(sq.stats());
                sq.close();
            }
        }
    }

    public boolean isEmpty(boolean read) {
        Resource res = (read ? resources.getResourceRead() : resources.getResourceWrite());
        boolean exists = client.indexExists(res.index());
        return (exists ? count(read) <= 0 : true);
    }

    public long count(boolean read) {
        Resource res = (read ? resources.getResourceRead() : resources.getResourceWrite());
        if (res.isTyped()) {
            return client.count(res.index(), res.type(), QueryUtils.parseQuery(settings));
        } else {
            return client.count(res.index(), QueryUtils.parseQuery(settings));
        }
    }

    public boolean waitForYellow() {
        return client.waitForHealth(resources.getResourceWrite().index(), RestClient.Health.YELLOW, TimeValue.timeValueSeconds(10));
    }

    @Override
    public Stats stats() {
        Stats copy = new Stats(stats);
        if (client != null) {
            // Aggregate stats if it's not already discarded
            copy.aggregate(client.stats());
        }
        if (bulkProcessor != null) {
            // Aggregate stats if it's not already discarded
            copy.aggregate(bulkProcessor.stats());
        }
        return copy;
    }

    public Settings getSettings() {
        return settings;
    }
}