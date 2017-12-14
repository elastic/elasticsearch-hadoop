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
import org.elasticsearch.hadoop.handler.EsHadoopAbortHandlerException;
import org.elasticsearch.hadoop.handler.HandlerResult;
import org.elasticsearch.hadoop.rest.handler.BulkWriteErrorCollector;
import org.elasticsearch.hadoop.rest.handler.BulkWriteErrorHandler;
import org.elasticsearch.hadoop.rest.handler.BulkWriteFailure;
import org.elasticsearch.hadoop.rest.handler.impl.BulkWriteHandlerLoader;
import org.elasticsearch.hadoop.rest.handler.impl.HttpRetryHandler;
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
import org.elasticsearch.hadoop.serialization.dto.mapping.FieldParser;
import org.elasticsearch.hadoop.serialization.dto.mapping.GeoField;
import org.elasticsearch.hadoop.serialization.dto.mapping.GeoField.GeoType;
import org.elasticsearch.hadoop.serialization.dto.mapping.Mapping;
import org.elasticsearch.hadoop.serialization.dto.mapping.MappingSet;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
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
    private BulkCommand command;
    // optional extractor passed lazily to BulkCommand
    private MetadataExtractor metaExtractor;

    // bulk request document error handlers
    private List<BulkWriteErrorHandler> documentBulkErrorHandlers;

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
            writeInitialized = true;

            autoFlush = !settings.getBatchFlushManual();
            ba.bytes(new byte[settings.getBatchSizeInBytes()], 0);
            trivialBytesRef = new BytesRef();
            bufferEntriesThreshold = settings.getBatchSizeInEntries();
            requiresRefreshAfterBulk = settings.getBatchRefreshAfterWrite();

            this.command = BulkCommands.create(settings, metaExtractor, client.internalVersion);

            this.documentBulkErrorHandlers = new ArrayList<BulkWriteErrorHandler>();

            documentBulkErrorHandlers.add(new HttpRetryHandler(settings));

            BulkWriteHandlerLoader handlerLoader = new BulkWriteHandlerLoader();
            handlerLoader.setSettings(settings);
            documentBulkErrorHandlers.addAll(handlerLoader.loadHandlers());
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
        //FIXHERE: Perform error handling for serialization here
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
        // FIXHERE: If a retry is performed with new data, this byte array will potentially grow in size.
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
                boolean retryOperation = false;
                long waitTime = 0L;
                List<Integer> attempts = Collections.emptyList();

                do {
                    if (retryOperation) {
                        if (waitTime > 0L) {
                            if (log.isDebugEnabled()) {
                                log.debug(String.format("Retrying failed bulk documents after backing off for [%s] ms",
                                        TimeValue.timeValueMillis(waitTime)));
                            }
                            try {
                                Thread.sleep(waitTime);
                            } catch (InterruptedException e) {
                                if (log.isDebugEnabled()) {
                                    log.debug("Thread interrupted - giving up on retrying...");
                                }
                                throw new EsHadoopException("Thread interrupted - giving up on retrying...", e);
                            }
                        } else {
                            if (log.isDebugEnabled()) {
                                log.debug(String.format("Retrying failed bulk documents immediately (without backoff)",
                                        TimeValue.timeValueMillis(waitTime)));
                            }
                        }
                    } else if (log.isDebugEnabled()) {
                        log.debug(String.format("Sending batch of [%d] bytes/[%s] entries", data.length(), dataEntries));
                    }

                    bulkResult = client.bulk(resources.getResourceWrite(), data);
                    executedBulkWrite = true;

                    if (retryOperation) {
                        stats.docsRetried += data.entries();
                        stats.bytesRetried += data.length();
                        stats.bulkRetries++;
                        stats.bulkRetriesTotalTime += bulkResult.getClientTimeSpent();
                    }

                    // Handle bulk write failures
                    if (!bulkResult.getDocumentErrors().isEmpty()) {
                        // Some documents failed. Pass them to retry handler.

                        List<Integer> previousAttempts = attempts;

                        // FIXHERE: Find a way to retry documents without growing the original backing array, or track bytes separately.
                        attempts = new ArrayList<Integer>();
                        BulkWriteErrorCollector errorCollector = new BulkWriteErrorCollector();

                        // Iterate over all errors, and for each error, attempt to handle the problem.
                        for (BulkResponse.BulkError bulkError : bulkResult.getDocumentErrors()) {
                            int requestAttempt;
                            if (previousAttempts.isEmpty() || (bulkError.getOriginalPosition() + 1) > previousAttempts.size()) {
                                // We don't have an attempt, assume first attempt
                                requestAttempt = 1;
                            } else {
                                // Get and increment previous attempt value
                                requestAttempt = previousAttempts.get(bulkError.getOriginalPosition()) + 1;
                            }

                            List<String> bulkErrorPassReasons = new ArrayList<String>();
                            BulkWriteFailure failure = new BulkWriteFailure(
                                    bulkError.getDocumentStatus(),
                                    // FIXHERE: Pick a better Exception type?
                                    new Exception(bulkError.getErrorMessage()),
                                    bulkError.getDocument(),
                                    requestAttempt,
                                    bulkErrorPassReasons
                            );
                            for (BulkWriteErrorHandler errorHandler : documentBulkErrorHandlers) {
                                HandlerResult result;
                                try {
                                    result = errorHandler.onError(failure, errorCollector);
                                } catch (EsHadoopAbortHandlerException ahe) {
                                    throw new EsHadoopException(ahe.getMessage());
                                } catch (Exception e) {
                                    throw new EsHadoopException("Encountered exception during error handler. Treating " +
                                            "it as an ABORT result.", e);
                                }

                                switch (result) {
                                    case HANDLED:
                                        Assert.isTrue(errorCollector.getAndClearMessage() == null,
                                                "Found pass message with Handled response. Be sure to return the value " +
                                                        "returned from pass(String) call.");
                                        // Check for document retries
                                        if (errorCollector.receivedRetries()) {
                                            BytesArray original = bulkError.getDocument();
                                            byte[] retryDataBuffer = errorCollector.getAndClearRetryValue();
                                            if (retryDataBuffer == null) {
                                                // Retry the same data.
                                                // Continue to track the previous attempts.
                                                attempts.add(requestAttempt);
                                            } else if (original.bytes() == retryDataBuffer) {
                                                // If we receive an array that is identity equal to the tracking bytes
                                                // array, then we'll use the same document from the tracking bytes array
                                                // as there have been no changes to it.
                                                // We will continue tracking previous attempts though.
                                                attempts.add(requestAttempt);
                                            } else {
                                                // Check document contents to see if it was deserialized and reserialized.
                                                byte[] originalContent = new byte[original.length()];
                                                System.arraycopy(original.bytes(), original.offset(), originalContent, 0, original.length());
                                                if (Arrays.equals(originalContent, retryDataBuffer)) {
                                                    // Same document content. Leave the data as is in tracking buffer,
                                                    // and continue tracking previous attempts.
                                                    attempts.add(requestAttempt);
                                                } else {
                                                    // Document has changed.
                                                    // Track new attempts.
                                                    // FIXHERE: This removal operation "shifts" all entries, so each removal will be off by one.
                                                    int currentPosition = bulkError.getCurrentArrayPosition();
                                                    data.remove(currentPosition);
                                                    data.copyFrom(new BytesArray(retryDataBuffer));
                                                    // Don't add item to attempts. When it's exhausted we'll assume 1's.
                                                }
                                            }
                                        }
                                        break;
                                    case PASS:
                                        String reason = errorCollector.getAndClearMessage();
                                        if (reason != null) {
                                            bulkErrorPassReasons.add(reason);
                                        }
                                        continue;
                                    case ABORT:
                                        errorCollector.getAndClearMessage(); // Sanity clearing
                                        throw new EsHadoopException("Error Handler returned an ABORT result for failed " +
                                                "bulk document. HTTP Status [" + bulkError.getDocumentStatus() +
                                                "], Error Message [" + bulkError.getErrorMessage() + "], Document Entry [" +
                                                bulkError.getDocument().toString() + "]");
                                }
                            }
                        }

                        if (!attempts.isEmpty()) {
                            retryOperation = true;
                            waitTime = errorCollector.getDelayTimeBetweenRetries();
                        } else {
                            retryOperation = false;
                        }
                    } else {
                        // Everything is good to go!
                        retryOperation = false;
                    }
                } while (retryOperation);
            } else {
                bulkResult = BulkResponse.complete();
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
        // FIXHERE: Begin here for determining rest response code handling for bulk operations.
        BulkResponse bulk = tryFlush();
        if (!bulk.getDocumentErrors().isEmpty()) {
            String header = String.format("Could not write all entries [%s/%s] (Maybe ES was overloaded?). Error sample (first [%s] error messages):\n", bulk.getDocumentErrors().size(), bulk.getTotalDocs(), 5);
            StringBuilder message = new StringBuilder(header);
            int i = 0;
            for (BulkResponse.BulkError errors : bulk.getDocumentErrors()) {
                if (i >=5 ) {
                    break;
                }
                message.append("\t").append(errors.getErrorMessage()).append("\n");
                i++;
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
                client.refresh(resources.getResourceWrite());

                if (log.isDebugEnabled()) {
                    log.debug(String.format("Refreshing index [%s]", resources.getResourceWrite()));
                }
            }
        } finally {
            client.close();
            stats.aggregate(client.stats());
            client = null;
            for (BulkWriteErrorHandler handler : documentBulkErrorHandlers) {
                handler.close();
            }
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
        return FieldParser.parseMapping(client.getMapping(resources.getResourceRead().mapping()));
    }

    public Map<String, GeoField> sampleGeoFields(Mapping mapping) {
        Map<String, GeoType> fields = MappingUtils.geoFields(mapping);
        Map<String, Object> geoMapping = client.sampleForFields(resources.getResourceRead().index(), resources.getResourceRead().type(), fields.keySet());

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
        Resource res = (read ? resources.getResourceRead() : resources.getResourceWrite());
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
        String index = resources.getResourceRead().index();
        return !(index.contains(",") || index.contains("*") || client.isAlias(resources.getResourceRead().aliases()));
    }

    public void putMapping(BytesArray mapping) {
        client.putMapping(resources.getResourceWrite().index(), resources.getResourceWrite().mapping(), mapping.bytes());
    }

    public boolean touch() {
        return client.touch(resources.getResourceWrite().index());
    }

    public void delete() {
        if (client.internalVersion.on(EsMajorVersion.V_1_X)) {
            // ES 1.x - delete as usual
            // Delete just the mapping
            client.delete(resources.getResourceWrite().index() + "/" + resources.getResourceWrite().type());
        }
        else {
            // try first a blind delete by query (since the plugin might be installed)
            try {
                client.delete(resources.getResourceWrite().index() + "/" + resources.getResourceWrite().type() + "/_query?q=*");
            } catch (EsHadoopInvalidRequest ehir) {
                log.info("Skipping delete by query as the plugin is not installed...");
            }

            // in ES 2.0 and higher this means scrolling and deleting the docs by hand...
            // do a scroll-scan without source

            // as this is a delete, there's not much value in making this configurable so we just go for some sane/safe defaults
            // 10m scroll timeout
            // 250 results

            int batchSize = 500;
            StringBuilder sb = new StringBuilder(resources.getResourceWrite().index() + "/" + resources.getResourceWrite().type());
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
                client.refresh(resources.getResourceWrite());
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
        return client.count(res.index() + "/" + res.type(), QueryUtils.parseQuery(settings));
    }

    public boolean waitForYellow() {
        return client.waitForHealth(resources.getResourceWrite().index(), RestClient.Health.YELLOW, TimeValue.timeValueSeconds(10));
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