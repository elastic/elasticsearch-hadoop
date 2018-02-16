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
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.FieldPresenceValidation;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.query.BoolQueryBuilder;
import org.elasticsearch.hadoop.rest.query.ConstantScoreQueryBuilder;
import org.elasticsearch.hadoop.rest.query.QueryBuilder;
import org.elasticsearch.hadoop.rest.query.QueryUtils;
import org.elasticsearch.hadoop.rest.query.RawQueryBuilder;
import org.elasticsearch.hadoop.rest.request.GetAliasesRequestBuilder;
import org.elasticsearch.hadoop.serialization.ScrollReader;
import org.elasticsearch.hadoop.serialization.ScrollReader.ScrollReaderConfig;
import org.elasticsearch.hadoop.serialization.builder.ValueReader;
import org.elasticsearch.hadoop.serialization.dto.IndicesAliases;
import org.elasticsearch.hadoop.serialization.dto.NodeInfo;
import org.elasticsearch.hadoop.serialization.dto.ShardInfo;
import org.elasticsearch.hadoop.serialization.dto.mapping.Mapping;
import org.elasticsearch.hadoop.serialization.dto.mapping.MappingSet;
import org.elasticsearch.hadoop.serialization.dto.mapping.MappingUtils;
import org.elasticsearch.hadoop.serialization.field.IndexExtractor;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.EsMajorVersion;
import org.elasticsearch.hadoop.util.IOUtils;
import org.elasticsearch.hadoop.util.ObjectUtils;
import org.elasticsearch.hadoop.util.SettingsUtils;
import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.hadoop.util.Version;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public abstract class RestService implements Serializable {
    public static class PartitionReader implements Closeable {
        public final ScrollReader scrollReader;
        public final RestRepository client;
        public final SearchRequestBuilder queryBuilder;

        private ScrollQuery scrollQuery;

        private boolean closed = false;

        PartitionReader(ScrollReader scrollReader, RestRepository client, SearchRequestBuilder queryBuilder) {
            this.scrollReader = scrollReader;
            this.client = client;
            this.queryBuilder = queryBuilder;
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                if (scrollQuery != null) {
                    scrollQuery.close();
                }
                client.close();
            }
        }

        public ScrollQuery scrollQuery() {
            if (scrollQuery == null) {
                scrollQuery = queryBuilder.build(client, scrollReader);
            }

            return scrollQuery;
        }
    }

    public static class PartitionWriter implements Closeable {
        public final RestRepository repository;
        public final long number;
        public final int total;
        public final Settings settings;

        private boolean closed = false;

        PartitionWriter(Settings settings, long splitIndex, int splitsSize, RestRepository repository) {
            this.settings = settings;
            this.repository = repository;
            this.number = splitIndex;
            this.total = splitsSize;
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                repository.close();
            }
        }
    }

    public static class MultiReaderIterator implements Closeable, Iterator {
        private final List<PartitionDefinition> definitions;
        private final Iterator<PartitionDefinition> definitionIterator;
        private PartitionReader currentReader;
        private ScrollQuery currentScroll;
        private boolean finished = false;

        private final Settings settings;
        private final Log log;

        MultiReaderIterator(List<PartitionDefinition> defs, Settings settings, Log log) {
            this.definitions = defs;
            definitionIterator = defs.iterator();

            this.settings = settings;
            this.log = log;
        }

        @Override
        public void close() {
            if (finished) {
                return;
            }

            ScrollQuery sq = getCurrent();
            if (sq != null) {
                sq.close();
            }
            if (currentReader != null) {
                currentReader.close();
            }

            finished = true;
        }

        @Override
        public boolean hasNext() {
            ScrollQuery sq = getCurrent();
            return (sq != null ? sq.hasNext() : false);
        }

        private ScrollQuery getCurrent() {
            if (finished) {
                return null;
            }


            for (boolean hasValue = false; !hasValue; ) {
                if (currentReader == null) {
                    if (definitionIterator.hasNext()) {
                        currentReader = RestService.createReader(settings, definitionIterator.next(), log);
                    } else {
                        finished = true;
                        return null;
                    }
                }

                if (currentScroll == null) {
                    currentScroll = currentReader.scrollQuery();
                }

                hasValue = currentScroll.hasNext();

                if (!hasValue) {
                    currentScroll.close();
                    currentScroll = null;

                    currentReader.close();
                    currentReader = null;
                }
            }

            return currentScroll;
        }

        @Override
        public Object[] next() {
            ScrollQuery sq = getCurrent();
            return sq.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    @SuppressWarnings("unchecked")
    public static List<PartitionDefinition> findPartitions(Settings settings, Log log) {
        Version.logVersion();

        InitializationUtils.validateSettings(settings);
        InitializationUtils.validateSettingsForReading(settings);

        EsMajorVersion version = InitializationUtils.discoverEsVersion(settings, log);
        List<NodeInfo> nodes = InitializationUtils.discoverNodesIfNeeded(settings, log);
        InitializationUtils.filterNonClientNodesIfNeeded(settings, log);
        InitializationUtils.filterNonDataNodesIfNeeded(settings, log);
        InitializationUtils.filterNonIngestNodesIfNeeded(settings, log);

        RestRepository client = new RestRepository(settings);
        try {
            boolean indexExists = client.indexExists(true);

            List<List<Map<String, Object>>> shards = null;

            if (!indexExists) {
                if (settings.getIndexReadMissingAsEmpty()) {
                    log.info(String.format("Index [%s] missing - treating it as empty", settings.getResourceRead()));
                    shards = Collections.emptyList();
                } else {
                    throw new EsHadoopIllegalArgumentException(
                            String.format("Index [%s] missing and settings [%s] is set to false", settings.getResourceRead(), ConfigurationOptions.ES_INDEX_READ_MISSING_AS_EMPTY));
                }
            } else {
                shards = client.getReadTargetShards();
                if (log.isTraceEnabled()) {
                    log.trace("Creating splits for shards " + shards);
                }
            }

            log.info(String.format("Reading from [%s]", settings.getResourceRead()));

            MappingSet mapping = null;
            if (!shards.isEmpty()) {
                mapping = client.getMappings();
                if (log.isDebugEnabled()) {
                    log.debug(String.format("Discovered resolved mapping {%s} for [%s]", mapping.getResolvedView(), settings.getResourceRead()));
                }
                // validate if possible
                FieldPresenceValidation validation = settings.getReadFieldExistanceValidation();
                if (validation.isRequired()) {
                    MappingUtils.validateMapping(SettingsUtils.determineSourceFields(settings), mapping.getResolvedView(), validation, log);
                }
            }
            final Map<String, NodeInfo> nodesMap = new HashMap<String, NodeInfo>();
            if (nodes != null) {
                for (NodeInfo node : nodes) {
                    nodesMap.put(node.getId(), node);
                }
            }
            final List<PartitionDefinition> partitions;
            if (version.onOrAfter(EsMajorVersion.V_5_X) && settings.getInputUseSlicedPartitions()) {
                partitions = findSlicePartitions(client.getRestClient(), settings, mapping, nodesMap, shards, log);
            } else {
                partitions = findShardPartitions(settings, mapping, nodesMap, shards, log);
            }
            Collections.shuffle(partitions);
            return partitions;
        } finally {
            client.close();
        }
    }

    /**
     * Create one {@link PartitionDefinition} per shard for each requested index.
     */
    static List<PartitionDefinition> findShardPartitions(Settings settings, MappingSet mappingSet, Map<String, NodeInfo> nodes,
                                                         List<List<Map<String, Object>>> shards, Log log) {
        Mapping resolvedMapping = mappingSet == null ? null : mappingSet.getResolvedView();
        List<PartitionDefinition> partitions = new ArrayList<PartitionDefinition>(shards.size());
        for (List<Map<String, Object>> group : shards) {
            String index = null;
            int shardId = -1;
            List<String> locationList = new ArrayList<String> ();
            for (Map<String, Object> replica : group) {
                ShardInfo shard = new ShardInfo(replica);
                index = shard.getIndex();
                shardId = shard.getName();
                if (nodes.containsKey(shard.getNode())) {
                    locationList.add(nodes.get(shard.getNode()).getPublishAddress());
                }
            }
            if (index == null) {
                // Could not find shards for this partition. Continue anyway?
                if (settings.getIndexReadAllowRedStatus()) {
                    log.warn("Shard information is missing from an index and will not be reached during job execution. " +
                            "Assuming shard is unavailable and cluster is red! Continuing with read operation by " +
                            "skipping this shard! This may result in incomplete data retrieval!");
                } else {
                    throw new IllegalStateException("Could not locate shard information for one of the read indices. " +
                            "Check your cluster status to see if it is unstable!");
                }
            } else {
                PartitionDefinition partition = new PartitionDefinition(settings, resolvedMapping, index, shardId,
                        locationList.toArray(new String[0]));
                partitions.add(partition);
            }
        }
        return partitions;
    }

    /**
     * Partitions the query based on the max number of documents allowed per partition {@link Settings#getMaxDocsPerPartition()}.
     */
    static List<PartitionDefinition> findSlicePartitions(RestClient client, Settings settings, MappingSet mappingSet,
                                                         Map<String, NodeInfo> nodes, List<List<Map<String, Object>>> shards, Log log) {
        QueryBuilder query = QueryUtils.parseQueryAndFilters(settings);
        int maxDocsPerPartition = settings.getMaxDocsPerPartition();
        String types = new Resource(settings, true).type();
        Mapping resolvedMapping = mappingSet == null ? null : mappingSet.getResolvedView();

        List<PartitionDefinition> partitions = new ArrayList<PartitionDefinition>(shards.size());
        for (List<Map<String, Object>> group : shards) {
            String index = null;
            int shardId = -1;
            List<String> locationList = new ArrayList<String> ();
            for (Map<String, Object> replica : group) {
                ShardInfo shard = new ShardInfo(replica);
                index = shard.getIndex();
                shardId = shard.getName();
                if (nodes.containsKey(shard.getNode())) {
                    locationList.add(nodes.get(shard.getNode()).getPublishAddress());
                }
            }
            String[] locations = locationList.toArray(new String[0]);
            if (index == null) {
                // Could not find shards for this partition. Continue anyway?
                if (settings.getIndexReadAllowRedStatus()) {
                    log.warn("Shard information is missing from an index and will not be reached during job execution. " +
                            "Assuming shard is unavailable and cluster is red! Continuing with read operation by " +
                            "skipping this shard! This may result in incomplete data retrieval!");
                } else {
                    throw new IllegalStateException("Could not locate shard information for one of the read indices. " +
                            "Check your cluster status to see if it is unstable!");
                }
            } else {
                StringBuilder indexAndType = new StringBuilder(index);
                if (StringUtils.hasLength(types)) {
                    indexAndType.append("/");
                    indexAndType.append(types);
                }
                // TODO applyAliasMetaData should be called in order to ensure that the count are exact (alias filters and routing may change the number of documents)
                long numDocs = client.count(indexAndType.toString(), Integer.toString(shardId), query);
                int numPartitions = (int) Math.max(1, numDocs / maxDocsPerPartition);
                for (int i = 0; i < numPartitions; i++) {
                    PartitionDefinition.Slice slice = new PartitionDefinition.Slice(i, numPartitions);
                    partitions.add(new PartitionDefinition(settings, resolvedMapping, index, shardId, slice, locations));
                }
            }
        }
        return partitions;
    }

    /**
     * Returns the first address in {@code locations} that is equals to a public IP of the system
     * @param locations The list of address (hostname:port or ip:port) to check
     * @return The first address in {@code locations} that is equals to a public IP of the system or null if none
     */
    static String checkLocality(String[] locations, Log log) {
        try {
            InetAddress[] candidates = NetworkUtils.getGlobalInterfaces();
            for (String address : locations) {
                StringUtils.IpAndPort ipAndPort = StringUtils.parseIpAddress(address);
                InetAddress addr = InetAddress.getByName(ipAndPort.ip);
                for (InetAddress candidate : candidates) {
                    if (addr.equals(candidate)) {
                        return address;
                    }
                }
            }
        } catch (SocketException e) {
            if (log.isDebugEnabled()) {
                log.debug("Unable to retrieve the global interfaces of the system", e);
            }
        } catch (UnknownHostException e) {
            if (log.isDebugEnabled()) {
                log.debug("Unable to retrieve IP address", e);
            }
        }
        return null;
    }

    /**
     * Creates a PartitionReader from a {@code PartitionDefinition}
     * @param settings The settings for the reader
     * @param partition The {@link PartitionDefinition} used to create the reader
     * @param log The logger
     * @return The {@link PartitionReader} that is able to read the documents associated with the {@code partition}
     */
    public static PartitionReader createReader(Settings settings, PartitionDefinition partition, Log log) {
        if (!SettingsUtils.hasPinnedNode(settings) && partition.getLocations().length > 0) {
            String pinAddress = checkLocality(partition.getLocations(), log);
            if (pinAddress != null) {
                if (log.isDebugEnabled()) {
                    log.debug(String.format("Partition reader instance [%s] assigned to [%s]", partition, pinAddress));
                }
                SettingsUtils.pinNode(settings, pinAddress);
            }
        }
        EsMajorVersion version = InitializationUtils.discoverEsVersion(settings, log);
        ValueReader reader = ObjectUtils.instantiate(settings.getSerializerValueReaderClassName(), settings);
        // initialize REST client
        RestRepository repository = new RestRepository(settings);
        Mapping fieldMapping = null;
        if (StringUtils.hasText(partition.getSerializedMapping())) {
            fieldMapping = IOUtils.deserializeFromBase64(partition.getSerializedMapping());
        }
        else {
            log.warn(String.format("No mapping found for [%s] - either no index exists or the partition configuration has been corrupted", partition));
        }

        ScrollReader scrollReader = new ScrollReader(new ScrollReaderConfig(reader, fieldMapping, settings));
        if (settings.getNodesClientOnly()) {
            String clientNode = repository.getRestClient().getCurrentNode();
            if (log.isDebugEnabled()) {
                log.debug(String.format("Client-node routing detected; partition reader instance [%s] assigned to [%s]",
                        partition, clientNode));
            }
            SettingsUtils.pinNode(settings, clientNode);
        }

        // take into account client node routing
        boolean includeVersion = settings.getReadMetadata() && settings.getReadMetadataVersion();
        Resource read = new Resource(settings, true);
        SearchRequestBuilder requestBuilder =
                new SearchRequestBuilder(version, includeVersion)
                        .types(read.type())
                        .indices(partition.getIndex())
                        .query(QueryUtils.parseQuery(settings))
                        .scroll(settings.getScrollKeepAlive())
                        .size(settings.getScrollSize())
                        .limit(settings.getScrollLimit())
                        .fields(SettingsUtils.determineSourceFields(settings))
                        .filters(QueryUtils.parseFilters(settings))
                        .shard(Integer.toString(partition.getShardId()))
                        .local(true)
                        .excludeSource(settings.getExcludeSource());
        if (partition.getSlice() != null && partition.getSlice().max > 1) {
            requestBuilder.slice(partition.getSlice().id, partition.getSlice().max);
        }
        String[] indices = read.index().split(",");
        if (QueryUtils.isExplicitlyRequested(partition.getIndex(), indices) == false) {
            IndicesAliases indicesAliases =
                    new GetAliasesRequestBuilder(repository.getRestClient())
                            .indices(partition.getIndex())
                            .execute().getIndices();
            Map<String, IndicesAliases.Alias> aliases = indicesAliases.getAliases(partition.getIndex());
            if (aliases != null && aliases.size() > 0) {
                requestBuilder = applyAliasMetadata(version, aliases, requestBuilder, partition.getIndex(), indices);
            }
        }
        return new PartitionReader(scrollReader, repository, requestBuilder);
    }

    /**
     * Check if the index name is part of the requested indices or the result of an alias.
     * If the index is the result of an alias, the filters and routing values of the alias are added in the
     * provided {@link SearchRequestBuilder}.
     */
    static SearchRequestBuilder applyAliasMetadata(EsMajorVersion version,
                                                   Map<String, IndicesAliases.Alias> aliases,
                                                   SearchRequestBuilder searchRequestBuilder,
                                                   String index, String... indicesOrAliases) {
        if (QueryUtils.isExplicitlyRequested(index, indicesOrAliases)) {
            return searchRequestBuilder;
        }

        Set<String> routing = new HashSet<String>();
        List<QueryBuilder> aliasFilters = new ArrayList<QueryBuilder>();
        for (IndicesAliases.Alias alias : aliases.values()) {
            if (QueryUtils.isExplicitlyRequested(alias.getName(), indicesOrAliases)) {
                // The alias is explicitly requested
                if (StringUtils.hasLength(alias.getSearchRouting())) {
                    for (String value : alias.getSearchRouting().split(",")) {
                        routing.add(value.trim());
                    }
                }
                if (alias.getFilter() != null) {
                    try {
                        aliasFilters.add(new RawQueryBuilder(alias.getFilter(), false));
                    } catch (IOException e) {
                        throw new EsHadoopIllegalArgumentException("Failed to parse alias filter: [" + alias.getFilter() + "]");
                    }
                }
            }
        }
        if (aliasFilters.size() > 0) {
            QueryBuilder aliasQuery;
            if (aliasFilters.size() == 1) {
                aliasQuery = aliasFilters.get(0);
            } else {
                aliasQuery = new BoolQueryBuilder();
                for (QueryBuilder filter : aliasFilters) {
                    ((BoolQueryBuilder) aliasQuery).should(filter);
                }
            }
            if (searchRequestBuilder.query() == null) {
                searchRequestBuilder.query(aliasQuery);
            } else {
                BoolQueryBuilder mainQuery = new BoolQueryBuilder();
                mainQuery.must(searchRequestBuilder.query());
                if (version.after(EsMajorVersion.V_1_X)) {
                    mainQuery.filter(aliasQuery);
                } else {
                    mainQuery.must(new ConstantScoreQueryBuilder().filter(aliasQuery).boost(0.0f));
                }
                searchRequestBuilder.query(mainQuery);
            }
        }
        if (routing.size() > 0) {
            searchRequestBuilder.routing(StringUtils.concatenate(routing, ","));
        }
        return searchRequestBuilder;
    }

    // expects currentTask to start from 0
    public static List<PartitionDefinition> assignPartitions(List<PartitionDefinition> partitions, int currentTask, int totalTasks) {
        int esPartitions = partitions.size();
        if (totalTasks >= esPartitions) {
            return (currentTask >= esPartitions ? Collections.<PartitionDefinition>emptyList() : Collections.singletonList(partitions.get(currentTask)));
        } else {
            int partitionsPerTask = esPartitions / totalTasks;
            int remainder = esPartitions % totalTasks;

            int partitionsPerCurrentTask = partitionsPerTask;

            // spread the reminder against the tasks
            if (currentTask < remainder) {
                partitionsPerCurrentTask++;
            }

            // find the offset inside the collection
            int offset = partitionsPerTask * currentTask;
            if (currentTask != 0) {
                offset += (remainder > currentTask ? 1 : remainder);
            }

            // common case
            if (partitionsPerCurrentTask == 1) {
                return Collections.singletonList(partitions.get(offset));
            }

            List<PartitionDefinition> pa = new ArrayList<PartitionDefinition>(partitionsPerCurrentTask);
            for (int index = offset; index < offset + partitionsPerCurrentTask; index++) {
                pa.add(partitions.get(index));
            }
            return pa;
        }
    }

    public static MultiReaderIterator multiReader(Settings settings, List<PartitionDefinition> definitions, Log log) {
        return new MultiReaderIterator(definitions, settings, log);
    }

    public static PartitionWriter createWriter(Settings settings, long currentSplit, int totalSplits, Log log) {
        Version.logVersion();

        InitializationUtils.validateSettings(settings);
        InitializationUtils.discoverEsVersion(settings, log);

        InitializationUtils.validateSettingsForWriting(settings);

        InitializationUtils.discoverNodesIfNeeded(settings, log);
        InitializationUtils.filterNonClientNodesIfNeeded(settings, log);
        InitializationUtils.filterNonDataNodesIfNeeded(settings, log);
        InitializationUtils.filterNonIngestNodesIfNeeded(settings, log);

        List<String> nodes = SettingsUtils.discoveredOrDeclaredNodes(settings);

        // check invalid splits (applicable when running in non-MR environments) - in this case fall back to Random..
        int selectedNode = (currentSplit < 0) ? new Random().nextInt(nodes.size()) : (int)(currentSplit % nodes.size());

        // select the appropriate nodes first, to spread the load before-hand
        SettingsUtils.pinNode(settings, nodes.get(selectedNode));

        Resource resource = new Resource(settings, false);

        log.info(String.format("Writing to [%s]", resource));

        // single index vs multi indices
        IndexExtractor iformat = ObjectUtils.instantiate(settings.getMappingIndexExtractorClassName(), settings);
        iformat.compile(resource.toString());

        RestRepository repository = (iformat.hasPattern() ? initMultiIndices(settings, currentSplit, resource, log) : initSingleIndex(settings, currentSplit, resource, log));

        return new PartitionWriter(settings, currentSplit, totalSplits, repository);
    }

    private static RestRepository initSingleIndex(Settings settings, long currentInstance, Resource resource, Log log) {
        if (log.isDebugEnabled()) {
            log.debug(String.format("Resource [%s] resolves as a single index", resource));
        }

        RestRepository repository = new RestRepository(settings);
        // create the index if needed
        if (repository.touch()) {
            if (repository.waitForYellow()) {
                log.warn(String.format("Timed out waiting for index [%s] to reach yellow health", resource));
            }
        }

        // if WAN mode is used, use an already selected node
        if (settings.getNodesWANOnly()) {
            String node = SettingsUtils.getPinnedNode(settings);
            if (log.isDebugEnabled()) {
                log.debug(String.format("Partition writer instance [%s] assigned to [%s]", currentInstance, node));
            }

            return repository;
        }

        // if client-nodes are used, simply use the underlying nodes
        if (settings.getNodesClientOnly()) {
            String clientNode = repository.getRestClient().getCurrentNode();
            if (log.isDebugEnabled()) {
                log.debug(String.format("Client-node routing detected; partition writer instance [%s] assigned to [%s]",
                        currentInstance, clientNode));
            }

            return repository;
        }

        // no routing necessary; select the relevant target shard/node
        Map<ShardInfo, NodeInfo> targetShards = Collections.emptyMap();

        targetShards = repository.getWriteTargetPrimaryShards(settings.getNodesClientOnly());
        repository.close();

        Assert.isTrue(!targetShards.isEmpty(),
                String.format("Cannot determine write shards for [%s]; likely its format is incorrect (maybe it contains illegal characters?)", resource));


        List<ShardInfo> orderedShards = new ArrayList<ShardInfo>(targetShards.keySet());
        // make sure the order is strict
        Collections.sort(orderedShards);
        if (log.isTraceEnabled()) {
            log.trace(String.format("Partition writer instance [%s] discovered [%s] primary shards %s", currentInstance, orderedShards.size(), orderedShards));
        }

        // if there's no task info, just pick a random bucket
        if (currentInstance <= 0) {
            currentInstance = new Random().nextInt(targetShards.size()) + 1;
        }
        int bucket = (int)(currentInstance % targetShards.size());
        ShardInfo chosenShard = orderedShards.get(bucket);
        NodeInfo targetNode = targetShards.get(chosenShard);

        // pin settings
        SettingsUtils.pinNode(settings, targetNode.getPublishAddress());
        String node = SettingsUtils.getPinnedNode(settings);
        repository = new RestRepository(settings);

        if (log.isDebugEnabled()) {
            log.debug(String.format("Partition writer instance [%s] assigned to primary shard [%s] at address [%s]",
                    currentInstance, chosenShard.getName(), node));
        }

        return repository;
    }

    private static RestRepository initMultiIndices(Settings settings, long currentInstance, Resource resource, Log log) {
        if (log.isDebugEnabled()) {
            log.debug(String.format("Resource [%s] resolves as an index pattern", resource));
        }

        // multi-index write - since we don't know before hand what index will be used, use an already selected node
        String node = SettingsUtils.getPinnedNode(settings);
        if (log.isDebugEnabled()) {
            log.debug(String.format("Partition writer instance [%s] assigned to [%s]", currentInstance, node));
        }

        return new RestRepository(settings);
    }
}
