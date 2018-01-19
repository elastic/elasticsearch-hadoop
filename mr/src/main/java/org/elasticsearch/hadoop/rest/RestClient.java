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

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.Request.Method;
import org.elasticsearch.hadoop.rest.query.QueryBuilder;
import org.elasticsearch.hadoop.rest.stats.Stats;
import org.elasticsearch.hadoop.rest.stats.StatsAware;
import org.elasticsearch.hadoop.serialization.ParsingUtils;
import org.elasticsearch.hadoop.serialization.dto.NodeInfo;
import org.elasticsearch.hadoop.serialization.json.JacksonJsonGenerator;
import org.elasticsearch.hadoop.serialization.json.JacksonJsonParser;
import org.elasticsearch.hadoop.serialization.json.JsonFactory;
import org.elasticsearch.hadoop.serialization.json.ObjectReader;
import org.elasticsearch.hadoop.util.ByteSequence;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.EsMajorVersion;
import org.elasticsearch.hadoop.util.FastByteArrayOutputStream;
import org.elasticsearch.hadoop.util.IOUtils;
import org.elasticsearch.hadoop.util.ObjectUtils;
import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.hadoop.util.TrackingBytesArray;
import org.elasticsearch.hadoop.util.encoding.HttpEncodingTools;
import org.elasticsearch.hadoop.util.unit.TimeValue;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import static org.elasticsearch.hadoop.rest.Request.Method.DELETE;
import static org.elasticsearch.hadoop.rest.Request.Method.GET;
import static org.elasticsearch.hadoop.rest.Request.Method.HEAD;
import static org.elasticsearch.hadoop.rest.Request.Method.POST;
import static org.elasticsearch.hadoop.rest.Request.Method.PUT;

public class RestClient implements Closeable, StatsAware {

    private final static int MAX_BULK_ERROR_MESSAGES = 5;

    private NetworkClient network;
    private final ObjectMapper mapper;
    private final TimeValue scrollKeepAlive;
    private final boolean indexReadMissingAsEmpty;
    private final HttpRetryPolicy retryPolicy;
    final EsMajorVersion internalVersion;
    private final ErrorExtractor errorExtractor;

    {
        mapper = new ObjectMapper();
        mapper.configure(DeserializationConfig.Feature.USE_ANNOTATIONS, false);
        mapper.configure(SerializationConfig.Feature.USE_ANNOTATIONS, false);
    }


    private final Stats stats = new Stats();

    public enum Health {
        RED, YELLOW, GREEN
    }

    public RestClient(Settings settings) {
        network = new NetworkClient(settings);

        scrollKeepAlive = TimeValue.timeValueMillis(settings.getScrollKeepAlive());
        indexReadMissingAsEmpty = settings.getIndexReadMissingAsEmpty();

        String retryPolicyName = settings.getBatchWriteRetryPolicy();

        if (ConfigurationOptions.ES_BATCH_WRITE_RETRY_POLICY_SIMPLE.equals(retryPolicyName)) {
            retryPolicyName = SimpleHttpRetryPolicy.class.getName();
        }
        else if (ConfigurationOptions.ES_BATCH_WRITE_RETRY_POLICY_NONE.equals(retryPolicyName)) {
            retryPolicyName = NoHttpRetryPolicy.class.getName();
        }

        retryPolicy = ObjectUtils.instantiate(retryPolicyName, settings);
        // Assume that the elasticsearch major version is the latest if the version is not already present in the settings
        internalVersion = settings.getInternalVersionOrLatest();
        errorExtractor = new ErrorExtractor(internalVersion);
    }

    public List<NodeInfo> getHttpNodes(boolean clientNodeOnly) {
        Map<String, Map<String, Object>> nodesData = get("_nodes/http", "nodes");
        List<NodeInfo> nodes = new ArrayList<NodeInfo>();

        for (Entry<String, Map<String, Object>> entry : nodesData.entrySet()) {
            NodeInfo node = new NodeInfo(entry.getKey(), entry.getValue());
            if (node.hasHttp() && (!clientNodeOnly || node.isClient())) {
                nodes.add(node);
            }
        }
        return nodes;
    }

    public List<NodeInfo> getHttpClientNodes() {
        return getHttpNodes(true);
    }

    public List<NodeInfo> getHttpDataNodes() {
        List<NodeInfo> nodes = getHttpNodes(false);

        Iterator<NodeInfo> it = nodes.iterator();
        while (it.hasNext()) {
            NodeInfo node = it.next();
            if (!node.isData()) {
                it.remove();
            }
        }
        return nodes;
    }

    public List<NodeInfo> getHttpIngestNodes() {
        List<NodeInfo> nodes = getHttpNodes(false);

        Iterator<NodeInfo> it = nodes.iterator();
        while (it.hasNext()) {
            NodeInfo nodeInfo = it.next();
            if (!nodeInfo.isIngest()) {
                it.remove();
            }
        }
        return nodes;
    }

    public <T> T get(String q, String string) {
        return parseContent(execute(GET, q), string);
    }

    @SuppressWarnings("unchecked")
    private <T> T parseContent(InputStream content, String string) {
        Map<String, Object> map = Collections.emptyMap();

        try {
            // create parser manually to lower Jackson requirements
            JsonParser jsonParser = mapper.getJsonFactory().createJsonParser(content);
            try {
                map = mapper.readValue(jsonParser, Map.class);
            } finally {
                countStreamStats(content);
            }
        } catch (IOException ex) {
            throw new EsHadoopParsingException(ex);
        }

        return (T) (string != null ? map.get(string) : map);
    }

    public static class BulkActionResponse {
        private Iterator<Map> entries;
        private long timeSpent;
        private int responseCode;

        public BulkActionResponse(Iterator<Map> entries, int responseCode, long timeSpent) {
            this.entries = entries;
            this.timeSpent = timeSpent;
            this.responseCode = responseCode;
        }

        public Iterator<Map> getEntries() {
            return entries;
        }

        public long getTimeSpent() {
            return timeSpent;
        }

        public int getResponseCode() {
            return responseCode;
        }
    }

    /**
     * Executes a single bulk operation against the provided resource, using the passed data as the request body.
     * This method will retry bulk requests if the entire bulk request fails, but will not retry singular
     * document failures.
     *
     * @param resource target of the bulk request.
     * @param data bulk request body. This body will be cleared of entries on any successful bulk request.
     * @return a BulkActionResponse object that will detail if there were failing documents that should be retried.
     */
    public BulkActionResponse bulk(Resource resource, TrackingBytesArray data) {
        // NB: dynamically get the stats since the transport can change
        long start = network.transportStats().netTotalTime;
        Response response = execute(PUT, resource.bulk(), data);
        long spent = network.transportStats().netTotalTime - start;

        stats.bulkTotal++;
        stats.docsSent += data.entries();
        stats.bulkTotalTime += spent;
        // bytes will be counted by the transport layer

        return new BulkActionResponse(parseBulkActionResponse(response), response.status(), spent);
    }

    @SuppressWarnings("rawtypes")
    Iterator<Map> parseBulkActionResponse(Response response) {
        InputStream content = response.body();
        // Check for failed writes
        try {
            ObjectReader r = JsonFactory.objectReader(mapper, Map.class);
            JsonParser parser = mapper.getJsonFactory().createJsonParser(content);
            try {
                if (ParsingUtils.seek(new JacksonJsonParser(parser), "items") == null) {
                    return Collections.<Map>emptyList().iterator();
                } else {
                    return r.readValues(parser);
                }
            } finally {
                countStreamStats(content);
            }
        } catch (IOException ex) {
            throw new EsHadoopParsingException(ex);
        }
    }

    public void refresh(Resource resource) {
        execute(POST, resource.refresh());
    }

    public List<List<Map<String, Object>>> targetShards(String index, String routing) {
        List<List<Map<String, Object>>> shardsJson = null;

        // https://github.com/elasticsearch/elasticsearch/issues/2726
        String target = index + "/_search_shards";
        if (routing != null) {
            target += "?routing=" + HttpEncodingTools.encode(routing);
        }
        if (indexReadMissingAsEmpty) {
            Request req = new SimpleRequest(GET, null, target);
            Response res = executeNotFoundAllowed(req);
            if (res.status() == HttpStatus.OK) {
                shardsJson = parseContent(res.body(), "shards");
            }
            else {
                shardsJson = Collections.emptyList();
            }
        }
        else {
            shardsJson = get(target, "shards");
        }

        return shardsJson;
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getMapping(String query) {
        return (Map<String, Object>) get(query, null);
    }

    public Map<String, Object> sampleForFields(String index, String type, Collection<String> fields) {
        if (fields == null || fields.isEmpty()) {
            return Collections.emptyMap();
        }

        StringBuilder sb = new StringBuilder();
        sb.append("{ \"terminate_after\":1, \"size\":1,\n");
        // use source since some fields might be objects
        sb.append("\"_source\": [");
        for (String field : fields) {
            sb.append(String.format(Locale.ROOT, "\"%s\",", field));
        }
        // remove trailing ,
        sb.setLength(sb.length() - 1);
        sb.append("],\n\"query\":{");

        if (internalVersion.onOrAfter(EsMajorVersion.V_2_X)) {
            sb.append("\"bool\": { \"must\":[");
        }
        else {
            sb.append("\"constant_score\":{ \"filter\": { \"and\":[");

        }
        for (String field: fields) {
            sb.append(String.format(Locale.ROOT, "\n{ \"exists\":{ \"field\":\"%s\"} },", field));
        }
        // remove trailing ,
        sb.setLength(sb.length() - 1);
        sb.append("\n]}");

        if (internalVersion.on(EsMajorVersion.V_1_X)) {
            sb.append("}");
        }

        sb.append("}}");

        String endpoint = index;
        if (StringUtils.hasText(type)) {
            endpoint = index + "/" + type;
        }

        Map<String, List<Map<String, Object>>> hits = parseContent(execute(GET, endpoint + "/_search", new BytesArray(sb.toString())).body(), "hits");
        List<Map<String, Object>> docs = hits.get("hits");
        if (docs == null || docs.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, Object> foundFields = docs.get(0);
        Map<String, Object> fieldInfo = (Map<String, Object>) foundFields.get("_source");

        return fieldInfo;
    }

    @Override
    public void close() {
        if (network != null) {
            network.close();
            stats.aggregate(network.stats());
            network = null;
        }
    }

    protected InputStream execute(Request request) {
        return execute(request, true).body();
    }

    protected InputStream execute(Method method, String path) {
        return execute(new SimpleRequest(method, null, path));
    }

    protected Response execute(Method method, String path, boolean checkStatus) {
        return execute(new SimpleRequest(method, null, path), checkStatus);
    }

    protected InputStream execute(Method method, String path, String params) {
        return execute(new SimpleRequest(method, null, path, params));
    }

    protected Response execute(Method method, String path, String params, boolean checkStatus) {
        return execute(new SimpleRequest(method, null, path, params), checkStatus);
    }

    protected Response execute(Method method, String path, ByteSequence buffer) {
        return execute(new SimpleRequest(method, null, path, null, buffer), true);
    }

    protected Response execute(Method method, String path, ByteSequence buffer, boolean checkStatus) {
        return execute(new SimpleRequest(method, null, path, null, buffer), checkStatus);
    }

    protected Response execute(Method method, String path, String params, ByteSequence buffer) {
        return execute(new SimpleRequest(method, null, path, params, buffer), true);
    }

    protected Response execute(Method method, String path, String params, ByteSequence buffer, boolean checkStatus) {
        return execute(new SimpleRequest(method, null, path, params, buffer), checkStatus);
    }

    protected Response execute(Request request, boolean checkStatus) {
        Response response = network.execute(request);
        if (checkStatus) {
            checkResponse(request, response);
        }
        return response;
    }

    protected Response executeNotFoundAllowed(Request req) {
        Response res = execute(req, false);
        switch (res.status()) {
        case HttpStatus.OK:
            break;
        case HttpStatus.NOT_FOUND:
            break;
        default:
            checkResponse(req, res);
        }

        return res;
    }

    private void checkResponse(Request request, Response response) {
        if (response.hasFailed()) {
            // check error first
            String msg = null;
            // try to parse the answer
            try {
                msg = errorExtractor.extractError(this.<Map> parseContent(response.body(), null));
                if (response.isClientError()) {
                    msg = msg + "\n" + request.body();
                }
                else {
                    msg = errorExtractor.prettify(msg, request.body());
                }
            } catch (Exception ex) {
                // can't parse message, move on
            }

            if (!StringUtils.hasText(msg)) {
                msg = String.format("[%s] on [%s] failed; server[%s] returned [%s|%s:%s]", request.method().name(),
                        request.path(), response.uri(), response.status(), response.statusDescription(),
                        IOUtils.asStringAlways(response.body()));
            }

            throw new EsHadoopInvalidRequest(msg);
        }
    }

    public InputStream scroll(String scrollId) {
        // NB: dynamically get the stats since the transport can change
        long start = network.transportStats().netTotalTime;
        try {
            BytesArray body;
            if (internalVersion.onOrAfter(EsMajorVersion.V_2_X)) {
                body = new BytesArray("{\"scroll_id\":\"" + scrollId + "\"}");
            } else {
                body = new BytesArray(scrollId);
            }
            // use post instead of get to avoid some weird encoding issues (caused by the long URL)
            InputStream is = execute(POST, "_search/scroll?scroll=" + scrollKeepAlive.toString(), body).body();
            stats.scrollTotal++;
            return is;
        } finally {
            stats.scrollTotalTime += network.transportStats().netTotalTime - start;
        }
    }

    public boolean delete(String indexOrType) {
        Request req = new SimpleRequest(DELETE, null, indexOrType);
        Response res = executeNotFoundAllowed(req);
        return (res.status() == HttpStatus.OK ? true : false);
    }
    public boolean deleteScroll(String scrollId) {
        BytesArray body;
        if (internalVersion.onOrAfter(EsMajorVersion.V_2_X)) {
            body = new BytesArray(("{\"scroll_id\":[\"" + scrollId + "\"]}").getBytes(StringUtils.UTF_8));
        } else {
            body = new BytesArray(scrollId.getBytes(StringUtils.UTF_8));
        }
        Request req = new SimpleRequest(DELETE, null, "_search/scroll", body);
        Response res = executeNotFoundAllowed(req);
        return (res.status() == HttpStatus.OK ? true : false);
    }

    public boolean documentExists(String index, String type, String id) {
        return exists(index + "/" + type + "/" + id);
    }

    public boolean typeExists(String index, String type) {
        String indexType;
        if (internalVersion.onOrAfter(EsMajorVersion.V_5_X)) {
            indexType = index + "/_mapping/" + type;
        } else {
            indexType = index + "/" + type;
        }
        return exists(indexType);
    }

    public boolean indexExists(String index) {
        return exists(index);
    }

    private boolean exists(String indexOrType) {
        Request req = new SimpleRequest(HEAD, null, indexOrType);
        Response res = executeNotFoundAllowed(req);

        return (res.status() == HttpStatus.OK ? true : false);
    }

    public boolean touch(String index) {
        if (!indexExists(index)) {
            Response response = execute(PUT, index, false);

            if (response.hasFailed()) {
                String msg = null;
                // try to parse the answer
                try {
                    msg = parseContent(response.body(), "error");
                } catch (Exception ex) {
                    // can't parse message, move on
                }

                if (StringUtils.hasText(msg) && !msg.contains("IndexAlreadyExistsException")) {
                    throw new EsHadoopIllegalStateException(msg);
                }
            }
            return response.hasSucceeded();
        }
        return false;
    }

    public long count(String indexAndType, QueryBuilder query) {
        return count(indexAndType, null, query);
    }

    public long count(String indexAndType, String shardId, QueryBuilder query) {
        return internalVersion.onOrAfter(EsMajorVersion.V_5_X) ?
                countInES5X(indexAndType, shardId, query) : countBeforeES5X(indexAndType, shardId, query);
    }

    private long countBeforeES5X(String indexAndType, String shardId, QueryBuilder query) {
        StringBuilder uri = new StringBuilder(indexAndType);
        uri.append("/_count");
        if (StringUtils.hasLength(shardId)) {
            uri.append("?preference=_shards:");
            uri.append(shardId);
        }
        Response response = execute(GET, uri.toString(), searchRequest(query));
        Number count = (Number) parseContent(response.body(), "count");
        return (count != null ? count.longValue() : -1);
    }

    private long countInES5X(String indexAndType, String shardId, QueryBuilder query) {
        StringBuilder uri = new StringBuilder(indexAndType);
        uri.append("/_search?size=0");
        if (StringUtils.hasLength(shardId)) {
            uri.append("&preference=_shards:");
            uri.append(shardId);
        }
        Response response = execute(GET, uri.toString(), searchRequest(query));
        Map<String, Object> content = parseContent(response.body(), "hits");
        Number count = (Number) content.get("total");
        return (count != null ? count.longValue() : -1);
    }

    static BytesArray searchRequest(QueryBuilder query) {
        FastByteArrayOutputStream out = new FastByteArrayOutputStream(256);
        JacksonJsonGenerator generator = new JacksonJsonGenerator(out);
        try {
            generator.writeBeginObject();
            generator.writeFieldName("query");
            generator.writeBeginObject();
            query.toJson(generator);
            generator.writeEndObject();
            generator.writeEndObject();
        } finally {
            generator.close();
        }
        return out.bytes();
    }

    public boolean isAlias(String query) {
        Map<String, Object> aliases = (Map<String, Object>) get(query, null);
        return (aliases.size() > 1);
    }

    public void putMapping(String index, String mapping, byte[] bytes) {
        // create index first (if needed) - it might return 403/404
        touch(index);

        execute(PUT, mapping, new BytesArray(bytes));
    }

    public EsMajorVersion remoteEsVersion() {
        Map<String, String> result = get("", "version");
        if (result == null || !StringUtils.hasText(result.get("number"))) {
            throw new EsHadoopIllegalStateException("Unable to retrieve elasticsearch version.");
        }
        return EsMajorVersion.parse(result.get("number"));
    }

    public Health getHealth(String index) {
        StringBuilder sb = new StringBuilder("/_cluster/health/");
        sb.append(index);
        String status = get(sb.toString(), "status");
        if (status == null) {
            throw new EsHadoopIllegalStateException("Could not determine index health, returned status was null. Bailing out...");
        }
        return Health.valueOf(status.toUpperCase());
    }

    public boolean waitForHealth(String index, Health health, TimeValue timeout) {
        StringBuilder sb = new StringBuilder("/_cluster/health/");
        sb.append(index);
        sb.append("?wait_for_status=");
        sb.append(health.name().toLowerCase(Locale.ROOT));
        sb.append("&timeout=");
        sb.append(timeout.toString());

        return (Boolean.TRUE.equals(get(sb.toString(), "timed_out")));
    }

    @Override
    public Stats stats() {
        Stats copy = new Stats(stats);
        if (network != null) {
            copy.aggregate(network.stats());
        }
        return copy;
    }

    private void countStreamStats(InputStream content) {
        if (content instanceof StatsAware) {
            stats.aggregate(((StatsAware) content).stats());
        }
    }

    public String getCurrentNode() {
        return network.currentNode();
    }
}