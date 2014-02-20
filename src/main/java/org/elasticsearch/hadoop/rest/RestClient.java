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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.Request.Method;
import org.elasticsearch.hadoop.rest.dto.Node;
import org.elasticsearch.hadoop.rest.stats.Stats;
import org.elasticsearch.hadoop.rest.stats.StatsAware;
import org.elasticsearch.hadoop.serialization.ParsingUtils;
import org.elasticsearch.hadoop.serialization.json.JacksonJsonParser;
import org.elasticsearch.hadoop.util.ByteSequence;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.ObjectUtils;
import org.elasticsearch.hadoop.util.SettingsUtils;
import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.hadoop.util.TrackingBytesArray;
import org.elasticsearch.hadoop.util.unit.TimeValue;

import static org.elasticsearch.hadoop.rest.Request.Method.*;

public class RestClient implements Closeable, StatsAware {

    private NetworkClient network;
    private ObjectMapper mapper = new ObjectMapper();
    private TimeValue scrollKeepAlive;
    private boolean indexReadMissingAsEmpty;
    private final HttpRetryPolicy retryPolicy;

    private final Stats stats = new Stats();

    public enum HEALTH {
        RED, YELLOW, GREEN
    }

    public RestClient(Settings settings) {
        network = new NetworkClient(settings, SettingsUtils.nodes(settings));

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
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public List<String> discoverNodes() throws IOException {
        String endpoint = "_nodes/transport";
        Map<String, Map> nodes = (Map<String, Map>) get(endpoint, "nodes");

        List<String> hosts = new ArrayList<String>(nodes.size());

        for (Map value : nodes.values()) {
            String inet = (String) value.get("http_address");
            if (StringUtils.hasText(inet)) {
                int startIp = inet.indexOf("/") + 1;
                int endIp = inet.indexOf("]");
                inet = inet.substring(startIp, endIp);
                hosts.add(inet);
            }
        }

        return hosts;
    }

    private <T> T get(String q, String string) throws IOException {
        return parseContent(execute(GET, q), string);
    }

    @SuppressWarnings("unchecked")
    private <T> T parseContent(InputStream content, String string) throws IOException {
        // create parser manually to lower Jackson requirements
        JsonParser jsonParser = mapper.getJsonFactory().createJsonParser(content);
        Map<String, Object> map = mapper.readValue(jsonParser, Map.class);
        return (T) (string != null ? map.get(string) : map);
    }

    public void bulk(Resource resource, TrackingBytesArray data) throws IOException {
        Retry retry = retryPolicy.init();
        int httpStatus = 0;

        boolean isRetry = false;

        do {
            Response response = execute(PUT, resource.bulk(), data);

            stats.bulkWrites++;
            stats.docsWritten += data.entries();
            // bytes will be counted by the transport layer

            if (isRetry) {
                stats.docsRetried += data.entries();
                stats.bytesRetried += data.length();
                stats.bulkRetries++;
            }

            isRetry = true;

            httpStatus = (retryFailedEntries(response.body(), data) ? HttpStatus.SERVICE_UNAVAILABLE : HttpStatus.OK);
        } while (data.length() > 0 && retry.retry(httpStatus));
    }

    @SuppressWarnings("rawtypes")
    private boolean retryFailedEntries(InputStream content, TrackingBytesArray data) throws IOException {
        ObjectReader r = mapper.reader(Map.class);
        JsonParser parser = mapper.getJsonFactory().createJsonParser(content);
        if (ParsingUtils.seek("items", new JacksonJsonParser(parser)) == null) {
            return false;
        }

        int entryToDeletePosition = 0; // head of the list
        for (Iterator<Map> iterator = r.readValues(parser); iterator.hasNext();) {
            Map map = iterator.next();
            Map values = (Map) map.values().iterator().next();
            String error = (String) values.get("error");
            if (error != null) {
                // status - introduced in 1.0.RC1
                Integer status = (Integer) values.get("status");
                if (status != null && HttpStatus.canRetry(status) || error.contains("EsRejectedExecutionException")) {
                    entryToDeletePosition++;
                }
                else {
                    String message = (status != null ? String.format("%s(%s) - %s", HttpStatus.getText(status), status, error) : error);
                    throw new IllegalStateException(String.format("Found unrecoverable error [%s]; Bailing out..", message));
                }
            }
            else {
                data.remove(entryToDeletePosition);
            }
        }

        return entryToDeletePosition > 0;
    }

    public void refresh(Resource resource) throws IOException {
        execute(POST, resource.refresh());
    }

    public void deleteIndex(String index) throws IOException {
        execute(DELETE, index);
    }

    public List<List<Map<String, Object>>> targetShards(Resource resource) throws IOException {
        List<List<Map<String, Object>>> shardsJson = null;

        if (indexReadMissingAsEmpty) {
            Response res = execute(GET, resource.targetShards(), false);
            if (res.status() == HttpStatus.NOT_FOUND) {
                shardsJson = Collections.emptyList();
            }
            else {
                shardsJson = parseContent(res.body(), "shards");
            }
        }
        else {
            shardsJson = get(resource.targetShards(), "shards");
        }

        return shardsJson;
    }

    public Map<String, Node> getNodes() throws IOException {
        Map<String, Map<String, Object>> nodesData = get("_nodes/http", "nodes");
        Map<String, Node> nodes = new LinkedHashMap<String, Node>();

        for (Entry<String, Map<String, Object>> entry : nodesData.entrySet()) {
            Node node = new Node(entry.getKey(), entry.getValue());
            nodes.put(entry.getKey(), node);
        }
        return nodes;
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getMapping(String query) throws IOException {
        return (Map<String, Object>) get(query, null);
    }

    @Override
    public void close() {
        network.close();
    }

    protected InputStream execute(Request request) throws IOException {
        return execute(request, true).body();
    }

    protected InputStream execute(Method method, String path) throws IOException {
        return execute(new SimpleRequest(method, null, path));
    }

    protected Response execute(Method method, String path, boolean checkStatus) throws IOException {
        return execute(new SimpleRequest(method, null, path), checkStatus);
    }

    protected Response execute(Method method, String path, ByteSequence buffer) throws IOException {
        return execute(new SimpleRequest(method, null, path, null, buffer), true);
    }

    protected Response execute(Request request, boolean checkStatus) throws IOException {
        Response response = network.execute(request);

        if (checkStatus && response.hasFailed()) {
            // check error first
            String msg = null;
            try {
                // try to parse the answer
                msg = parseContent(response.body(), "error");
            } catch (Exception ex) {
                // ignore
            }
            if (!StringUtils.hasText(msg)) {
                msg = String.format("[%s] on [%s] failed; server[%s] returned [%s:%s]", request.method().name(),
                        request.path(), response.uri(), response.status(), response.statusDescription());
            }

            throw new IllegalStateException(msg);
        }

        return response;
    }

    public String[] scan(String query, BytesArray body) throws IOException {
        Map<String, Object> scan = parseContent(execute(POST, query, body).body(), null);

        String[] data = new String[2];
        data[0] = scan.get("_scroll_id").toString();
        data[1] = ((Map<?, ?>) scan.get("hits")).get("total").toString();
        return data;
    }

    public InputStream scroll(String scrollId) throws IOException {
        // use post instead of get to avoid some weird encoding issues (caused by the long URL)
        return execute(POST, "_search/scroll?scroll=" + scrollKeepAlive.toString(),
                new BytesArray(scrollId.getBytes(StringUtils.UTF_8))).body();
    }

    public boolean exists(String indexOrType) throws IOException {
        return (execute(HEAD, indexOrType, false).hasSucceeded());
    }

    public boolean touch(String indexOrType) throws IOException {
        return (execute(PUT, indexOrType, false).hasSucceeded());
    }

    public void putMapping(String index, String mapping, byte[] bytes) throws IOException {
        // create index first (if needed) - it might return 403
        touch(index);

        execute(PUT, mapping, new BytesArray(bytes));
    }

    public String esVersion() throws IOException {
        Map<String, String> version = get("", "version");
        return version.get("number");
    }

    public boolean health(String index, HEALTH health, TimeValue timeout) throws IOException {
        StringBuilder sb = new StringBuilder("/_cluster/health/");
        sb.append(index);
        sb.append("?wait_for_status=");
        sb.append(health.name().toLowerCase(Locale.ENGLISH));
        sb.append("&timeout=");
        sb.append(timeout.toString());

        return (Boolean.TRUE.equals(get(sb.toString(), "timed_out")));
    }

    @Override
    public Stats stats() {
        return stats.aggregate(network.stats());
    }
}