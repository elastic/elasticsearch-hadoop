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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.HeadMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.dto.Node;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.hadoop.util.unit.TimeValue;

/**
 * REST client used for interacting with ElasticSearch. Performs basic operations; for buffer/batching operation consider using BufferedRestClient.
 */
public class RestClient implements Closeable {

    private static final Log log = LogFactory.getLog(RestClient.class);

    private NetworkClient network;
    private ObjectMapper mapper = new ObjectMapper();
    private TimeValue scrollKeepAlive;
    private boolean indexReadMissingAsEmpty;

    public enum HEALTH {
        RED, YELLOW, GREEN
    }

    public RestClient(Settings settings) {
        // TODO: extract nodes
        network = new NetworkClient(settings, Collections.<String> emptyList());

        scrollKeepAlive = TimeValue.timeValueMillis(settings.getScrollKeepAlive());
        indexReadMissingAsEmpty = settings.getIndexReadMissingAsEmpty();
    }

    private <T> T get(String q, String string) throws IOException {
        return parseContent(execute(GET, new GetMethod(q)), string);
    }

    @SuppressWarnings("unchecked")
    private <T> T parseContent(byte[] content, String string) throws IOException {
        // create parser manually to lower Jackson requirements
        JsonParser jsonParser = mapper.getJsonFactory().createJsonParser(content);
        Map<String, Object> map = mapper.readValue(jsonParser, Map.class);
        return (T) (string != null ? map.get(string) : map);
    }

    @SuppressWarnings("unchecked")
    public void bulk(Resource resource, BytesArray buffer) throws IOException {
        //empty buffer, ignore
        if (buffer.size() == 0) {
            return;
        }

        PostMethod post = new PostMethod(resource.bulk());
        post.setRequestEntity(new BytesArrayRequestEntity(buffer));
        post.setContentChunked(false);

        if (log.isTraceEnabled()) {
            log.trace("Sending bulk request " + buffer.toString());
        }

        byte[] content = execute(post);
        // create parser manually to lower Jackson requirements
        JsonParser jsonParser = mapper.getJsonFactory().createJsonParser(content);
        Map<String, Object> map = mapper.readValue(jsonParser, Map.class);
        List<Object> items = (List<Object>) map.get("items");

        for (Object item : items) {
            Map<String, String> messages = (Map<String, String>) ((Map) item).values().iterator().next();
            String message = messages.get("error");
            if (StringUtils.hasText(message)) {
                throw new IllegalStateException(String.format(
                        "Bulk request on index [%s] failed; at least one error reported [%s]", resource.indexAndType(), message));
            }
        }

        if (log.isTraceEnabled()) {
            log.trace("Received bulk response " + new String(content));
        }
    }

    public void refresh(Resource resource) {
        execute(new PostMethod(resource.refresh()));
    }

    public void deleteIndex(String index) {
        execute(new DeleteMethod(index));
    }

    public List<List<Map<String, Object>>> targetShards(Resource resource) throws IOException {
        List<List<Map<String, Object>>> shardsJson = null;

        if (indexReadMissingAsEmpty) {
            GetMethod get = new GetMethod(resource.targetShards());
            byte[] content = execute(get, false);
            if (get.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                shardsJson = Collections.emptyList();
            }
            else {
                shardsJson = parseContent(content, "shards");
            }
        }
        else {
            shardsJson = get(resource.targetShards(), "shards");
        }

        return shardsJson;
    }

    public Map<String, Node> getNodes() throws IOException {
        Map<String, Map<String, Object>> nodesData = get("_nodes", "nodes");
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

    byte[] execute(Request request) throws IOException {
        return execute(request, true);
    }

    byte[] execute(Request request, boolean checkStatus) throws IOException {
        Response response = network.execute(request);

        if (checkStatus && response.status() >= HttpStatus.SC_MULTI_STATUS) {
            String bodyAsString = StringUtils.asUTFString(response.body());
            throw new IllegalStateException(String.format("[%s] on [%s] failed; server[%s] returned [%s]",
                    request.method().name(), request.path(), response.uri(), bodyAsString));
        }

        return response.body();
    }

    public String[] scan(String query, BytesArray body) throws IOException {
        PostMethod post = new PostMethod(query);
        if (body != null && body.size() > 0) {
            post.setContentChunked(false);
            post.setRequestEntity(new BytesArrayRequestEntity(body));
        }

        Map<String, Object> scan = parseContent(execute(post), null);

        String[] data = new String[2];
        data[0] = scan.get("_scroll_id").toString();
        data[1] = ((Map<?, ?>) scan.get("hits")).get("total").toString();
        return data;
    }

    public byte[] scroll(String scrollId) throws IOException {
        // use post instead of get to avoid some weird encoding issues (caused by the long URL)
        PostMethod post = new PostMethod("_search/scroll?scroll=" + scrollKeepAlive.toString());
        post.setContentChunked(false);
        post.setRequestEntity(new ByteArrayRequestEntity(scrollId.getBytes(StringUtils.UTF_8)));
        return execute(post);
    }

    public boolean exists(String indexOrType) {
        HeadMethod headMethod = new HeadMethod(indexOrType);
        execute(headMethod, false);
        return (headMethod.getStatusCode() == HttpStatus.SC_OK);
    }

    public boolean touch(String indexOrType) {
        PutMethod method = new PutMethod(indexOrType);
        execute(method, false);
        return (method.getStatusCode() == HttpStatus.SC_OK);
    }

    public void putMapping(String index, String mapping, byte[] bytes) {
        // create index first (if needed) - it might return 403
        touch(index);

        // create actual mapping
        PutMethod put = new PutMethod(mapping);
        put.setRequestEntity(new ByteArrayRequestEntity(bytes));
        execute(put);
    }

    public boolean health(String index, HEALTH health, TimeValue timeout) throws IOException {
        StringBuilder sb = new StringBuilder("/_cluster/health/");
        sb.append(index);
        sb.append("?wait_for_status=");
        sb.append(health.name().toLowerCase());
        sb.append("&timeout=");
        sb.append(timeout.toString());

        return (Boolean.TRUE.equals(get(sb.toString(), "timed_out")));
    }
}