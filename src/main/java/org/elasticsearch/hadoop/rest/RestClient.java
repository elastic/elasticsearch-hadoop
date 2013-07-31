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
import java.util.Map.Entry;

import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.HttpMethodBase;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.SimpleHttpConnectionManager;
import org.apache.commons.httpclient.URI;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.HeadMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.params.HttpClientParams;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.dto.Node;
import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.hadoop.util.unit.TimeValue;

/**
 * REST client used for interacting with ElasticSearch. Performs basic operations; for buffer/batching operation consider using BufferedRestClient.
 */
public class RestClient implements Closeable {

    private static final Log log = LogFactory.getLog(RestClient.class);

    private HttpClient client;
    private ObjectMapper mapper = new ObjectMapper();
    private TimeValue scrollKeepAlive;

    public RestClient(Settings settings) {
        HttpClientParams params = new HttpClientParams();
        params.setConnectionManagerTimeout(settings.getHttpTimeout());

        client = new HttpClient(params);
        HostConfiguration hostConfig = new HostConfiguration();
        String targetUri = settings.getTargetUri();
        try {
            hostConfig.setHost(new URI(targetUri, false));
        } catch (IOException ex) {
            throw new IllegalArgumentException("Invalid target URI " + targetUri, ex);
        }
        client.setHostConfiguration(hostConfig);

        scrollKeepAlive = TimeValue.timeValueMillis(settings.getScrollKeepAlive());
    }

    @SuppressWarnings("unchecked")
    private <T> T get(String q, String string) throws IOException {
        byte[] content = execute(new GetMethod(q));
        Map<String, Object> map = mapper.readValue(content, Map.class);
        return (T) (string != null ? map.get(string) : map);
    }

    public void bulk(String index, byte[] buffer, int bufferSize) {
        PostMethod post = new PostMethod(index + "/_bulk");
        post.setRequestEntity(new JsonByteArrayRequestEntity(buffer, bufferSize));
        post.setContentChunked(false);
        execute(post);
    }

    public void refresh(String index) {
        int slash = index.indexOf("/");
        String indx = (slash < 0) ? index : index.substring(0, slash);
        execute(new PostMethod(indx + "/_refresh"));
    }

    private void create(String q, byte[] value) {
        PostMethod post = new PostMethod(q);
        post.setRequestEntity(new ByteArrayRequestEntity(value));
        execute(post);
    }

    public void deleteIndex(String index) {
        execute(new DeleteMethod(index));
    }

    public List<List<Map<String, Object>>> targetShards(String query) throws IOException {
        List<List<Map<String, Object>>> shardsJson = get(query, "shards");
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

    public Map<String, Object> getMapping(String query) throws IOException {
        return (Map<String, Object>) get(query, null);
    }

    @Override
    public void close() {
        HttpConnectionManager manager = client.getHttpConnectionManager();
        if (manager instanceof SimpleHttpConnectionManager) {
            try {
                ((SimpleHttpConnectionManager) manager).closeIdleConnections(0);
            } catch (NullPointerException npe) {
                // ignore
            } catch (Exception ex) {
                // log - not much else to do
                log.warn("Exception closing underlying HTTP manager", ex);
            }
        }
    }

    byte[] execute(HttpMethodBase method) {
        return execute(method, true);
    }

    byte[] execute(HttpMethodBase method, boolean checkStatus) {
        try {
            int status = client.executeMethod(method);
            if (checkStatus && status >= HttpStatus.SC_MULTI_STATUS) {
                String body;
                try {
                    body = method.getResponseBodyAsString();
                } catch (IOException ex) {
                    body = "";
                }
                throw new IllegalStateException(String.format("[%s] on [%s] failed; server[%s] returned [%s]",
                        method.getName(), method.getURI(), client.getHostConfiguration().getHostURL(), body));
            }
            return method.getResponseBody();
        } catch (IOException io) {
            String target;
            try {
                target = method.getURI().toString();
            } catch (IOException ex) {
                target = method.getPath();
            }
            throw new IllegalStateException(String.format("Cannot get response body for [%s][%s]", method.getName(), target));
        } finally {
            method.releaseConnection();
        }
    }

    public String[] scan(String query) throws IOException {
        Map<String, Object> scan = get(query, null);
        String[] data = new String[2];
        data[0] = scan.get("_scroll_id").toString();
        data[1] = ((Map) scan.get("hits")).get("total").toString();
        return data;
    }

    public byte[] scroll(String scrollId) throws IOException {
        // use post instead of get to avoid some weird encoding issues (caused by the long URL)
        PostMethod post = new PostMethod("_search/scroll?scroll=" + scrollKeepAlive.toString());
        post.setRequestEntity(new ByteArrayRequestEntity(scrollId.getBytes(StringUtils.UTF_8)));
        return execute(post);
    }

    public boolean exists(String indexOrType) {
        HeadMethod headMethod = new HeadMethod(indexOrType);
        execute(headMethod, false);
        return (headMethod.getStatusCode() == HttpStatus.SC_OK);
    }

    public void putMapping(String mapping, byte[] bytes) {
        PutMethod put = new PutMethod(mapping);
        put.setRequestEntity(new ByteArrayRequestEntity(bytes));
        execute(put);
    }
}