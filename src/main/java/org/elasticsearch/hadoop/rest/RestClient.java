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
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.HttpMethodBase;
import org.apache.commons.httpclient.SimpleHttpConnectionManager;
import org.apache.commons.httpclient.URI;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.params.HttpClientParams;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.hadoop.cfg.Settings;

/**
 * REST client used for interacting with ElasticSearch. Performs basic operations; for buffer/batching operation consider using BufferedRestClient.
 */
public class RestClient implements Closeable {

    private static final Log log = LogFactory.getLog(RestClient.class);

    private HttpClient client;
    private ObjectMapper mapper = new ObjectMapper();

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
    }

    /**
     * Queries ElasticSearch using pagination.
     *
     * @param uri query to execute
     * @param from where to start
     * @param size what size to request
     * @return
     */
    public List<Map<String, Object>> query(String uri, long from, int size) throws IOException {
        String q = URIUtils.addParam(uri, "from=" + from, "size=" + size);

        Map hits = (Map) get(q, "hits");
        Object h = hits.get("hits");
        return (List<Map<String, Object>>) h;
    }

    private Object get(String q, String string) throws IOException {
        byte[] content = execute(new GetMethod(q));
        Map<String, Object> map = mapper.readValue(content, Map.class);
        return map.get(string);
    }

    public void bulk(String index, byte[] buffer, int bufferSize) throws IOException {
        PostMethod post = new PostMethod(index + "/_bulk");
        post.setRequestEntity(new JsonByteArrayRequestEntity(buffer, bufferSize));
        post.setContentChunked(false);
        execute(post);
    }

    public void refresh(String index) throws IOException {
        String indx = index.substring(0, index.indexOf("/"));
        execute(new PostMethod(indx + "/_refresh"));
    }

    private void create(String q, byte[] value) throws IOException {
        PostMethod post = new PostMethod(q);
        post.setRequestEntity(new ByteArrayRequestEntity(value));
        execute(post);
    }

    public void deleteIndex(String index) throws IOException {
        execute(new DeleteMethod(index));
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

    private byte[] execute(HttpMethodBase method) throws IOException {
        try {
            int status = client.executeMethod(method);
            if (status >= 300) {
                String body;
                try {
                    body = IOUtils.toString(method.getResponseBodyAsStream());
                } catch (IOException ex) {
                    body = "";
                }

                throw new IOException(String.format("[%s] on [%s] failed; server returned [%s]", method.getName(), method.getURI(), body));
            }
            return method.getResponseBody();
        } finally {
            method.releaseConnection();
        }
    }
}