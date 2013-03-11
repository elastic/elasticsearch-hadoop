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

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.map.ObjectMapper;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

/**
 * Jersey-based REST client used for interacting with ElasticSearch.
 */
public class JerseyClient implements Closeable {

    private static final Log log = LogFactory.getLog(JerseyClient.class);

    private Client client;
    private final String target;
    private ObjectMapper mapper = new ObjectMapper();

    public JerseyClient(String target) {
        client = Client.create();
        this.target = target;
    }

    /**
     * Asks for the number of results for the given query.
     * Used to discover the number of splits required.
     *
     * @param query query to execute
     * @return number of hits
     */
    public long numberOfResults(String uri) throws IOException {
        String q = URIUtils.addPath(uri, "_count");
        return Long.valueOf((Integer) get(q, "count"));
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
        // don't use WebResource#path as it encodes the URI
        WebResource res = client.resource(target + q);
        //log.warn("Executing GET query " + res.toString());
        ClientResponse response = res.get(ClientResponse.class);
        if (response.getStatus() >= 300) {
            throw new IOException("Error processing query " + q + "; server returned "
                    + IOUtils.toString(response.getEntityInputStream()));
        }
        Map<String, Object> map = mapper.readValue(response.getEntityInputStream(), Map.class);
        return map.get(string);
    }

    public void addToIndex(String index, List<Object> values) throws IOException {
        // TODO: add bulk support
        for (Object object : values) {
            String valueAsString = mapper.writeValueAsString(object);
            create(index, valueAsString);
        }
    }

    private void create(String q, Object value) throws IOException {
        WebResource res = client.resource(target).path(q);
        //log.warn("Executing PUT query " + res.toString() + "value=" + value);
        ClientResponse response = res.post(ClientResponse.class, value);
        if (response.getStatus() >= 300) {
            throw new IOException("Error processing query " + q + "; server returned "
                    + IOUtils.toString(response.getEntityInputStream()));
        }
    }

    public void deleteIndex(String index) {
        WebResource res = client.resource(target).path(index);
        res.delete();
    }

    @Override
    public void close() throws IOException {
        client.destroy();
    }
}
