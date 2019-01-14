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

import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.query.MatchAllQueryBuilder;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.FastByteArrayInputStream;
import org.elasticsearch.hadoop.util.TestSettings;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class RestClientTest {

    @Test
    public void testPostDocumentSuccess() throws Exception {
        String index = "index/type";
        Settings settings = new TestSettings();
        settings.setResourceWrite(index);
        Resource writeResource = new Resource(settings, false);
        BytesArray document = new BytesArray("{\"field\":\"value\"}");
        SimpleRequest request = new SimpleRequest(Request.Method.POST, null, index, null, document);
        String response =
                "{\n" +
                "  \"_index\": \"index\",\n" +
                "  \"_type\": \"type\",\n" +
                "  \"_id\": \"AbcDefGhiJklMnoPqrS_\",\n" +
                "  \"_version\": 1,\n" +
                "  \"result\": \"created\",\n" +
                "  \"_shards\": {\n" +
                "    \"total\": 2,\n" +
                "    \"successful\": 1,\n" +
                "    \"failed\": 0\n" +
                "  },\n" +
                "  \"_seq_no\": 0,\n" +
                "  \"_primary_term\": 1\n" +
                "}";

        NetworkClient mock = Mockito.mock(NetworkClient.class);
        Mockito.when(mock.execute(Mockito.eq(request))).thenReturn(new SimpleResponse(201, new FastByteArrayInputStream(new BytesArray(response)), "localhost:9200"));

        RestClient client = new RestClient(new TestSettings(), mock);

        String id = client.postDocument(writeResource, document);

        assertEquals("AbcDefGhiJklMnoPqrS_", id);
    }

    @Test(expected = EsHadoopInvalidRequest.class)
    public void testPostDocumentFailure() throws Exception {
        String index = "index/type";
        Settings settings = new TestSettings();
        settings.setResourceWrite(index);
        Resource writeResource = new Resource(settings, false);
        BytesArray document = new BytesArray("{\"field\":\"value\"}");
        SimpleRequest request = new SimpleRequest(Request.Method.POST, null, index, null, document);
        String response =
                "{\n" +
                "  \"error\": {\n" +
                "    \"root_cause\": [\n" +
                "      {\n" +
                "        \"type\": \"io_exception\",\n" +
                "        \"reason\": \"test failure\"\n" +
                "      }\n" +
                "    ],\n" +
                "    \"type\": \"io_exception\",\n" +
                "    \"reason\": \"test failure\",\n" +
                "    \"caused_by\": {\n" +
                "      \"type\": \"io_exception\",\n" +
                "      \"reason\": \"This test needs to fail\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"status\": 400\n" +
                "}";

        NetworkClient mock = Mockito.mock(NetworkClient.class);
        Mockito.when(mock.execute(Mockito.eq(request))).thenReturn(new SimpleResponse(400, new FastByteArrayInputStream(new BytesArray(response)), "localhost:9200"));

        RestClient client = new RestClient(new TestSettings(), mock);

        client.postDocument(writeResource, document);

        fail("Request should have failed");
    }

    @Test(expected = EsHadoopInvalidRequest.class)
    public void testPostDocumentWeirdness() throws Exception {
        String index = "index/type";
        Settings settings = new TestSettings();
        settings.setResourceWrite(index);
        Resource writeResource = new Resource(settings, false);
        BytesArray document = new BytesArray("{\"field\":\"value\"}");
        SimpleRequest request = new SimpleRequest(Request.Method.POST, null, index, null, document);
        String response =
                "{\n" +
                "  \"_index\": \"index\",\n" +
                "  \"_type\": \"type\",\n" +
                "  \"definitely_not_an_id\": \"AbcDefGhiJklMnoPqrS_\",\n" + // Make the ID go away
                "  \"_version\": 1,\n" +
                "  \"result\": \"created\",\n" +
                "  \"_shards\": {\n" +
                "    \"total\": 2,\n" +
                "    \"successful\": 1,\n" +
                "    \"failed\": 0\n" +
                "  },\n" +
                "  \"_seq_no\": 0,\n" +
                "  \"_primary_term\": 1\n" +
                "}";

        NetworkClient mock = Mockito.mock(NetworkClient.class);
        Mockito.when(mock.execute(Mockito.eq(request))).thenReturn(new SimpleResponse(201, new FastByteArrayInputStream(new BytesArray(response)), "localhost:9200"));

        RestClient client = new RestClient(new TestSettings(), mock);

        String id = client.postDocument(writeResource, document);

        assertEquals("AbcDefGhiJklMnoPqrS_", id);
    }

    @Test
    public void testCount5x() throws Exception {
        String index = "index/type";

        BytesArray query = new BytesArray("{\"query\":{\"match_all\":{}}}");
        SimpleRequest request = new SimpleRequest(Request.Method.GET, null, index + "/_search?size=0&track_total_hits=true", null, query);
        String response =
                "{\n" +
                "    \"took\": 6,\n" +
                "    \"timed_out\": false,\n" +
                "    \"_shards\": {\n" +
                "        \"total\": 1,\n" +
                "        \"successful\": 1,\n" +
                "        \"skipped\": 0,\n" +
                "        \"failed\": 0\n" +
                "    },\n" +
                "    \"hits\": {\n" +
                "        \"total\": 5,\n" +
                "        \"max_score\": null,\n" +
                "        \"hits\": []\n" +
                "    }\n" +
                "}";

        NetworkClient mock = Mockito.mock(NetworkClient.class);
        Mockito.when(mock.execute(Mockito.eq(request))).thenReturn(new SimpleResponse(201, new FastByteArrayInputStream(new BytesArray(response)), "localhost:9200"));

        RestClient client = new RestClient(new TestSettings(), mock);

        long count = client.count(index, MatchAllQueryBuilder.MATCH_ALL);

        assertEquals(5L, count);
    }

    @Test
    public void testCount7x() throws Exception {
        String index = "index/type";

        BytesArray query = new BytesArray("{\"query\":{\"match_all\":{}}}");
        SimpleRequest request = new SimpleRequest(Request.Method.GET, null, index + "/_search?size=0&track_total_hits=true", null, query);
        String response =
                "{\n" +
                        "    \"took\": 6,\n" +
                        "    \"timed_out\": false,\n" +
                        "    \"_shards\": {\n" +
                        "        \"total\": 1,\n" +
                        "        \"successful\": 1,\n" +
                        "        \"skipped\": 0,\n" +
                        "        \"failed\": 0\n" +
                        "    },\n" +
                        "    \"hits\": {\n" +
                        "        \"total\": {\n" +
                        "            \"value\": 5,\n" +
                        "            \"relation\": \"eq\"\n" +
                        "        },\n" +
                        "        \"max_score\": null,\n" +
                        "        \"hits\": []\n" +
                        "    }\n" +
                        "}";

        NetworkClient mock = Mockito.mock(NetworkClient.class);
        Mockito.when(mock.execute(Mockito.eq(request))).thenReturn(new SimpleResponse(201, new FastByteArrayInputStream(new BytesArray(response)), "localhost:9200"));

        RestClient client = new RestClient(new TestSettings(), mock);

        long count = client.count(index, MatchAllQueryBuilder.MATCH_ALL);

        assertEquals(5L, count);
    }

    @Test(expected = EsHadoopParsingException.class)
    public void testCount7xBadRelation() throws Exception {
        String index = "index/type";

        BytesArray query = new BytesArray("{\"query\":{\"match_all\":{}}}");
        SimpleRequest request = new SimpleRequest(Request.Method.GET, null, index + "/_search?size=0&track_total_hits=true", null, query);
        String response =
                "{\n" +
                        "    \"took\": 6,\n" +
                        "    \"timed_out\": false,\n" +
                        "    \"_shards\": {\n" +
                        "        \"total\": 1,\n" +
                        "        \"successful\": 1,\n" +
                        "        \"skipped\": 0,\n" +
                        "        \"failed\": 0\n" +
                        "    },\n" +
                        "    \"hits\": {\n" +
                        "        \"total\": {\n" +
                        "            \"value\": 5,\n" +
                        "            \"relation\": \"gte\"\n" +
                        "        },\n" +
                        "        \"max_score\": null,\n" +
                        "        \"hits\": []\n" +
                        "    }\n" +
                        "}";

        NetworkClient mock = Mockito.mock(NetworkClient.class);
        Mockito.when(mock.execute(Mockito.eq(request))).thenReturn(new SimpleResponse(201, new FastByteArrayInputStream(new BytesArray(response)), "localhost:9200"));

        RestClient client = new RestClient(new TestSettings(), mock);

        long count = client.count(index, MatchAllQueryBuilder.MATCH_ALL);

        assertEquals(5L, count);
    }
}