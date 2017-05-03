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
import org.apache.commons.logging.impl.NoOpLog;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.hadoop.cfg.PropertiesSettings;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.serialization.dto.NodeInfo;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_RESOURCE_READ;
import static org.elasticsearch.hadoop.rest.query.MatchAllQueryBuilder.MATCH_ALL;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class FindPartitionsTest {
    private static final ObjectMapper MAPPER =
            new ObjectMapper()
                    .configure(JsonParser.Feature.ALLOW_COMMENTS, true);

    private static final Log LOGGER = new NoOpLog("FindPartitionsTest");

    private static final PartitionDefinition[] EXPECTED_SHARDS_PARTITIONS;
    static {
        List<PartitionDefinition> expected =
                new ArrayList<PartitionDefinition>();
        for (int i = 0; i < 15; i++) {
            expected.add(new PartitionDefinition(null, null, "index1", i));
        }
        for (int i = 0; i < 18; i++) {
            expected.add(new PartitionDefinition(null, null, "index2", i));
        }
        for (int i = 0; i < 1; i++) {
            expected.add(new PartitionDefinition(null, null, "index3", i));
        }
        Collections.sort(expected);
        EXPECTED_SHARDS_PARTITIONS = expected.toArray(new PartitionDefinition[0]);
    }

    @Test
    public void testEmpty() {
        Settings settings = new PropertiesSettings();
        settings.setProperty(ES_RESOURCE_READ, "_all");
        assertEquals(RestService.findShardPartitions(settings, null,
                Collections.<String, NodeInfo>emptyMap(), Collections.<List<Map<String,Object>>>emptyList(), LOGGER).size(), 0);
        assertEquals(RestService.findSlicePartitions(null, settings, null,
                Collections.<String, NodeInfo>emptyMap(), Collections.<List<Map<String,Object>>>emptyList(), LOGGER).size(), 0);
    }

    @Test
    public void testShardPartitions() throws IOException {
        List<List<Map<String, Object>>> shards =
                MAPPER.readValue(getClass().getResourceAsStream("search-shards-response.json"), ArrayList.class);
        List<PartitionDefinition> partitions = RestService.findShardPartitions(null, null, Collections.<String, NodeInfo>emptyMap(),
                shards, LOGGER);
        Collections.sort(partitions);
        assertEquals(partitions.size(), 34);
        assertEquals(new HashSet(partitions).size(), 34);
        assertArrayEquals(partitions.toArray(), EXPECTED_SHARDS_PARTITIONS);
    }

    @Test
    public void testSlicePartitions() throws IOException {
        List<List<Map<String, Object>>> shards =
                MAPPER.readValue(getClass().getResourceAsStream("search-shards-response.json"), ArrayList.class);
        RestClient client = Mockito.mock(RestClient.class);
        Settings settings = new PropertiesSettings();
        settings.setProperty(ES_RESOURCE_READ, "index1,index2,index3/type1");
        for (int i = 0; i < 15; i++) {
            Mockito.when(client.count("index1/type1", Integer.toString(i), MATCH_ALL)).thenReturn(1000L);
        }
        for (int i = 0; i < 18; i++) {
            Mockito.when(client.count("index2/type1", Integer.toString(i), MATCH_ALL)).thenReturn(10000L);
        }
        for (int i = 0; i < 1; i++) {
            Mockito.when(client.count("index3/type1", Integer.toString(i), MATCH_ALL)).thenReturn(100000L);
        }
        {
            settings.setMaxDocsPerPartition(1000);
            List<PartitionDefinition> partitions = RestService.findSlicePartitions(client, settings, null,
                    Collections.<String, NodeInfo>emptyMap(), shards, LOGGER);
            // 15 + 18*10 + 1*100
            assertEquals(partitions.size(), 295);
            assertEquals(new HashSet(partitions).size(), 295);
        }
        {
            settings.setMaxDocsPerPartition(100);
            List<PartitionDefinition> partitions = RestService.findSlicePartitions(client, settings, null,
                    Collections.<String, NodeInfo>emptyMap(), shards, LOGGER);
            // 15*10 + 18*100 + 1*1000
            assertEquals(partitions.size(), 2950);
            assertEquals(new HashSet(partitions).size(), 2950);
        }
        {
            settings.setMaxDocsPerPartition(Integer.MAX_VALUE);
            List<PartitionDefinition> partitions = RestService.findSlicePartitions(client, settings, null,
                    Collections.<String, NodeInfo>emptyMap(), shards, LOGGER);

            // 15 + 18 + 1
            assertEquals(partitions.size(), 34);
            assertEquals(new HashSet(partitions).size(), 34);
        }
        for (int i = 0; i < 15; i++) {
            Mockito.when(client.count("index1/type1", Integer.toString(i), MATCH_ALL)).thenReturn(0L);
        }
        for (int i = 0; i < 18; i++) {
            Mockito.when(client.count("index2/type1", Integer.toString(i), MATCH_ALL)).thenReturn(0L);
        }
        for (int i = 0; i < 1; i++) {
            Mockito.when(client.count("index3/type1", Integer.toString(i), MATCH_ALL)).thenReturn(0L);
        }
        {
            List<PartitionDefinition> partitions = RestService.findSlicePartitions(client, settings, null,
                    Collections.<String, NodeInfo>emptyMap(), shards, LOGGER);
            assertEquals(partitions.size(), 34);
            assertEquals(new HashSet(partitions).size(), 34);
        }
    }
}
