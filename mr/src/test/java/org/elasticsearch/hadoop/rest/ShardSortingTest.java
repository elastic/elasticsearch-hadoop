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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.elasticsearch.hadoop.serialization.dto.Node;
import org.elasticsearch.hadoop.serialization.dto.Shard;
import org.junit.Test;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static org.junit.Assert.*;

public class ShardSortingTest {

    private final URL nodeList = this.getClass().getResource("node-list.json");
    private final URL shardList = this.getClass().getResource("shard-list.json");
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void testLargeIndexShardList() throws Exception {
        Map<String, Node> nodes = readNodes();
        List<List<Map<String, Object>>> targetShards = readShards();
        Map<Shard, Node> result = ShardSorter.find(targetShards, nodes, null);
        Set<Integer> index1Shards = Sets.newHashSet(0, 1, 2, 3, 4);
        Set<Integer> index2Shards = Sets.newHashSet(0, 1, 2, 3, 4);
        for(Shard shard : result.keySet()) {
            String index = shard.getIndex();
            if(index.equals("index1")) {
                index1Shards.remove(shard.getName());
            } else if (index.equals("index2")){
                index2Shards.remove(shard.getName());
            }
        }
        assertTrue(index1Shards.isEmpty());
        assertTrue(index2Shards.isEmpty());
    }

    private Map<String, Node> readNodes() throws java.io.IOException {
        Map<String, Object> values = mapper.readValue(nodeList.openStream(),
                                                      new TypeReference<Map<String, Object>>() {
                                                      });
        Map<String, Object> rawNodes = (Map<String, Object>)values.get("nodes");
        Map<String, Node> nodes = Maps.newLinkedHashMap();
        for (String nodeId : rawNodes.keySet()) {
            Node node = new Node(nodeId, (Map<String, Object>)rawNodes.get(nodeId));
            nodes.put(nodeId, node);
        }
        return nodes;
    }

    private List<List<Map<String, Object>>> readShards() throws java.io.IOException {
        return mapper.readValue(shardList.openStream(),
                                new TypeReference<List<List<Map<String, Object>>>>() {});
    }

    @Test
    public void testPowerSet() {
        Set<Integer> set = new LinkedHashSet<Integer>();

        for (int i = 0; i < 10; i++) {
            set.add(Integer.valueOf(i));
        }
        List<Set<Integer>> powerList = Lists.newArrayList(ShardSorter.powerList(set));
        assertEquals(1023, powerList.size());
    }

    private <K, V> Map<K, V> map(K k1, V v1) {
        return map(k1, v1, null, null, null, null);
    }

    private <K, V> Map<K, V> map(K k1, V v1, K k2, V v2) {
        return map(k1, v1, k2, v2, null, null);
    }

    private <K, V> Map<K, V> map(K k1, V v1, K k2, V v2, K k3, V v3) {
        Map<K, V> m = new LinkedHashMap<K, V>();
        m.put(k1, v1);
        if (k2 != null && v2 != null) {
            m.put(k2, v2);
            if (k3 != null && v3 != null) {
                m.put(k3, v3);
            }
        }
        return m;
    }

    @Test
    public void testAllShardsOnOneNode() throws Exception {
        assertEquals(map("A", "N").toString(),
                topology(map("N", Arrays.asList("A", "B", "C"))).toString());
    }

    @Test
    public void testPrimariesAndReplicas() throws Exception {
        assertEquals(map("A", "M").toString(),
                topology(map(
                        "N", Arrays.asList("A", "B", "C"),
                        "M", Arrays.asList("A", "B", "C")
                        )).toString());
    }

    @Test
    public void testDuplicatesOnAllNodes() throws Exception {
        assertEquals(Collections.emptyMap().toString(),
                topology(map(
                        "N", Arrays.asList("A", "B"),
                        "M", Arrays.asList("B", "C"),
                        "P", Arrays.asList("A", "C")
                        )).toString());
    }

    @Test
    public void testTwoShardsDuplicatedOnTwoNodesAndOneShardSingular() throws Exception {
        assertEquals(map("A", "M", "C", "P").toString(),
                topology(map(
                        "N", Arrays.asList("A", "B"),
                        "M", Arrays.asList("B", "A"),
                        "P", Arrays.asList("C")
                        )).toString());
    }

    // Shard => Node
    private Map<String, String> topology(Map<String, List<String>> shardsPerNode) {
        Map<String, Node> nodes = new LinkedHashMap<String, Node>();
        for (String node : shardsPerNode.keySet()) {
            Map<String, Object> data = new LinkedHashMap<String, Object>();
            data.put("name", node);
            data.put("http_address", "inet[/1.2.3.4:9200]");
            nodes.put(node, new Node(node, data));
        }

        Map<String, List<Map<String, Object>>> shardGroups = new LinkedHashMap<String, List<Map<String, Object>>>();

        for (Map.Entry<String, List<String>> entry : shardsPerNode.entrySet()) {
            Node node = nodes.get(entry.getKey());

            for (String shardName : entry.getValue()) {
                Map<String, Object> data = new LinkedHashMap<String, Object>();
                data.put("state", "STARTED");
                data.put("primary", true);
                data.put("node", node.getId());
                data.put("shard", 0);
                data.put("index", shardName);

                List<Map<String, Object>> list = shardGroups.get(shardName);
                if (list == null) {
                    list = new ArrayList<Map<String, Object>>();
                    shardGroups.put(shardName, list);
                }
                list.add(data);
            }
        }

        List<List<Map<String, Object>>> targetShards = new ArrayList<List<Map<String, Object>>>();
        targetShards.addAll(shardGroups.values());

        Map<Shard, Node> find = ShardSorter.find(targetShards, nodes, LogFactory.getLog(ShardSortingTest.class));
        if (find.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, String> nodesName = new LinkedHashMap<String, String>();
        for (Entry<Shard, Node> entry : find.entrySet()) {
            nodesName.put(entry.getKey().getIndex(), entry.getValue().getName());
        }

        return nodesName;
    }
}