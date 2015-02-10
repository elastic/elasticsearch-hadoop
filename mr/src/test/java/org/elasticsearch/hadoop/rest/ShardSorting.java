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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.LogFactory;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.hadoop.serialization.dto.Node;
import org.elasticsearch.hadoop.serialization.dto.Shard;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ShardSorting {

    @Test
    public void testPowerSet() {
        Set<Integer> set = new LinkedHashSet<Integer>();

        for (int i = 0; i < 10; i++) {
            set.add(Integer.valueOf(i));
        }
        assertEquals(1023, ShardSorter.powerList(set).size());
    }


    @Test
    public void testAllShardsOnOneNode() throws Exception {
        assertEquals(ImmutableMap.of("A", "N").toString(),
                topology(ImmutableMap.<String, List<String>> of("N", ImmutableList.of("A", "B", "C"))).toString());
    }

    @Test
    public void testPrimariesAndReplicas() throws Exception {
        assertEquals(ImmutableMap.of("A", "M").toString(),
                topology(ImmutableMap.<String, List<String>> of(
                        "N", ImmutableList.of("A", "B", "C"),
                        "M", ImmutableList.of("A", "B", "C")
                        )).toString());
    }

    @Test
    public void testDuplicatesOnAllNodes() throws Exception {
        assertEquals(Collections.emptyMap().toString(),
                topology(ImmutableMap.<String, List<String>> of(
                        "N", ImmutableList.of("A", "B"),
                        "M", ImmutableList.of("B", "C"),
                        "P", ImmutableList.of("A", "C")
                        )).toString());
    }

    @Test
    public void testTwoShardsDuplicatedOnTwoNodesAndOneShardSingular() throws Exception {
        assertEquals(ImmutableMap.of("A", "M", "C", "P").toString(),
                topology(ImmutableMap.<String, List<String>> of(
                        "N", ImmutableList.of("A", "B"),
                        "M", ImmutableList.of("B", "A"),
                        "P", ImmutableList.of("C")
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

        Map<Shard, Node> find = ShardSorter.find(targetShards, nodes, LogFactory.getLog(ShardSorting.class));
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