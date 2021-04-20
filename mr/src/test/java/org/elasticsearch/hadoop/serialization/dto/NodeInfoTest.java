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
package org.elasticsearch.hadoop.serialization.dto;

import org.elasticsearch.hadoop.thirdparty.codehaus.jackson.JsonParser;
import org.elasticsearch.hadoop.thirdparty.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NodeInfoTest {
    @Test
    public void testV1() throws Exception {
        Map<String, NodeInfo> nodeMap = testNodeInfo(getClass().getResourceAsStream("client-nodes-v1.json"));
        assertFalse(nodeMap.get("Darkhawk").isIngest());
        assertFalse(nodeMap.get("Unseen").isIngest());
    }

    @Test
    public void testV2() throws Exception {
        Map<String, NodeInfo> nodeMap = testNodeInfo(getClass().getResourceAsStream("client-nodes-v2.json"));
        assertFalse(nodeMap.get("Darkhawk").isIngest());
        assertFalse(nodeMap.get("Unseen").isIngest());
    }

    @Test
    public void testV5() throws Exception {
        Map<String, NodeInfo> nodeMap = testNodeInfo(getClass().getResourceAsStream("client-nodes-v5.json"));
        assertFalse(nodeMap.get("Darkhawk").isIngest());
        assertTrue(nodeMap.get("Unseen").isIngest());
    }

    static Map<String, NodeInfo> testNodeInfo(InputStream input) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonParser jsonParser = mapper.getJsonFactory().createJsonParser(input);
        Map<String, Map<String, Object>> map =
                (Map<String, Map<String, Object>>) mapper.readValue(jsonParser, Map.class).get("nodes");
        Map<String, NodeInfo> nodeMap = new HashMap<String, NodeInfo>();
        for (Map.Entry<String, Map<String, Object>> entry : map.entrySet()) {
            NodeInfo nodeInfo = new NodeInfo(entry.getKey(), entry.getValue());
            nodeMap.put(nodeInfo.getName(), nodeInfo);
        }
        assertEquals(nodeMap.size(), 2);
        NodeInfo nodeInfo = nodeMap.get("Darkhawk");
        assertTrue(nodeInfo != null);
        assertTrue(nodeInfo.hasHttp());
        assertTrue(nodeInfo.isClient());
        assertFalse(nodeInfo.isData());
        assertEquals(nodeInfo.getHost(), "cerberus");
        assertEquals(nodeInfo.getId(), "fiR5azTbTDiX59m78yzOTw");
        assertEquals(nodeInfo.getPublishAddress(), "192.168.1.50:9200");

        nodeInfo = nodeMap.get("Unseen");
        assertTrue(nodeInfo != null);
        assertTrue(nodeInfo.hasHttp());
        assertFalse(nodeInfo.isClient());
        assertTrue(nodeInfo.isData());
        assertEquals(nodeInfo.getHost(), "cerberus");
        assertEquals(nodeInfo.getId(), "L9DM79IvStq2RStogXR8Sg");
        assertEquals(nodeInfo.getPublishAddress(), "192.168.1.50:9201");

        return nodeMap;
    }
}
