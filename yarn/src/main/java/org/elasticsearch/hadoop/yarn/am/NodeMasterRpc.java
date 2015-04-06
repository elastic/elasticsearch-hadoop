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
package org.elasticsearch.hadoop.yarn.am;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.NMTokenCache;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.elasticsearch.hadoop.yarn.compat.YarnCompat;

class NodeMasterRpc implements AutoCloseable {

    private final Configuration cfg;
    private final NMTokenCache tokenCache;
    private NMClient client;

    public NodeMasterRpc(Configuration cfg, NMTokenCache tokenCache) {
        this.cfg = new YarnConfiguration(cfg);
        this.tokenCache = tokenCache;
    }

    public void start() {
        if (client != null) {
            return;
        }

        client = NMClient.createNMClient("Elasticsearch-YARN");
        YarnCompat.setNMTokenCache(client, tokenCache);
        client.init(cfg);
        client.start();
    }

    public Map<String, ByteBuffer> startContainer(Container container, ContainerLaunchContext containerLaunchContext) {
        try {
            return client.startContainer(container, containerLaunchContext);
        } catch (Exception ex) {
            throw new EsYarnNmException();
        }
    }

    public ContainerStatus getContainerState(ContainerId containerId, NodeId nodeId) {
        try {
            return client.getContainerStatus(containerId, nodeId);
        } catch (Exception ex) {
            throw new EsYarnNmException();
        }
    }

    @Override
    public void close() {
        if (client == null) {
            return;
        }

        client.stop();
        client = null;
    }
}
