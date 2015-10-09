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
package org.elasticsearch.hadoop;

import java.util.Properties;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.hadoop.util.StringUtils.IpAndPort;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

public class EsEmbeddedServer {

    private final Node node;
    private IpAndPort ipAndPort;

    public EsEmbeddedServer(String clusterName, String homePath, String dataPath, String httpRange, String transportRange, boolean hasSlave) {
        Properties props = new Properties();
        props.setProperty("path.home", homePath);
        props.setProperty("path.data", dataPath);
        props.setProperty("http.port", httpRange);
        props.setProperty("transport.tcp.port", transportRange);
        props.setProperty("cluster.name", "es.hadoop.test");
        props.setProperty("node.local", "true");
        //props.setProperty("es.index.store.type", "memory");
        // props.setProperty("gateway.type", "none");
        if (!hasSlave) {
            props.setProperty("discovery.zen.ping.multicast", "false");
            props.setProperty("discovery.zen.ping.multicast.enabled", "false");
        }
        //props.setProperty("script.disable_dynamic", "false");
        props.setProperty("script.inline", "on");
        props.setProperty("script.indexed", "on");

        Settings settings = Settings.settingsBuilder().put(props).build();
        node = NodeBuilder.nodeBuilder().local(false).client(false).settings(settings).clusterName(clusterName).build();
    }

    public void start() {
        node.start();
        // find out port
        String localNodeId = node.client().admin().cluster().prepareState().get().getState().getNodes().getLocalNodeId();
        String value = node.client().admin().cluster().prepareNodesInfo(localNodeId).get().iterator().next().getHttp().address().publishAddress().toString();

        ipAndPort = StringUtils.parseIpAddress(value);
    }

    public void stop() {
        node.close();
    }

    public IpAndPort getIpAndPort() {
        return ipAndPort;
    }
}
