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

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.hadoop.util.StringUtils.IpAndPort;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.groovy.GroovyPlugin;

public class EsEmbeddedServer {
    private static class PluginConfigurableNode extends Node {
        public PluginConfigurableNode(Settings settings, Collection<Class<? extends Plugin>> classpathPlugins) {
            super(InternalSettingsPreparer.prepareEnvironment(settings, null), Version.CURRENT, classpathPlugins);
        }
    }

    private final Node node;
    private IpAndPort ipAndPort;

    @SuppressWarnings({ "unchecked", "rawtypes" })
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
        props.setProperty("script.inline", "true");
        props.setProperty("script.indexed", "true");

        Settings settings = NodeBuilder.nodeBuilder().local(false).client(false).settings(Settings.settingsBuilder().put(props).build()).clusterName(clusterName).getSettings().build();
        Collection plugins = Arrays.asList(GroovyPlugin.class);
        node = new PluginConfigurableNode(settings, plugins);
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
