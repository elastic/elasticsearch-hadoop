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
package org.elasticsearch.hadoop.integration;

import java.util.Properties;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

public class ESEmbeddedServer {

    private final Node node;

    public ESEmbeddedServer(String dataPath, String httpRange, String transportRange) {
        Properties props = new Properties();
        props.setProperty("path.data", dataPath);
        props.setProperty("http.port", httpRange);
        props.setProperty("transport.tcp.port", transportRange);

        Settings settings = ImmutableSettings.settingsBuilder().put(props).build();
        node = NodeBuilder.nodeBuilder().local(false).client(false).settings(settings).build();
    }

    public void start() {
        node.start();
    }

    public void stop() {
        node.close();
    }
}
