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
package org.elasticsearch.hadoop.util;

import java.util.List;
import java.util.Properties;

import org.elasticsearch.hadoop.cfg.PropertiesSettings;
import org.junit.Test;

import static org.junit.Assert.assertThat;

import static org.hamcrest.core.IsEqual.equalTo;

public class SettingsUtilsTest {

    @Test
    public void testHostWithoutAPortFallingBackToDefault() throws Exception {
        Properties props = new Properties();
        props.setProperty("es.nodes", "localhost");
        props.setProperty("es.port", "9800");

        PropertiesSettings settings = new PropertiesSettings(props);
        List<String> nodes = SettingsUtils.discoveredOrDeclaredNodes(settings);
        assertThat(nodes.size(), equalTo(1));
        assertThat("127.0.0.1:9800", equalTo(nodes.get(0)));
    }

    @Test
    public void testHostWithoutAPortFallingBackToDefaultAndNoDiscovery() throws Exception {
        Properties props = new Properties();
        props.setProperty("es.nodes", "localhost");
        props.setProperty("es.port", "9800");
        props.setProperty("es.nodes.discovery", "false");

        PropertiesSettings settings = new PropertiesSettings(props);
        List<String> nodes = SettingsUtils.discoveredOrDeclaredNodes(settings);
        assertThat(nodes.size(), equalTo(1));
        assertThat("127.0.0.1:9800", equalTo(nodes.get(0)));
    }

    @Test
    public void testHostWithAPortAndFallBack() throws Exception {
        Properties props = new Properties();
        props.setProperty("es.nodes", "localhost:9800");
        props.setProperty("es.port", "9300");
        props.setProperty("es.nodes.discovery", "false");

        PropertiesSettings settings = new PropertiesSettings(props);
        List<String> nodes = SettingsUtils.discoveredOrDeclaredNodes(settings);
        assertThat(nodes.size(), equalTo(1));
        assertThat("127.0.0.1:9800", equalTo(nodes.get(0)));
    }
}
