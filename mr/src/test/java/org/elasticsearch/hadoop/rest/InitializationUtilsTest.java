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

import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.util.TestSettings;
import org.junit.Test;

import static org.junit.Assert.*;

import static org.elasticsearch.hadoop.cfg.ConfigurationOptions.*;
import static org.elasticsearch.hadoop.rest.InitializationUtils.*;

public class InitializationUtilsTest {

    @Test
    public void testValidateDefaultSettings() throws Exception {
        Settings set = new TestSettings();
        validateSettings(set);

        assertFalse(set.getNodesWANOnly());
        assertTrue(set.getNodesDiscovery());
        assertTrue(set.getNodesDataOnly());
        assertFalse(set.getNodesClientOnly());
    }

    @Test
    public void testValidateWANOnly() throws Exception {
        Settings set = new TestSettings();
        set.setProperty(ES_NODES_WAN_ONLY, "true");
        validateSettings(set);

        assertTrue(set.getNodesWANOnly());
        assertFalse(set.getNodesDiscovery());
        assertFalse(set.getNodesDataOnly());
        assertFalse(set.getNodesClientOnly());
    }

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testValidateWANOnlyWithDiscovery() throws Exception {
        Settings set = new TestSettings();
        set.setProperty(ES_NODES_WAN_ONLY, "true");
        set.setProperty(ES_NODES_DISCOVERY, "true");
        validateSettings(set);
    }

    @Test
    public void testValidateClientOnlyNodesWithDefaultData() throws Exception {
        Settings set = new TestSettings();
        set.setProperty(ES_NODES_CLIENT_ONLY, "true");
        validateSettings(set);
    }

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testValidateDefaultDataVsClientOnlyNodes() throws Exception {
        Settings set = new TestSettings();
        set.setProperty(ES_NODES_CLIENT_ONLY, "true");
        set.setProperty(ES_NODES_DATA_ONLY, "true");
        validateSettings(set);
    }
}