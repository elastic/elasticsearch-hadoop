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


import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.RestUtils;
import org.elasticsearch.hadoop.rest.InitializationUtils;
import org.elasticsearch.hadoop.util.EsMajorVersion;
import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.hadoop.util.TestSettings;
import org.elasticsearch.hadoop.util.TestUtils;
import org.elasticsearch.hadoop.util.unit.Booleans;
import org.junit.rules.ExternalResource;

public class LocalEs extends ExternalResource {

    private static EsEmbeddedServer master;

    @Override
    protected void before() throws Throwable {
        if (Booleans.parseBoolean(HdpBootstrap.hadoopConfig().get("test.disable.local.es"))) {
            LogFactory.getLog(getClass()).warn(
                    "local ES disable; assuming an external instance and bailing out...");
            setSingleNodeTemplate();
            clearState();
            return;
        }

        String host = HdpBootstrap.hadoopConfig().get(ConfigurationOptions.ES_NODES);
        if (StringUtils.hasText(host)) {
            LogFactory.getLog(getClass()).warn("es.nodes/host specified; assuming an external instance and bailing out...");
            setSingleNodeTemplate();
            clearState();
            return;
        }

        if (master == null) {
            System.out.println("Locating Elasticsearch Node...");
            master = new EsEmbeddedServer();
            System.out.println("Found Elasticsearch Node on port " + master.getIpAndPort().port);
            System.setProperty(TestUtils.ES_LOCAL_PORT, String.valueOf(master.getIpAndPort().port));

            // force initialization of test properties
            new TestSettings();
            setSingleNodeTemplate();
            clearState();
        }
    }

    private void setSingleNodeTemplate() throws Exception {
        LogFactory.getLog(getClass()).warn("Installing single node template...");
        EsMajorVersion version = InitializationUtils.discoverEsVersion(new TestSettings(), LogFactory.getLog(this.getClass()));
        if (version.onOrBefore(EsMajorVersion.V_5_X)) {
            RestUtils.put("_template/single-node-template",
                    "{\"template\": \"*\", \"settings\": {\"number_of_shards\": 1,\"number_of_replicas\": 0}}".getBytes());
        } else {
            RestUtils.put("_template/single-node-template",
                    "{\"index_patterns\": \"*\", \"settings\": {\"number_of_shards\": 1,\"number_of_replicas\": 0}}".getBytes());
        }
    }

    private void clearState() throws Exception {
        LogFactory.getLog(getClass()).warn("Wiping all existing indices from the node");
        RestUtils.delete("/_all");
    }

    @Override
    protected void after() {
        if (master != null) {
            try {
                System.clearProperty(TestUtils.ES_LOCAL_PORT);
            } catch (Exception ex) {
                // ignore
            }
            master = null;
        }
    }
}
