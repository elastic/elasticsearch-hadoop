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
package org.elasticsearch.hadoop.fixtures;


import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.HdpBootstrap;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.rest.RestUtils;
import org.elasticsearch.hadoop.rest.InitializationUtils;
import org.elasticsearch.hadoop.util.ClusterInfo;
import org.elasticsearch.hadoop.util.EsMajorVersion;
import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.hadoop.util.TestSettings;
import org.elasticsearch.hadoop.util.TestUtils;
import org.elasticsearch.hadoop.util.unit.Booleans;
import org.junit.rules.ExternalResource;

public class LocalEs extends ExternalResource {

    private static EsEmbeddedCluster embeddedCluster;

    @Override
    protected void before() throws Throwable {
        if (Booleans.parseBoolean(HdpBootstrap.hadoopConfig().get(EsEmbeddedCluster.DISABLE_LOCAL_ES))) {
            LogFactory.getLog(getClass()).warn("local ES disable; assuming an external instance...");
            setSingleNodeTemplate();
            clearState();
            return;
        }

        String host = HdpBootstrap.hadoopConfig().get(ConfigurationOptions.ES_NODES);
        if (StringUtils.hasText(host)) {
            LogFactory.getLog(getClass()).warn("es.nodes/host specified; assuming an external instance...");
            setSingleNodeTemplate();
            clearState();
            return;
        }

        if (embeddedCluster == null) {
            System.out.println("Locating Embedded Elasticsearch Cluster...");
            embeddedCluster = new EsEmbeddedCluster();
            for (StringUtils.IpAndPort ipAndPort : embeddedCluster.getIpAndPort()) {
                System.out.println("Found Elasticsearch Node on port " + ipAndPort.port);
            }
            System.setProperty(TestUtils.ES_LOCAL_PORT, String.valueOf(embeddedCluster.getIpAndPort().get(0).port));

            // force initialization of test properties
            new TestSettings();
            setSingleNodeTemplate();
            clearState();
        }
    }

    /**
     * Installs an index template that sets number of shards to 1 and number of replicas to 0.
     * Note, that this is only needed for ES version prior to 7.0
     */
    private void setSingleNodeTemplate() throws Exception {
        LogFactory.getLog(getClass()).warn("Installing single node template...");
        ClusterInfo clusterInfo = InitializationUtils.discoverClusterInfo(new TestSettings(), LogFactory.getLog(this.getClass()));
        if (clusterInfo.getMajorVersion().onOrBefore(EsMajorVersion.V_5_X)) {
            RestUtils.put("_template/single-node-template",
                    "{\"template\": \"*\", \"settings\": {\"number_of_shards\": 1,\"number_of_replicas\": 0}}".getBytes());
        } else if (clusterInfo.getMajorVersion().onOrBefore(EsMajorVersion.V_6_X)) {
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
        if (embeddedCluster != null) {
            try {
                System.clearProperty(TestUtils.ES_LOCAL_PORT);
            } catch (Exception ex) {
                // ignore
            }
            embeddedCluster = null;
        }
    }
}
