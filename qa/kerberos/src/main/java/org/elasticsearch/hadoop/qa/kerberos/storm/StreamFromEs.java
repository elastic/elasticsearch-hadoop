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

package org.elasticsearch.hadoop.qa.kerberos.storm;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.security.LoginUtil;
import org.elasticsearch.storm.EsSpout;
import org.elasticsearch.storm.security.AutoElasticsearch;

public class StreamFromEs {
    public static void main(String[] args) throws Exception {
        final String submitPrincipal = args[0];
        final String submitKeytab = args[1];
        final String esNodes = args[2];
        LoginContext loginContext = LoginUtil.keytabLogin(submitPrincipal, submitKeytab);
        try {
            Subject.doAs(loginContext.getSubject(), new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                    submitJob(submitPrincipal, submitKeytab, esNodes);
                    return null;
                }
            });
        } finally {
            loginContext.logout();
        }
    }

    public static void submitJob(String principal, String keytab, String esNodes) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("ES", new EsSpout("storm-test"));
        builder.setBolt("Output", new CapturingBolt()).shuffleGrouping("ES");

        // Nimbus needs to be started with the cred renewer and credentials plugins set in its config file

        Config conf = new Config();
        List<Object> plugins = new ArrayList<Object>();
        plugins.add(AutoElasticsearch.class.getName());
        conf.put(Config.TOPOLOGY_AUTO_CREDENTIALS, plugins);
        conf.put(ConfigurationOptions.ES_NODES, esNodes);
        conf.put(ConfigurationOptions.ES_SECURITY_AUTHENTICATION, "kerberos");
        conf.put(ConfigurationOptions.ES_NET_SPNEGO_AUTH_ELASTICSEARCH_PRINCIPAL, "HTTP/build.elastic.co@BUILD.ELASTIC.CO");
        conf.put(ConfigurationOptions.ES_INPUT_JSON, "true");
        StormSubmitter.submitTopology("test-read", conf, builder.createTopology());
    }
}
