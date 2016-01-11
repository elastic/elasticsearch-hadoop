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
package org.elasticsearch.hadoop.integration.rest.ssl;

import java.util.Properties;

import org.elasticsearch.hadoop.HdpBootstrap;
import org.elasticsearch.hadoop.cfg.PropertiesSettings;
import org.elasticsearch.hadoop.rest.Request;
import org.elasticsearch.hadoop.rest.Request.Method;
import org.elasticsearch.hadoop.rest.Response;
import org.elasticsearch.hadoop.rest.SimpleRequest;
import org.elasticsearch.hadoop.rest.commonshttp.CommonsHttpTransport;
import org.elasticsearch.hadoop.util.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

import static org.junit.Assert.*;

import static org.elasticsearch.hadoop.cfg.ConfigurationOptions.*;

public class SSLTests {

    private static int SSL_PORT;
    private final String PREFIX = "prefixed_path";

    @ClassRule
    public static ExternalResource SSL_SERVER = new ExternalResource() {
        private BasicSSLServer server;

        @Override
        protected void before() throws Throwable {
            Properties props = HdpBootstrap.asProperties(HdpBootstrap.hadoopConfig());
            SSL_PORT = Integer.parseInt(props.getProperty("test.ssl.port", "12345"));

            server = new BasicSSLServer(SSL_PORT);
            server.start();
        }

        @Override
        protected void after() {
            try {
                server.stop();
            } catch (Exception ex) {
            }
        }
    };

    private PropertiesSettings cfg;
    private CommonsHttpTransport transport;

    @Before
    public void setup() {
        cfg = new PropertiesSettings();
        cfg.setPort(SSL_PORT);
        cfg.setProperty(ES_NET_USE_SSL, "true");
        cfg.setProperty(ES_NET_SSL_CERT_ALLOW_SELF_SIGNED, "true");
        cfg.setProperty(ES_NET_SSL_TRUST_STORE_LOCATION, "ssl/client.jks");
        cfg.setProperty(ES_NET_SSL_TRUST_STORE_PASS, "testpass");
        cfg.setProperty(ES_NODES_PATH_PREFIX, PREFIX);

        transport = new CommonsHttpTransport(cfg.copy(), "localhost");
    }

    @After
    public void destroy() {
        transport.close();
    }

    @Test
    public void testBasicSSLHandshake() throws Exception {
        String uri = "localhost:" + SSL_PORT;
        String path = "/basicSSL";
        Request req = new SimpleRequest(Method.GET, uri, path);

        Response execute = transport.execute(req);
        String content = IOUtils.asString(execute.body());
        assertEquals("/" + PREFIX + path, content);
    }

    @Test
    public void testBasicSSLHandshakeWithSchema() throws Exception {
        String uri = "https://localhost:" + SSL_PORT;
        String path = "/basicSSL";
        Request req = new SimpleRequest(Method.GET, uri, path);

        Response execute = transport.execute(req);
        String content = IOUtils.asString(execute.body());
        assertEquals("/" + PREFIX + path, content);
    }
}