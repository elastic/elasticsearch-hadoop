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

package org.elasticsearch.hadoop.qa.kerberos;

import java.security.PrivilegedExceptionAction;
import java.util.List;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;

import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.RestUtils;
import org.elasticsearch.hadoop.rest.RestClient;
import org.elasticsearch.hadoop.rest.commonshttp.auth.spnego.SpnegoNegotiator;
import org.elasticsearch.hadoop.serialization.dto.NodeInfo;
import org.elasticsearch.hadoop.util.TestSettings;
import org.junit.Test;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class AbstractKerberosClientTest {

    @Test
    public void testNegotiateWithExternalKDC() throws Exception {
        LoginContext loginCtx = LoginUtil.login("client", "password");
        try {
            Subject.doAs(loginCtx.getSubject(), new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                    SpnegoNegotiator negotiator = new SpnegoNegotiator("client", "HTTP/es.build.elastic.co");
                    assertNotNull(negotiator.send());
                    return null;
                }
            });
        } finally {
            loginCtx.logout();
        }
    }

    @Test
    public void testSpnegoAuthToES() throws Exception {
        RestUtils.postData("_xpack/security/role_mapping/kerberos_client_mapping",
                "{\"roles\":[\"superuser\"],\"enabled\":true,\"rules\":{\"field\":{\"username\":\"client@BUILD.ELASTIC.CO\"}}}".getBytes());

        LoginContext loginCtx = LoginUtil.login("client", "password");
        try {
            Subject.doAs(loginCtx.getSubject(), new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                    TestSettings testSettings = new TestSettings();
                    // Remove the regular auth settings
                    testSettings.asProperties().remove(ConfigurationOptions.ES_NET_HTTP_AUTH_USER);
                    testSettings.asProperties().remove(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS);

                    // Set kerberos settings
                    testSettings.setProperty(ConfigurationOptions.ES_NET_SPNEGO_AUTH_USER_PRINCIPAL, "client@BUILD.ELASTIC.CO");
                    testSettings.setProperty(ConfigurationOptions.ES_NET_SPNEGO_AUTH_ELASTICSEARCH_PRINCIPAL, "HTTP/es.build.elastic.co@BUILD.ELASTIC.CO");

                    RestClient restClient = new RestClient(testSettings);
                    List<NodeInfo> httpDataNodes = restClient.getHttpDataNodes();
                    assertThat(httpDataNodes.size(), is(greaterThan(0)));

                    return null;
                }
            });
        } finally {
            loginCtx.logout();
            RestUtils.delete("_xpack/security/role_mapping/kerberos_client_mapping");
        }
    }
}
