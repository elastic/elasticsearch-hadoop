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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.NoOpLog;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.RestUtils;
import org.elasticsearch.hadoop.mr.security.HadoopUserProvider;
import org.elasticsearch.hadoop.rest.InitializationUtils;
import org.elasticsearch.hadoop.rest.NetworkClient;
import org.elasticsearch.hadoop.rest.Request;
import org.elasticsearch.hadoop.rest.RestClient;
import org.elasticsearch.hadoop.rest.SimpleRequest;
import org.elasticsearch.hadoop.rest.commonshttp.auth.spnego.SpnegoNegotiator;
import org.elasticsearch.hadoop.security.EsToken;
import org.elasticsearch.hadoop.security.JdkUserProvider;
import org.elasticsearch.hadoop.security.LoginUtil;
import org.elasticsearch.hadoop.security.User;
import org.elasticsearch.hadoop.security.UserProvider;
import org.elasticsearch.hadoop.serialization.dto.NodeInfo;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.ObjectUtils;
import org.elasticsearch.hadoop.util.TestSettings;
import org.junit.Assume;
import org.junit.Test;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class AbstractKerberosClientTest {

    private static final Log LOG = LogFactory.getLog(AbstractKerberosClientTest.class);

    @Test
    public void testNegotiateWithExternalKDC() throws Exception {
        LoginContext loginCtx = LoginUtil.login("client", "password");
        try {
            Subject.doAs(loginCtx.getSubject(), new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                    SpnegoNegotiator negotiator = new SpnegoNegotiator("client", "HTTP/build.elastic.co");
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
                    InitializationUtils.setUserProviderIfNotSet(testSettings, JdkUserProvider.class, null);
                    // Remove the regular auth settings
                    testSettings.asProperties().remove(ConfigurationOptions.ES_NET_HTTP_AUTH_USER);
                    testSettings.asProperties().remove(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS);

                    // Set kerberos settings
                    testSettings.setProperty(ConfigurationOptions.ES_SECURITY_AUTHENTICATION, "kerberos");
                    testSettings.setProperty(ConfigurationOptions.ES_NET_SPNEGO_AUTH_ELASTICSEARCH_PRINCIPAL, "HTTP/build.elastic.co@BUILD.ELASTIC.CO");

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

    @Test
    public void testSpnegoAuthWithKeytabToES() throws Exception {
        String hivePrincipal = System.getProperty("tests.hive.principal");
        Assert.hasText(hivePrincipal, "Needs tests.hive.principal system property");
        String hiveKeytab = System.getProperty("tests.hive.keytab");
        Assert.hasText(hiveKeytab, "Needs tests.hive.keytab system property");

        RestUtils.postData("_xpack/security/role_mapping/kerberos_client_mapping",
                ("{\"roles\":[\"superuser\"],\"enabled\":true,\"rules\":{\"field\":{\"username\":\""+hivePrincipal+"\"}}}").getBytes());

        LoginContext loginCtx = LoginUtil.keytabLogin(hivePrincipal, hiveKeytab);
        try {
            Subject.doAs(loginCtx.getSubject(), new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                    TestSettings testSettings = new TestSettings();
                    InitializationUtils.setUserProviderIfNotSet(testSettings, JdkUserProvider.class, null);
                    // Remove the regular auth settings
                    testSettings.asProperties().remove(ConfigurationOptions.ES_NET_HTTP_AUTH_USER);
                    testSettings.asProperties().remove(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS);

                    // Set kerberos settings
                    testSettings.setProperty(ConfigurationOptions.ES_SECURITY_AUTHENTICATION, "kerberos");
                    testSettings.setProperty(ConfigurationOptions.ES_NET_SPNEGO_AUTH_ELASTICSEARCH_PRINCIPAL, "HTTP/build.elastic.co@BUILD.ELASTIC.CO");

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

    @Test
    public void testMutualSpnegoAuthToES() throws Exception {
        RestUtils.postData("_xpack/security/role_mapping/kerberos_client_mapping",
                "{\"roles\":[\"superuser\"],\"enabled\":true,\"rules\":{\"field\":{\"username\":\"client@BUILD.ELASTIC.CO\"}}}".getBytes());

        LoginContext loginCtx = LoginUtil.login("client", "password");
        try {
            Subject.doAs(loginCtx.getSubject(), new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                    TestSettings testSettings = new TestSettings();
                    InitializationUtils.setUserProviderIfNotSet(testSettings, JdkUserProvider.class, null);
                    // Remove the regular auth settings
                    testSettings.asProperties().remove(ConfigurationOptions.ES_NET_HTTP_AUTH_USER);
                    testSettings.asProperties().remove(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS);

                    // Set kerberos settings
                    testSettings.setProperty(ConfigurationOptions.ES_SECURITY_AUTHENTICATION, "kerberos");
                    testSettings.setProperty(ConfigurationOptions.ES_NET_SPNEGO_AUTH_ELASTICSEARCH_PRINCIPAL, "HTTP/build.elastic.co@BUILD.ELASTIC.CO");
                    testSettings.setProperty(ConfigurationOptions.ES_NET_SPNEGO_AUTH_MUTUAL, "true");

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

    @Test
    public void testProxyKerberosAuth() throws Exception {
        String hivePrincipal = System.getProperty("tests.hive.principal");
        Assert.hasText(hivePrincipal, "Needs tests.hive.principal system property");
        String hiveKeytab = System.getProperty("tests.hive.keytab");
        Assert.hasText(hiveKeytab, "Needs tests.hive.keytab system property");
        String realUserName = hivePrincipal;
        String proxyUserName = "client";

        // Create a user that the real user will proxy as
        LOG.info("Creating proxied user");
        RestUtils.postData("_xpack/security/user/" + proxyUserName, (
                "{\n" +
                "  \"enabled\" : true,\n" +
                "  \"password\" : \"password\",\n" +
                "  \"roles\" : [ \"superuser\" ],\n" +
                "  \"full_name\" : \"Client McClientFace\"\n" +
                "}").getBytes());

        // This just mirrors the superuser role as they can impersonate all users
        LOG.info("Creating proxy role");
        RestUtils.postData("_xpack/security/role/proxier", (
                "{\n" +
                "  \"cluster\": [\n" +
                "    \"all\"\n" +
                "  ],\n" +
                "  \"indices\": [\n" +
                "    {\n" +
                "      \"names\": [\n" +
                "        \"*\"\n" +
                "      ],\n" +
                "      \"privileges\": [\n" +
                "        \"all\"\n" +
                "      ],\n" +
                "      \"allow_restricted_indices\": true\n" +
                "    }\n" +
                "  ],\n" +
                "  \"applications\": [\n" +
                "    {\n" +
                "      \"application\": \"*\",\n" +
                "      \"privileges\": [\n" +
                "        \"*\"\n" +
                "      ],\n" +
                "      \"resources\": [\n" +
                "        \"*\"\n" +
                "      ]\n" +
                "    }\n" +
                "  ],\n" +
                "  \"run_as\": [\n" +
                "    \"*\"\n" +
                "  ],\n" +
                "  \"transient_metadata\": {}\n" +
                "}").getBytes());

        LOG.info("Creating mapping for hive principal to proxier role");
        RestUtils.postData("_xpack/security/role_mapping/kerberos_proxy_client_mapping",
                ("{\"roles\":[\"proxier\"],\"enabled\":true,\"rules\":{\"field\":{\"username\":\""+realUserName+"\"}}}").getBytes());

        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        SecurityUtil.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS, configuration);
        UserGroupInformation.setConfiguration(configuration);
        UserGroupInformation realUser = UserGroupInformation.loginUserFromKeytabAndReturnUGI(hivePrincipal, hiveKeytab);
        UserGroupInformation proxyUser = UserGroupInformation.createProxyUser(proxyUserName, realUser);
        proxyUser.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
                assertEquals("client", currentUser.getUserName());
                assertEquals("hive/build.elastic.co@BUILD.ELASTIC.CO", currentUser.getRealUser().getUserName());
                assertFalse(currentUser.hasKerberosCredentials());
                assertEquals(UserGroupInformation.AuthenticationMethod.PROXY, currentUser.getAuthenticationMethod());
                assertEquals(UserGroupInformation.AuthenticationMethod.KERBEROS, currentUser.getRealAuthenticationMethod());

                Settings testSettings = new TestSettings();
                testSettings.asProperties().remove(ConfigurationOptions.ES_NET_HTTP_AUTH_USER);
                testSettings.asProperties().remove(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS);

                InitializationUtils.setUserProviderIfNotSet(testSettings, HadoopUserProvider.class, new NoOpLog());
                testSettings.setProperty(ConfigurationOptions.ES_SECURITY_AUTHENTICATION, "kerberos");
                testSettings.setProperty(ConfigurationOptions.ES_NET_SPNEGO_AUTH_ELASTICSEARCH_PRINCIPAL, "HTTP/build.elastic.co@BUILD.ELASTIC.CO");

                UserProvider userProvider = UserProvider.create(testSettings);
                assertTrue(userProvider.isEsKerberosEnabled());

                LOG.info("Getting cluster info");
                InitializationUtils.discoverClusterInfo(testSettings, LOG);

                LOG.info("Checking authenticate with Proxied User");
                NetworkClient network = new NetworkClient(testSettings);
                try {
                    network.execute(new SimpleRequest(Request.Method.GET, "", "/_security/_authenticate", ""));
                } finally {
                    network.close();
                }

                LOG.info("Getting an API Token");
                RestClient client = new RestClient(testSettings);
                EsToken proxyToken;
                try {
                    proxyToken = client.createNewApiToken("proxyToken");
                } finally {
                    client.close();
                }

                LOG.info("Making another client without the token available yet");
                network = new NetworkClient(testSettings);
                try {
                    LOG.info("Checking authenticate to make sure it's still SPNEGO");
                    network.execute(new SimpleRequest(Request.Method.GET, "", "/_security/_authenticate", ""));
                    LOG.info("Adding token to user now");
                    userProvider.getUser().addEsToken(proxyToken);
                    LOG.info("Checking authenticate with same client again to make sure it's still SPNEGO");
                    network.execute(new SimpleRequest(Request.Method.GET, "", "/_security/_authenticate", ""));
                } finally {
                    network.close();
                }

                LOG.info("Making new client to pick up newly added token");
                network = new NetworkClient(testSettings);
                try {
                    network.execute(new SimpleRequest(Request.Method.GET, "", "/_security/_authenticate", ""));
                } finally {
                    network.close();
                }

                return null;
            }
        });
        RestUtils.delete("_xpack/security/role_mapping/kerberos_proxy_client_mapping");
    }

    @Test
    public void testBasicIntoTokenAuth() throws Exception {
        TestSettings testSettings = new TestSettings();
        Assume.assumeTrue(testSettings.getNetworkHttpAuthUser() != null);
        Assume.assumeTrue(testSettings.getNetworkHttpAuthPass() != null);
        InitializationUtils.setUserProviderIfNotSet(testSettings, JdkUserProvider.class, LOG);
        RestClient restClient = null;
        try {
            restClient = new RestClient(testSettings);
            EsToken token = restClient.createNewApiToken("test_key");
            UserProvider provider = ObjectUtils.instantiate(testSettings.getSecurityUserProviderClass(), testSettings);
            User userInfo = provider.getUser();
            assertNotNull(userInfo);
            userInfo.addEsToken(token);
            userInfo.doAs(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                    TestSettings innerTestSettings = new TestSettings();
                    InitializationUtils.setUserProviderIfNotSet(innerTestSettings, JdkUserProvider.class, LOG);
                    InitializationUtils.discoverClusterInfo(innerTestSettings, LOG);
                    // Remove the regular auth settings
                    innerTestSettings.asProperties().remove(ConfigurationOptions.ES_NET_HTTP_AUTH_USER);
                    innerTestSettings.asProperties().remove(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS);

                    innerTestSettings.setProperty(ConfigurationOptions.ES_NET_SPNEGO_AUTH_ELASTICSEARCH_PRINCIPAL, "HTTP/build.elastic.co@BUILD.ELASTIC.CO");
                    innerTestSettings.setProperty(ConfigurationOptions.ES_NET_SPNEGO_AUTH_MUTUAL, "true");

                    // Rest Client should use token auth
                    RestClient tokenClient = new RestClient(innerTestSettings);
                    List<NodeInfo> httpDataNodes = tokenClient.getHttpDataNodes();
                    assertThat(httpDataNodes.size(), is(greaterThan(0)));
                    tokenClient.close();
                    return null;
                }
            });
        } finally {
            if (restClient != null) {
                restClient.close();
            }
        }
    }

    @Test
    public void testSpnegoIntoTokenAuth() throws Exception {
        final TestSettings testSettings = new TestSettings();
        Assume.assumeTrue(testSettings.getNetworkHttpAuthUser() != null);
        Assume.assumeTrue(testSettings.getNetworkHttpAuthPass() != null);

        // Setup role mapping
        RestUtils.postData("_xpack/security/role_mapping/kerberos_client_mapping",
                "{\"roles\":[\"superuser\"],\"enabled\":true,\"rules\":{\"field\":{\"username\":\"client@BUILD.ELASTIC.CO\"}}}".getBytes());

        // Configure client settings
        InitializationUtils.setUserProviderIfNotSet(testSettings, JdkUserProvider.class, LOG);
        // Remove the regular auth settings
        testSettings.asProperties().remove(ConfigurationOptions.ES_NET_HTTP_AUTH_USER);
        testSettings.asProperties().remove(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS);
        // Add Kerberos auth settings
        testSettings.setProperty(ConfigurationOptions.ES_SECURITY_AUTHENTICATION, "kerberos");
        testSettings.setProperty(ConfigurationOptions.ES_NET_SPNEGO_AUTH_ELASTICSEARCH_PRINCIPAL, "HTTP/build.elastic.co@BUILD.ELASTIC.CO");

        // Login and perform test
        LoginContext loginCtx = LoginUtil.login("client", "password");
        try {
            Subject.doAs(loginCtx.getSubject(), new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                    // Discover cluster info and get token using SPNEGO
                    InitializationUtils.discoverClusterInfo(testSettings, LOG);
                    RestClient restClient = new RestClient(testSettings);
                    EsToken token = restClient.createNewApiToken("test_key");
                    restClient.close();

                    // Add token to current user
                    UserProvider.create(testSettings).getUser().addEsToken(token);

                    // Remove kerberos information
                    testSettings.asProperties().remove(ConfigurationOptions.ES_SECURITY_AUTHENTICATION);
                    testSettings.asProperties().remove(ConfigurationOptions.ES_NET_SPNEGO_AUTH_ELASTICSEARCH_PRINCIPAL);

                    // Use token to contact ES
                    restClient = new RestClient(testSettings);
                    List<NodeInfo> httpDataNodes = restClient.getHttpDataNodes();
                    assertThat(httpDataNodes.size(), is(greaterThan(0)));

                    // Cancel the token using the token as the auth method
                    restClient.cancelToken(token);
                    restClient.close();
                    return null;
                }
            });
        } finally {
            loginCtx.logout();
            RestUtils.delete("_xpack/security/role_mapping/kerberos_client_mapping");
        }
    }
}
