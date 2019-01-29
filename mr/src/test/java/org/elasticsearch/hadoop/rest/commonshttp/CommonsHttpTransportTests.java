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

package org.elasticsearch.hadoop.rest.commonshttp;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;

import org.apache.commons.httpclient.ConnectTimeoutException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.params.HttpConnectionParams;
import org.apache.commons.httpclient.protocol.Protocol;
import org.apache.commons.httpclient.protocol.ProtocolSocketFactory;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.PropertiesSettings;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.Request;
import org.elasticsearch.hadoop.rest.SimpleRequest;
import org.elasticsearch.hadoop.security.SecureSettings;
import org.elasticsearch.hadoop.util.TestSettings;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.*;

public class CommonsHttpTransportTests {

    /**
     * See <a href="https://tools.ietf.org/html/rfc5735">RFC 5735 Section 3:</a>
     *
     * 192.0.2.0/24 - This block is assigned as "TEST-NET-1" for use in
     * documentation and example code.  It is often used in conjunction with
     * domain names example.com or example.net in vendor and protocol
     * documentation.  As described in [RFC5737], addresses within this
     * block do not legitimately appear on the public Internet and can be
     * used without any coordination with IANA or an Internet registry.  See
     * [RFC1166].
     */
    public static final String TEST_NET_1 = "192.0.2.0";

    private Protocol original;

    @Before
    public void setup() {
        original = Protocol.getProtocol("https");
    }

    @After
    public void reset() {
        Protocol.registerProtocol("https", original);
    }

    @Test
    public void testProtocolReplacement() throws Exception {
        final ProtocolSocketFactory socketFactory = getSocketFactory();
        CommonsHttpTransport.replaceProtocol(socketFactory, "https", 443);

        Protocol protocol = Protocol.getProtocol("https");
        assertThat(protocol, instanceOf(DelegatedProtocol.class));

        DelegatedProtocol delegatedProtocol = (DelegatedProtocol) protocol;
        assertThat(delegatedProtocol.getSocketFactory(), sameInstance(socketFactory));
        assertThat(delegatedProtocol.getOriginal(), sameInstance(original));

        // ensure we do not re-wrap a delegated protocol
        CommonsHttpTransport.replaceProtocol(socketFactory, "https", 443);
        protocol = Protocol.getProtocol("https");
        assertThat(protocol, instanceOf(DelegatedProtocol.class));

        delegatedProtocol = (DelegatedProtocol) protocol;
        assertThat(delegatedProtocol.getSocketFactory(), sameInstance(socketFactory));
        assertThat(delegatedProtocol.getOriginal(), sameInstance(original));
    }

    private ProtocolSocketFactory getSocketFactory() throws Exception {
        final SSLSocketFactory delegate = SSLContext.getDefault().getSocketFactory();
        return new ProtocolSocketFactory() {

            @Override
            public Socket createSocket(String host, int port, InetAddress localAddress, int localPort) throws IOException, UnknownHostException {
                return delegate.createSocket(host, port, localAddress, localPort);
            }

            @Override
            public Socket createSocket(String host, int port, InetAddress localAddress, int localPort, HttpConnectionParams params) throws IOException, UnknownHostException, ConnectTimeoutException {
                return this.createSocket(host, port, localAddress, localPort);
            }

            @Override
            public Socket createSocket(String host, int port) throws IOException, UnknownHostException {
                return delegate.createSocket(host, port);
            }
        };
    }

    @Test
    public void testTimeout() {
        Settings testSettings = new TestSettings();
        testSettings.setProperty(ConfigurationOptions.ES_HTTP_TIMEOUT, "3s");
        String garbageHost = TEST_NET_1 + ":80";
        long maxTime = 3000L + 1000L; // 5s plus some buffer

        long startTime = System.currentTimeMillis();
        try {
            CommonsHttpTransport transport = new CommonsHttpTransport(testSettings, garbageHost);
            transport.execute(new SimpleRequest(Request.Method.GET, null, "/"));
        } catch (Exception e) {
            long endTime = System.currentTimeMillis();
            long took = endTime - startTime;
            assertThat("Connection Timeout not respected", took, Matchers.lessThan(maxTime));
            return;
        }
        fail("Should not be able to connect to TEST_NET_1");
    }

    @Test
    public void testDefaultTransformers() {
        // setup
        CommonsHttpTransport underTest = new CommonsHttpTransport(new PropertiesSettings(), "127.0.0.1");

        // then
        assertNotNull(underTest.getBeforeHttpTransformers());
        assertEquals(0, underTest.getBeforeHttpTransformers().size());
        assertNotNull(underTest.getAfterHttpTransformers());
        assertEquals(0, underTest.getAfterHttpTransformers().size());
    }

    public static class TestHttpBeforeTransformerFactory implements HttpTransformerFactory {

        @Override
        public HttpTransformerExecutionType getExecutionType() {
            return HttpTransformerExecutionType.BEFORE;
        }

        @Override
        public HttpTransformer getHttpTransformer(Settings settings, SecureSettings secureSettings, String hostInfo) {
            return new TestHttpTransformer();
        }
    }

    public static class TestHttpAfterTransformerFactory implements HttpTransformerFactory {

        @Override
        public HttpTransformerExecutionType getExecutionType() {
            return HttpTransformerExecutionType.AFTER;
        }

        @Override
        public HttpTransformer getHttpTransformer(Settings settings, SecureSettings secureSettings, String hostInfo) {
            return new TestHttpTransformer();
        }
    }

    public static class TestHttpAfterTransformerFactoryNoDefaultConstructor implements HttpTransformerFactory {

        public TestHttpAfterTransformerFactoryNoDefaultConstructor(int nothing) {
            // no-op
        }

        @Override
        public HttpTransformerExecutionType getExecutionType() {
            return HttpTransformerExecutionType.AFTER;
        }

        @Override
        public HttpTransformer getHttpTransformer(Settings settings, SecureSettings secureSettings, String hostInfo) {
            return new TestHttpTransformer();
        }
    }

    public static class TestHttpAfterTransformerFactoryNoRequiredInterface {

        public HttpTransformerFactory.HttpTransformerExecutionType getExecutionType() {
            return HttpTransformerFactory.HttpTransformerExecutionType.AFTER;
        }

        public HttpTransformer getHttpTransformer(Settings settings, SecureSettings secureSettings, String hostInfo) {
            return new TestHttpTransformer();
        }
    }

    public static class TestHttpTransformer implements HttpTransformer {

        @Override
        public HttpMethod transform(HttpMethod httpMethod) {
            // no-op
            return httpMethod;
        }
    }

    @Test
    public void testBeforeTransformerConfig() {
        // setup
        PropertiesSettings settings = new PropertiesSettings();
        settings.setHttpTransformerFactories(TestHttpBeforeTransformerFactory.class.getName());
        CommonsHttpTransport underTest = new CommonsHttpTransport(settings, "127.0.0.1");

        // then
        assertNotNull(underTest.getBeforeHttpTransformers());
        assertEquals(1, underTest.getBeforeHttpTransformers().size());
        assertEquals(TestHttpTransformer.class, underTest.getBeforeHttpTransformers().get(0).getClass());
        assertNotNull(underTest.getAfterHttpTransformers());
        assertEquals(0, underTest.getAfterHttpTransformers().size());
    }

    @Test
    public void testAfterTransformerConfig() {
        // setup
        PropertiesSettings settings = new PropertiesSettings();
        settings.setHttpTransformerFactories(TestHttpAfterTransformerFactory.class.getName());
        CommonsHttpTransport underTest = new CommonsHttpTransport(settings, "127.0.0.1");

        // then
        assertNotNull(underTest.getBeforeHttpTransformers());
        assertEquals(0, underTest.getBeforeHttpTransformers().size());
        assertNotNull(underTest.getAfterHttpTransformers());
        assertEquals(1, underTest.getAfterHttpTransformers().size());
        assertEquals(TestHttpTransformer.class, underTest.getAfterHttpTransformers().get(0).getClass());
    }


    @Test
    public void testBothTransformersConfig() {
        // setup
        PropertiesSettings settings = new PropertiesSettings();
        settings.setHttpTransformerFactories(TestHttpBeforeTransformerFactory.class.getName()
                + " , " + TestHttpAfterTransformerFactory.class.getName());
        CommonsHttpTransport underTest = new CommonsHttpTransport(settings, "127.0.0.1");

        // then
        assertNotNull(underTest.getBeforeHttpTransformers());
        assertEquals(1, underTest.getBeforeHttpTransformers().size());
        assertEquals(TestHttpTransformer.class, underTest.getBeforeHttpTransformers().get(0).getClass());
        assertNotNull(underTest.getAfterHttpTransformers());
        assertEquals(1, underTest.getAfterHttpTransformers().size());
        assertEquals(TestHttpTransformer.class, underTest.getAfterHttpTransformers().get(0).getClass());
    }

    @Test(expected = IllegalStateException.class)
    public void testWrongFactoryClassName() {
        PropertiesSettings settings = new PropertiesSettings();
        settings.setHttpTransformerFactories(TestHttpBeforeTransformerFactory.class.getName() + "123"); // non-existing class name
        new CommonsHttpTransport(settings, "127.0.0.1");
    }

    @Test(expected = IllegalStateException.class)
    public void testFactoryNoDefaultConstructor() {
        PropertiesSettings settings = new PropertiesSettings();
        settings.setHttpTransformerFactories(TestHttpAfterTransformerFactoryNoDefaultConstructor.class.getName());
        new CommonsHttpTransport(settings, "127.0.0.1");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFactoryNoRequiredInterface() {
        PropertiesSettings settings = new PropertiesSettings();
        settings.setHttpTransformerFactories(TestHttpAfterTransformerFactoryNoRequiredInterface.class.getName());
        new CommonsHttpTransport(settings, "127.0.0.1");
    }
}
