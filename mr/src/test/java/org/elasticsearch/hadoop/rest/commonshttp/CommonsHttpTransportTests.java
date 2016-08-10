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

import org.apache.commons.httpclient.ConnectTimeoutException;
import org.apache.commons.httpclient.params.HttpConnectionParams;
import org.apache.commons.httpclient.protocol.Protocol;
import org.apache.commons.httpclient.protocol.ProtocolSocketFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

public class CommonsHttpTransportTests {

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
}
