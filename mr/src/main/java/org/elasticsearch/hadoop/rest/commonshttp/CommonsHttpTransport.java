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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.Socket;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpConnection;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpState;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.SimpleHttpConnectionManager;
import org.apache.commons.httpclient.URI;
import org.apache.commons.httpclient.URIException;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.methods.EntityEnclosingMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.HeadMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.params.HttpClientParams;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.httpclient.protocol.Protocol;
import org.apache.commons.httpclient.protocol.ProtocolSocketFactory;
import org.apache.commons.httpclient.protocol.SecureProtocolSocketFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.DelegatingInputStream;
import org.elasticsearch.hadoop.rest.EsHadoopTransportException;
import org.elasticsearch.hadoop.rest.Request;
import org.elasticsearch.hadoop.rest.Response;
import org.elasticsearch.hadoop.rest.ReusableInputStream;
import org.elasticsearch.hadoop.rest.SimpleResponse;
import org.elasticsearch.hadoop.rest.Transport;
import org.elasticsearch.hadoop.rest.stats.Stats;
import org.elasticsearch.hadoop.rest.stats.StatsAware;
import org.elasticsearch.hadoop.util.ByteSequence;
import org.elasticsearch.hadoop.util.ReflectionUtils;
import org.elasticsearch.hadoop.util.StringUtils;

/**
 * Transport implemented on top of Commons Http. Provides transport retries.
 */
public class CommonsHttpTransport implements Transport, StatsAware {

    private static Log log = LogFactory.getLog(CommonsHttpTransport.class);
    private static final Method GET_SOCKET;

    static {
        GET_SOCKET = ReflectionUtils.findMethod(HttpConnection.class, "getSocket", (Class[]) null);
        ReflectionUtils.makeAccessible(GET_SOCKET);
    }


    private final HttpClient client;
    private final Stats stats = new Stats();
    private HttpConnection conn;
    private String proxyInfo = "";
    private final String httpInfo;
    private final Settings settings;

    private static class ResponseInputStream extends DelegatingInputStream implements ReusableInputStream {

        private final HttpMethod method;
        private final boolean reusable;

        public ResponseInputStream(HttpMethod http) throws IOException {
            super(http.getResponseBodyAsStream());
            this.method = http;
            reusable = (delegate() instanceof ByteArrayInputStream);
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return super.equals(obj);
        }

        @Override
        public InputStream copy() {
            try {
                return (reusable ? method.getResponseBodyAsStream() : null);
            } catch (IOException ex) {
                throw new EsHadoopIllegalStateException(ex);
            }
        }

        @Override
        public void close() throws IOException {
            if (!isNull()) {
                try {
                    super.close();
                } catch (IOException e) {
                    // silently ignore
                }
            }
            method.releaseConnection();
        }
    }

    private class SocketTrackingConnectionManager extends SimpleHttpConnectionManager {

        @Override
        public HttpConnection getConnectionWithTimeout(HostConfiguration hostConfiguration, long timeout) {
            conn = super.getConnectionWithTimeout(hostConfiguration, timeout);
            return conn;
        }

        public void close() {
            if (httpConnection != null) {
                if (httpConnection.isOpen()) {
                    releaseConnection(httpConnection);
                }

                httpConnection.close();
            }

            httpConnection = null;
            conn = null;
        }
    }

    public CommonsHttpTransport(Settings settings, String host) {
        this.settings = settings;
        httpInfo = host;

        HttpClientParams params = new HttpClientParams();
        params.setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(
                settings.getHttpRetries(), false) {

            @Override
            public boolean retryMethod(HttpMethod method, IOException exception, int executionCount) {
                if (super.retryMethod(method, exception, executionCount)) {
                    stats.netRetries++;
                    return true;
                }
                return false;
            }
        });

        params.setConnectionManagerTimeout(settings.getHttpTimeout());
        params.setSoTimeout((int) settings.getHttpTimeout());
        HostConfiguration hostConfig = new HostConfiguration();

        hostConfig = setupSSLIfNeeded(settings, hostConfig);
        hostConfig = setupSocksProxy(settings, hostConfig);
        Object[] authSettings = setupHttpProxy(settings, hostConfig);
        hostConfig = (HostConfiguration) authSettings[0];

        try {
            hostConfig.setHost(new URI(escapeUri(host, settings.getNetworkSSLEnabled()), false));
        } catch (IOException ex) {
            throw new EsHadoopTransportException("Invalid target URI " + host, ex);
        }
        client = new HttpClient(params, new SocketTrackingConnectionManager());
        client.setHostConfiguration(hostConfig);

        addHttpAuth(settings, authSettings);
        completeAuth(authSettings);

        HttpConnectionManagerParams connectionParams = client.getHttpConnectionManager().getParams();
        // make sure to disable Nagle's protocol
        connectionParams.setTcpNoDelay(true);

        if (log.isTraceEnabled()) {
            log.trace("Opening HTTP transport to " + httpInfo);
        }
    }

    private HostConfiguration setupSSLIfNeeded(Settings settings, HostConfiguration hostConfig) {
        if (!settings.getNetworkSSLEnabled()) {
            return hostConfig;
        }

        // we actually have a socks proxy, let's start the setup
        if (log.isDebugEnabled()) {
            log.debug("SSL Connection enabled");
        }

        //
        // switch protocol
        // due to how HttpCommons work internally this dance is best to be kept as is
        //
        String schema = "https";
        int port = 443;
        SecureProtocolSocketFactory sslFactory = new SSLSocketFactory(settings);

        replaceProtocol(hostConfig, sslFactory, schema, port);

        return hostConfig;
    }

    private void addHttpAuth(Settings settings, Object[] authSettings) {
        if (StringUtils.hasText(settings.getNetworkHttpAuthUser())) {
            HttpState state = (authSettings[1] != null ? (HttpState) authSettings[1] : new HttpState());
            authSettings[1] = state;
            state.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(settings.getNetworkHttpAuthUser(), settings.getNetworkHttpAuthPass()));
            if (log.isDebugEnabled()) {
                log.info("Using detected HTTP Auth credentials...");
            }
        }
    }

    private void completeAuth(Object[] authSettings) {
        if (authSettings[1] != null) {
            client.setState((HttpState) authSettings[1]);
            client.getParams().setAuthenticationPreemptive(true);
        }
    }

    private Object[] setupHttpProxy(Settings settings, HostConfiguration hostConfig) {
        // return HostConfiguration + HttpState
        Object[] results = new Object[2];
        results[0] = hostConfig;
        // set proxy settings
        String proxyHost = null;
        int proxyPort = -1;
        if (settings.getNetworkHttpUseSystemProperties()) {
            proxyHost = System.getProperty("http.proxyHost");
            proxyPort = Integer.getInteger("http.proxyPort", -1);
        }
        if (StringUtils.hasText(settings.getNetworkProxyHttpHost())) {
            proxyHost = settings.getNetworkProxyHttpHost();
        }
        if (settings.getNetworkProxyHttpPort() > 0) {
            proxyPort = settings.getNetworkProxyHttpPort();
        }

        if (StringUtils.hasText(proxyHost)) {
            hostConfig.setProxy(proxyHost, proxyPort);
            proxyInfo = proxyInfo.concat(String.format("[HTTP proxy %s:%s]", proxyHost, proxyPort));

            // client is not yet initialized so postpone state
            if (StringUtils.hasText(settings.getNetworkProxyHttpUser())) {
                if (!StringUtils.hasText(settings.getNetworkProxyHttpPass())) {
                    log.warn(String.format("HTTP proxy user specified but no/empty password defined - double check the [%s] property", ConfigurationOptions.ES_NET_PROXY_HTTP_PASS));

                }
                HttpState state = new HttpState();
                state.setProxyCredentials(AuthScope.ANY, new UsernamePasswordCredentials(settings.getNetworkProxyHttpUser(), settings.getNetworkProxyHttpPass()));
                // client is not yet initialized so simply save the object for later
                results[1] = state;
            }

            if (log.isDebugEnabled()) {
                if (StringUtils.hasText(settings.getNetworkProxyHttpUser())) {
                    log.debug(String.format("Using authenticated HTTP proxy [%s:%s]", proxyHost, proxyPort));
                }
                else {
                    log.debug(String.format("Using HTTP proxy [%s:%s]", proxyHost, proxyPort));
                }
            }
        }

        return results;
    }

    private HostConfiguration setupSocksProxy(Settings settings, HostConfiguration hostConfig) {
        // set proxy settings
        String proxyHost = null;
        int proxyPort = -1;
        String proxyUser = null;
        String proxyPass = null;

        if (settings.getNetworkHttpUseSystemProperties()) {
            proxyHost = System.getProperty("socksProxyHost");
            proxyPort = Integer.getInteger("socksProxyPort", -1);
            proxyUser = System.getProperty("java.net.socks.username");
            proxyPass = System.getProperty("java.net.socks.password");
        }
        if (StringUtils.hasText(settings.getNetworkProxySocksHost())) {
            proxyHost = settings.getNetworkProxySocksHost();
        }
        if (settings.getNetworkProxySocksPort() > 0) {
            proxyPort = settings.getNetworkProxySocksPort();
        }
        if (StringUtils.hasText(settings.getNetworkProxySocksUser())) {
            proxyUser = settings.getNetworkProxySocksUser();
        }
        if (StringUtils.hasText(settings.getNetworkProxySocksPass())) {
            proxyPass = settings.getNetworkProxySocksPass();
        }

        // we actually have a socks proxy, let's start the setup
        if (StringUtils.hasText(proxyHost)) {
            proxyInfo = proxyInfo.concat(String.format("[SOCKS proxy %s:%s]", proxyHost, proxyPort));

            if (!StringUtils.hasText(proxyUser)) {
                log.warn(String.format(
                        "SOCKS proxy user specified but no/empty password defined - double check the [%s] property",
                        ConfigurationOptions.ES_NET_PROXY_SOCKS_PASS));
            }

            if (log.isDebugEnabled()) {
                if (StringUtils.hasText(proxyUser)) {
                    log.debug(String.format("Using authenticated SOCKS proxy [%s:%s]", proxyHost, proxyPort));
                }
                else {
                    log.debug(String.format("Using SOCKS proxy [%s:%s]", proxyHost, proxyPort));
                }
            }

            //
            // switch protocol
            // due to how HttpCommons work internally this dance is best to be kept as is
            //
            String schema = settings.getNetworkSSLEnabled() ? "https" : "http";
            int port = settings.getNetworkSSLEnabled() ? 443 : 80;
            SocksSocketFactory socketFactory = new SocksSocketFactory(proxyHost, proxyPort, proxyUser, proxyPass);
            replaceProtocol(hostConfig, socketFactory, schema, port);
        }

        return hostConfig;
    }

    private void replaceProtocol(HostConfiguration hostConfig, ProtocolSocketFactory socketFactory, String schema, int defaultPort) {
        //
        // switch protocol
        // due to how HttpCommons work internally this dance is best to be kept as is
        //

        // NB: not really needed (see below that the protocol is reseted) but in place just in case
        hostConfig = new ProtocolAwareHostConfiguration(hostConfig);
        Protocol directHttp = Protocol.getProtocol(schema);
        Protocol proxiedHttp = new DelegatedProtocol(socketFactory, directHttp, schema, defaultPort);
        // NB: register the new protocol since when using absolute URIs, HttpClient#executeMethod will override the configuration (#387)
        // NB: hence why the original/direct http protocol is saved - as otherwise the connection is not closed since it is considered different
        // NB: (as the protocol identities don't match)

        // this is not really needed since it's being replaced later on
        // hostConfig.setHost(proxyHost, proxyPort, proxiedHttp);
        Protocol.registerProtocol(schema, proxiedHttp);

        // end dance
    }

    @Override
    public Response execute(Request request) throws IOException {
        HttpMethod http = null;

        switch (request.method()) {
        case DELETE:
            http = new DeleteMethodWithBody();
            break;
        case HEAD:
            http = new HeadMethod();
            break;
        case GET:
            http = (request.body() == null ? new GetMethod() : new GetMethodWithBody());
            break;
        case POST:
            http = new PostMethod();
            break;
        case PUT:
            http = new PutMethod();
            break;

        default:
            throw new EsHadoopTransportException("Unknown request method " + request.method());
        }

        CharSequence uri = request.uri();
        if (StringUtils.hasText(uri)) {
            http.setURI(new URI(escapeUri(uri.toString(), settings.getNetworkSSLEnabled()), false));
        }
        // NB: initialize the path _after_ the URI otherwise the path gets reset to /
        http.setPath(prefixPath(request.path().toString()));

        try {
            // validate new URI
            uri = http.getURI().toString();
        } catch (URIException uriex) {
            throw new EsHadoopTransportException("Invalid target URI " + request, uriex);
        }

        CharSequence params = request.params();
        if (StringUtils.hasText(params)) {
            http.setQueryString(params.toString());
        }

        ByteSequence ba = request.body();
        if (ba != null && ba.length() > 0) {
            if (!(http instanceof EntityEnclosingMethod)) {
                throw new IllegalStateException(String.format("Method %s cannot contain body - implementation bug", request.method().name()));
            }
            EntityEnclosingMethod entityMethod = (EntityEnclosingMethod) http;
            entityMethod.setRequestEntity(new BytesArrayRequestEntity(ba));
            entityMethod.setContentChunked(false);
        }

        // when tracing, log everything
        if (log.isTraceEnabled()) {
            log.trace(String.format("Tx %s[%s]@[%s][%s] w/ payload [%s]", proxyInfo, request.method().name(), httpInfo, request.path(), request.body()));
        }

        long start = System.currentTimeMillis();
        try {
            client.executeMethod(http);
        } finally {
            stats.netTotalTime += (System.currentTimeMillis() - start);
        }

        if (log.isTraceEnabled()) {
            Socket sk = ReflectionUtils.invoke(GET_SOCKET, conn, (Object[]) null);
            String addr = sk.getLocalAddress().getHostAddress();
            log.trace(String.format("Rx %s@[%s] [%s-%s] [%s]", proxyInfo, addr, http.getStatusCode(), HttpStatus.getStatusText(http.getStatusCode()), http.getResponseBodyAsString()));
        }

        // the request URI is not set (since it is retried across hosts), so use the http info instead for source
        return new SimpleResponse(http.getStatusCode(), new ResponseInputStream(http), httpInfo);
    }

    @Override
    public void close() {
        if (log.isTraceEnabled()) {
            log.trace("Closing HTTP transport to " + httpInfo);
        }

        HttpConnectionManager manager = client.getHttpConnectionManager();
        if (manager instanceof SocketTrackingConnectionManager) {
            try {
                ((SocketTrackingConnectionManager) manager).close();
            } catch (NullPointerException npe) {
                // ignore
            } catch (Exception ex) {
                // log - not much else to do
                log.warn("Exception closing underlying HTTP manager", ex);
            }
        }
    }

    private static String escapeUri(String uri, boolean ssl) {
        // escape the uri right away
        String escaped = StringUtils.encodeUri(uri);
        return escaped.contains("://") ? escaped : (ssl ? "https://" : "http://") + escaped;
    }

    private static String prefixPath(String string) {
        return string.startsWith("/") ? string : "/" + string;
    }

    @Override
    public Stats stats() {
        return stats;
    }
}