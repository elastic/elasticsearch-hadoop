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
import java.lang.reflect.Method;
import java.net.Socket;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpConnection;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.SimpleHttpConnectionManager;
import org.apache.commons.httpclient.URI;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.EntityEnclosingMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.HeadMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.params.HttpClientParams;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.DelegatingInputStream;
import org.elasticsearch.hadoop.rest.Request;
import org.elasticsearch.hadoop.rest.Response;
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


    private static class ResponseInputStream extends DelegatingInputStream {

        private final HttpMethod method;

        public ResponseInputStream(HttpMethod http) throws IOException {
            super(http.getResponseBodyAsStream());
            this.method = http;
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
    }

    public CommonsHttpTransport(Settings settings, String host) {
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
        client = new HttpClient(params, new SocketTrackingConnectionManager());

        HostConfiguration hostConfig = new HostConfiguration();

        try {
            hostConfig.setHost(new URI(prefixUri(host), false));
        } catch (IOException ex) {
            throw new IllegalArgumentException("Invalid target URI " + host, ex);
        }
        client.setHostConfiguration(hostConfig);

        HttpConnectionManagerParams connectionParams = client.getHttpConnectionManager().getParams();
        // make sure to disable Nagle's protocol
        connectionParams.setTcpNoDelay(true);
    }

    @Override
    public Response execute(Request request) throws IOException {
        HttpMethod http = null;

        switch (request.method()) {
        case DELETE:
            http = new DeleteMethod();
            break;
        case HEAD:
            http = new HeadMethod();
            break;
        case GET:
            http = new GetMethod();
            break;
        case POST:
            http = new PostMethod();
            break;
        case PUT:
            http = new PutMethod();
            break;

        default:
            throw new IllegalArgumentException("Unknown method");
        }

        CharSequence uri = request.uri();
        if (StringUtils.hasText(uri)) {
            http.setURI(new URI(prefixUri(uri.toString()), false));
        }
        // NB: initialize the path _after_ the URI otherwise the path gets reset to /
        http.setPath(prefixPath(request.path().toString()));

        CharSequence params = request.params();
        if (StringUtils.hasText(params)) {
            http.setQueryString(params.toString());
        }

        ByteSequence ba = request.body();
        if (ba != null && ba.length() > 0) {
            EntityEnclosingMethod entityMethod = (EntityEnclosingMethod) http;
            entityMethod.setRequestEntity(new BytesArrayRequestEntity(ba));
            entityMethod.setContentChunked(false);
        }

        // when tracing, log everything
        if (log.isTraceEnabled()) {
            log.trace(String.format("Tx [%s]@[%s][%s] w/ payload [%s]", request.method().name(), request.uri(), request.path(), request.body()));
        }

        client.executeMethod(http);

        if (log.isTraceEnabled()) {
            Socket sk = ReflectionUtils.invoke(GET_SOCKET, conn, (Object[]) null);
            String addr = sk.getLocalAddress().getHostAddress();
            log.trace(String.format("Rx @[%s] [%s-%s] [%s]", addr, http.getStatusCode(), HttpStatus.getStatusText(http.getStatusCode()), http.getResponseBodyAsString()));
        }

        return new SimpleResponse(http.getStatusCode(), new ResponseInputStream(http), request.uri());
    }

    @Override
    public void close() {
        HttpConnectionManager manager = client.getHttpConnectionManager();
        if (manager instanceof SimpleHttpConnectionManager) {
            try {
                ((SimpleHttpConnectionManager) manager).closeIdleConnections(0);
            } catch (NullPointerException npe) {
                // ignore
            } catch (Exception ex) {
                // log - not much else to do
                log.warn("Exception closing underlying HTTP manager", ex);
            }
        }
    }

    private static String prefixUri(String uri) {
        return uri.contains("://") ? uri : "http://" + uri;
    }

    private static String prefixPath(String string) {
        return string.startsWith("/") ? string : "/" + string;
    }

    @Override
    public Stats stats() {
        return stats;
    }
}