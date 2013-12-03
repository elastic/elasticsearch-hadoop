/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticsearch.hadoop.rest;

import java.io.IOException;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.SimpleHttpConnectionManager;
import org.apache.commons.httpclient.URI;
import org.apache.commons.httpclient.methods.DeleteMethod;
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
import org.elasticsearch.hadoop.util.StringUtils;

/**
 * Transport implemented on top of Commons Http. Provides transport retries.
 */
public class CommonsHttpTransport implements Transport {

    private static Log log = LogFactory.getLog(CommonsHttpTransport.class);
    private final HttpClient client;

    public CommonsHttpTransport(Settings settings) {
        HttpClientParams params = new HttpClientParams();
        params.setParameter(HttpMethodParams.RETRY_HANDLER,
                new DefaultHttpMethodRetryHandler(settings.getHttpRetries(), false));
        params.setConnectionManagerTimeout(settings.getHttpTimeout());
        params.setSoTimeout((int) settings.getHttpTimeout());
        client = new HttpClient(params);

        HostConfiguration hostConfig = new HostConfiguration();
        String targetUri = settings.getTargetUri();
        try {
            hostConfig.setHost(new URI(targetUri, false));
        } catch (IOException ex) {
            throw new IllegalArgumentException("Invalid target URI " + targetUri, ex);
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
            break;
        }

        http.setPath(request.path().toString());
        CharSequence uri = request.uri();
        if (StringUtils.hasText(uri)) {
            http.setURI(new URI(uri.toString(), false));
        }
        CharSequence params = request.params();
        if (StringUtils.hasText(params)) {
            http.setQueryString(params.toString());
        }

        try {
            client.executeMethod(http);
            return new SimpleResponse(http.getStatusCode(), http.getResponseBody(), request.uri());
        } finally {
            http.releaseConnection();
        }
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
}