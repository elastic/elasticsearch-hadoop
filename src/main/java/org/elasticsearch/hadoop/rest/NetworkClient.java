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
package org.elasticsearch.hadoop.rest;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.commonshttp.CommonsHttpTransport;
import org.elasticsearch.hadoop.rest.stats.Stats;
import org.elasticsearch.hadoop.rest.stats.StatsAware;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.ByteSequence;


public class NetworkClient implements StatsAware {

    private static Log log = LogFactory.getLog(NetworkClient.class);

    private final Settings settings;
    private final List<String> nodes;

    private Transport currentTransport;
    private String currentUri;
    private int nextClient = 0;

    private final Stats stats = new Stats();

    public NetworkClient(Settings settings, List<String> hostURIs) {
        this.settings = settings.copy();
        this.nodes = hostURIs;

        selectNextNode();

        Assert.notNull(currentTransport, "no node information provided");
    }

    private boolean selectNextNode() {
        if (nextClient >= nodes.size()) {
            return false;
        }

        if (currentTransport != null) {
            stats.nodeRetries++;
        }

        currentUri = nodes.get(nextClient++);
        close();

        settings.setHosts(currentUri);
        currentTransport = new CommonsHttpTransport(settings, currentUri);
        return true;
    }

    public Response execute(Request request) throws EsHadoopProtocolException {
        Response response = null;

        SimpleRequest routedRequest = new SimpleRequest(request.method(), currentUri, request.path(), request.params(), request.body());

        boolean newNode;
        do {
            newNode = false;
            try {
                response = currentTransport.execute(routedRequest);
                ByteSequence body = routedRequest.body();
                if (body != null) {
                    stats.bytesWritten += body.length();
                }
            } catch (Exception ex) {
                if (log.isTraceEnabled()) {
                    log.trace(String.format("Caught exception while performing request [%s][%s] - falling back to the next node in line...", currentUri, request.path()), ex);
                }

                String failed = currentUri;
                newNode = selectNextNode();

                log.error(String.format("Node [%s] failed; " + (newNode ? "selected next node [" +  currentUri + "]" : "no other nodes left - aborting..."), failed));

                if (!newNode) {
                    throw new EsHadoopProtocolException("Connection error (check network and/or proxy settings) - out of nodes and retries", ex);
                }
            }
        } while (newNode);

        return response;
    }

    void close() {
        if (currentTransport != null) {
            if (currentTransport instanceof StatsAware) {
                stats.aggregate(((StatsAware) currentTransport).stats());
            }

            currentTransport.close();
        }
    }

    @Override
    public Stats stats() {
        return stats;
    }
}