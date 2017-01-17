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
package org.elasticsearch.hadoop.rest.pooling;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.Request;
import org.elasticsearch.hadoop.rest.Response;
import org.elasticsearch.hadoop.rest.SimpleRequest;
import org.elasticsearch.hadoop.rest.Transport;
import org.elasticsearch.hadoop.rest.commonshttp.CommonsHttpTransport;
import org.elasticsearch.hadoop.rest.stats.Stats;
import org.elasticsearch.hadoop.util.unit.TimeValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.hadoop.rest.Request.Method.GET;

/**
 * A basic connection pool meant for allocating {@link Transport} objects.
 * This only supports pooling of the {@link CommonsHttpTransport} object at this time.
 */
final class TransportPool {

    private final Log log = LogFactory.getLog(this.getClass());

    private final Settings transportSettings;
    private final String hostName;
    private final String jobPoolingKey;
    private final TimeValue idleTransportTimeout;

    private final SimpleRequest validationRequest = new SimpleRequest(/*method:*/GET, /*uri:*/null, /*path:*/"");

    private final Map<PooledTransport, Long> idle;
    private final Map<PooledTransport, Long> leased;

    /**
     * @param jobPoolingKey Unique key for all pooled connections for this job
     * @param hostName Host name to pool transports for
     * @param transportSettings Settings to use for this pool and for new transports
     */
    TransportPool(String jobPoolingKey, String hostName, Settings transportSettings) {
        this.jobPoolingKey = jobPoolingKey;
        this.hostName = hostName;
        this.transportSettings = transportSettings;
        this.leased = new HashMap<PooledTransport, Long>();
        this.idle = new HashMap<PooledTransport, Long>();

        this.idleTransportTimeout = transportSettings.getTransportPoolingExpirationTimeout();
    }

    /**
     * @return This pool's assigned job id
     */
    String getJobPoolingKey() {
        return jobPoolingKey;
    }

    /**
     * @return a new Transport for use in the pool.
     */
    private PooledTransport create() {
        if (log.isDebugEnabled()) {
            log.debug("Creating new pooled CommonsHttpTransport for host ["+hostName+"] belonging to job ["+jobPoolingKey+"]");
        }
        return new PooledCommonsHttpTransport(transportSettings, hostName);
    }

    /**
     * Used to validate an idle pooled transport is still good for consumption.
     * @param transport to test
     * @return if the transport succeeded the validation or not
     */
    private boolean validate(PooledTransport transport) {
        try {
            Response response = transport.execute(validationRequest);
            return response.hasSucceeded();
        } catch (IOException ioe) {
            log.warn("Could not validate pooled connection on lease. Releasing pooled connection and trying again...", ioe);
            return false;
        }
    }

    /**
     * Takes the steps required to close the given Transport object
     * @param transport to be closed
     */
    private void release(PooledTransport transport) {
        transport.close();
    }

    /**
     * Borrows a Transport from this pool. If there are no pooled Transports available, a new one is created.
     * @return A Transport backed by a pooled resource
     */
    synchronized Transport borrowTransport() {
        long now = System.currentTimeMillis();

        List<PooledTransport> garbageTransports = new ArrayList<PooledTransport>();
        PooledTransport candidate = null;

        // Grab a transport
        for (Map.Entry<PooledTransport, Long> entry : idle.entrySet()) {
            PooledTransport transport = entry.getKey();
            if (validate(transport)) {
                candidate = transport;
                break;
            } else {
                garbageTransports.add(transport);
            }
        }

        // Remove any dead connections found
        for (PooledTransport transport : garbageTransports) {
            idle.remove(transport);
            release(transport);
        }

        // Create the connection if we didn't find any, remove it from the pool if we did.
        if (candidate == null) {
            candidate = create();
        } else {
            idle.remove(candidate);
        }

        // Lease.
        leased.put(candidate, now);
        return new LeasedTransport(candidate, this);
    }

    /**
     * Returns a transport to the pool.
     * @param returning Transport to be cleaned and returned to the pool.
     */
    private synchronized void returnTransport(Transport returning) {
        long now = System.currentTimeMillis();
        PooledTransport unwrapped;

        // check if they're returning a leased transport
        if (returning instanceof LeasedTransport) {
            LeasedTransport leasedTransport = (LeasedTransport) returning;
            unwrapped = leasedTransport.delegate;
        } else if (returning instanceof PooledTransport) {
            unwrapped = (PooledTransport) returning;
        } else {
            throw new EsHadoopIllegalStateException("Cannot return a non-poolable Transport to the pool");
        }

        // make sure that this is even a leased transport before returning it
        if (leased.containsKey(unwrapped)) {
            leased.remove(unwrapped);
            idle.put(unwrapped, now);
        } else {
            throw new EsHadoopIllegalStateException("Cannot return a Transport object to a pool that was not sourced from the pool");
        }
    }

    /**
     * Cleans the pool by removing any resources that have been idle for longer than the configured transport pool idle time.
     * @return how many connections in the pool still exist (idle AND leased).
     */
    synchronized int removeOldConnections() {
        long now = System.currentTimeMillis();
        long expirationTime = now - idleTransportTimeout.millis();

        List<PooledTransport> removeFromIdle = new ArrayList<PooledTransport>();
        for (Map.Entry<PooledTransport, Long> idleEntry : idle.entrySet()) {
            long lastUsed = idleEntry.getValue();
            if (lastUsed < expirationTime) {
                PooledTransport removed = idleEntry.getKey();
                if (log.isTraceEnabled()) {
                    log.trace("Expiring idle transport for job [" + jobPoolingKey + "], transport: ["
                            + removed.toString() + "]. Last used [" + new TimeValue(now-lastUsed) + "] ago. Expired ["
                            + idleTransportTimeout + "] ago.");
                }
                release(removed);
                removeFromIdle.add(removed);
            }
        }

        for (PooledTransport toRemove : removeFromIdle) {
            idle.remove(toRemove);
        }

        return idle.size() + leased.size();
    }

    /**
     * A delegating transport object that returns it's delegate to a
     * Transport Pool instead of closing it when close is called.
     */
    private final class LeasedTransport implements Transport {

        private final PooledTransport delegate;
        private final TransportPool lender;
        private boolean open = true;
        private Stats finalResults;

        LeasedTransport(PooledTransport delegate, TransportPool lender) {
            this.delegate = delegate;
            this.lender = lender;
        }

        @Override
        public Response execute(Request request) throws IOException {
            if (!open) {
                throw new EsHadoopIllegalStateException("Calling execute on a closed Transport object");
            }
            return delegate.execute(request);
        }

        @Override
        public void close() {
            if (open) {
                open = false;
                finalResults = delegate.stats(); // capture stats before cleaning the transport
                delegate.clean();
                lender.returnTransport(delegate);
            }
        }

        @Override
        public Stats stats() {
            if (!open) {
                return finalResults;
            } else {
                return delegate.stats();
            }
        }
    }

    /**
     * Any transport that can be cleaned for reuse via a connection pool.
     */
    private interface PooledTransport extends Transport {
        void clean();
    }

    /**
     * A subclass of {@link CommonsHttpTransport} that allows us to
     * clean up stuff like the 'stats' member before returning the
     * transport to the pool.
     */
    private final class PooledCommonsHttpTransport extends CommonsHttpTransport implements PooledTransport {
        private final String loggingHostInformation;

        PooledCommonsHttpTransport(Settings settings, String host) {
            super(settings, host);
            this.loggingHostInformation = host;
        }

        @Override
        public void clean() {
            this.stats = new Stats();
        }

        @Override
        public String toString() {
            return "PooledCommonsHttpTransport{'" + loggingHostInformation + "'}";
        }
    }
}
