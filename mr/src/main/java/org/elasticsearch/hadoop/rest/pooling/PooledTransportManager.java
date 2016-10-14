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
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.TransportFactory;
import org.elasticsearch.hadoop.util.SettingsUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Central location to request {@link PooledHttpTransportFactory} instances so that the instances can be persisted
 * across multiple tasks on the same executor.
 */
public final class PooledTransportManager {

    private static final Log LOG = LogFactory.getLog(PooledTransportManager.class);

    private PooledTransportManager() {
        //no instance
    }

    private static final ConcurrentMap<String, PooledHttpTransportFactory> poolRegistry
            = new ConcurrentHashMap<String, PooledHttpTransportFactory>();

    public static TransportFactory getTransportFactory(Settings jobSettings) {
        SettingsUtils.ensureJobTransportPoolingKey(jobSettings);
        String jobKey = SettingsUtils.getJobTransportPoolingKey(jobSettings);

        PooledHttpTransportFactory factoryForJob = poolRegistry.get(jobKey);
        if (factoryForJob == null) {
            synchronized (PooledTransportManager.class) {
                factoryForJob = poolRegistry.get(jobKey); // Double Check after getting the lock.
                if (factoryForJob == null) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Creating new PooledTransportFactory for job key [" + jobKey + "]");
                    }
                    factoryForJob = new PooledHttpTransportFactory(jobKey);
                    poolRegistry.put(jobKey, factoryForJob);
                }
            }
        }
        return factoryForJob;
    }

    static {
        Thread cleanup = new Thread(new PoolCleaner());
        cleanup.setDaemon(true);
        cleanup.start();
    }

    private static class PoolCleaner implements Runnable {
        private final Log log = LogFactory.getLog(getClass());
        private final long cleaningInterval = TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);

        @Override
        public void run() {
            log.trace("Started PoolCleaner...");
            try {
                while (true) {
                    log.trace("Waiting");
                    Thread.sleep(cleaningInterval);
                    log.trace("Cleaning...");
                    for (Map.Entry<String, PooledHttpTransportFactory> entry : poolRegistry.entrySet()) {
                        entry.getValue().cleanPools();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
