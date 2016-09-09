package org.elasticsearch.hadoop.rest.pooling;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.cfg.InternalConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.TransportFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
        String jobKey = jobSettings.getProperty(InternalConfigurationOptions.INTERNAL_TRANSPORT_POOLING_KEY);

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
}
