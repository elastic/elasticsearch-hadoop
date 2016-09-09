package org.elasticsearch.hadoop.rest.pooling;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.EsHadoopException;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.Transport;
import org.elasticsearch.hadoop.rest.TransportFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.elasticsearch.hadoop.cfg.InternalConfigurationOptions.INTERNAL_TRANSPORT_POOLING_KEY;

/**
 * Creates HTTP Transports that are backed by a pool of Transport objects for reuse.
 */
class PooledHttpTransportFactory implements TransportFactory {

    private final Log log = LogFactory.getLog(this.getClass());
    private final ConcurrentMap<String, TransportPool> hostPools = new ConcurrentHashMap<String, TransportPool>();
    private final String jobKey;

    PooledHttpTransportFactory(String jobKey) {
        this.jobKey = jobKey;
    }

    @Override
    public Transport create(Settings settings, String hostInfo) {
        // Make sure that the caller's Settings has the correct job pool key.
        if (!jobKey.equals(settings.getProperty(INTERNAL_TRANSPORT_POOLING_KEY))) {
            throw new EsHadoopIllegalArgumentException("Settings object passed does not have the same `"+INTERNAL_TRANSPORT_POOLING_KEY+"` property as when this pool was created. This could be a different job incorrectly polluting the TransportPool. Bailing out...");
        }

        TransportPool pool = hostPools.get(hostInfo);
        if (pool == null) {
            synchronized (this) {
                pool = hostPools.get(hostInfo); // Check again in case it was added while waiting for the lock
                if (pool == null) {
                    pool = new TransportPool(jobKey, hostInfo, settings);
                    hostPools.put(hostInfo, pool);
                    if (log.isDebugEnabled()) {
                        log.debug("Creating new TransportPool for job ["+jobKey+"] for host ["+hostInfo+"]");
                    }
                }
            }
        }

        if (!pool.getJobPoolingKey().equals(jobKey)) {
            throw new EsHadoopIllegalArgumentException("PooledTransportFactory found a pool with a different owner than this job. This could be a different job incorrectly polluting the TransportPool. Bailing out...");
        }

        Transport borrowed;
        try {
            borrowed = pool.borrowTransport();
        } catch (Exception e) {
            throw new EsHadoopException(String.format("Could not get a Transport from the Transport Pool for host [%s]", hostInfo));
        }
        return borrowed;
    }
}
