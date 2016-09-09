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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.hadoop.rest.Request.Method.GET;

/**
 * A basic connection pool meant for allocating {@link Transport} objects.
 * This only supports pooling of the {@link CommonsHttpTransport} object at this time.
 */
class TransportPool {

    private final Log log = LogFactory.getLog(this.getClass());

    private final Settings transportSettings;
    private final String hostName;
    private final String jobPoolingKey;

    private final SimpleRequest validationRequest = new SimpleRequest(/*method:*/GET, /*uri:*/null, /*path:*/"", /*params:*/"");

    private final Map<PooledTransport, Long> idle;
    private final Map<PooledTransport, Long> leased;

    private volatile long lastUsed = 0L;

    TransportPool(String jobPoolingKey, String hostName, Settings transportSettings) {
        this.jobPoolingKey = jobPoolingKey;
        this.hostName = hostName;
        this.transportSettings = transportSettings;
        this.leased = new HashMap<PooledTransport, Long>();
        this.idle = new HashMap<PooledTransport, Long>();
    }

    String getJobPoolingKey() {
        return jobPoolingKey;
    }

    private PooledTransport create() {
        if (log.isDebugEnabled()) {
            log.debug("Creating new pooled CommonsHttpTransport for host ["+hostName+"] belonging to job ["+jobPoolingKey+"]");
        }
        return new PooledCommonsHttpTransport(transportSettings, hostName);
    }

    private boolean validate(PooledTransport transport) {
        // TODO: better validation logic (logging errors and other things)
        try {
            Response response = transport.execute(validationRequest);
            return response.hasSucceeded();
        } catch (IOException ioe) {
            return false;
        }
    }

    private void release(PooledTransport transport) {
        transport.close();
    }

    synchronized Transport borrowTransport() {
        long now = System.currentTimeMillis();
        for (Map.Entry<PooledTransport, Long> entry : idle.entrySet()) {
            PooledTransport transport = entry.getKey();
            if (validate(transport)) {
                idle.remove(transport);
                leased.put(transport, now);
                lastUsed = now;
                return new LeasedTransport(transport, this);
            } else {
                idle.remove(transport);
                release(transport);
            }
        }
        PooledTransport transport = create();
        leased.put(transport, now);
        lastUsed = now;
        return new LeasedTransport(transport, this);
    }

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
            lastUsed = now;
        } else {
            throw new EsHadoopIllegalStateException("Cannot return a Transport object to a pool that was not sourced from the pool");
        }
    }

    public long lastUsed() {
        return lastUsed;
    }

    public synchronized int leaseCount() {
        return leased.size();
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
     * clean up certain things about the object before returning it
     * to the pool.
     */
    private final class PooledCommonsHttpTransport extends CommonsHttpTransport implements PooledTransport {
        PooledCommonsHttpTransport(Settings settings, String host) {
            super(settings, host);
        }

        @Override
        public void clean() {
            this.stats = new Stats();
        }
    }
}
