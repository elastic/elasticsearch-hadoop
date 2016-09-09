package org.elasticsearch.hadoop.rest.commonshttp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.Transport;
import org.elasticsearch.hadoop.rest.TransportFactory;

/**
 * Creates regular instances of {@link CommonsHttpTransport}
 */
public class CommonsHttpTransportFactory implements TransportFactory {

    private final Log log = LogFactory.getLog(this.getClass());

    @Override
    public Transport create(Settings settings, String hostInfo) {
        if (log.isDebugEnabled()) {
            log.debug("Creating new CommonsHttpTransport");
        }
        return new CommonsHttpTransport(settings, hostInfo);
    }
}
