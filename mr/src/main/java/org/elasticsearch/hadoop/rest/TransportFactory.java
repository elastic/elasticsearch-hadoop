package org.elasticsearch.hadoop.rest;

import org.elasticsearch.hadoop.cfg.Settings;

/**
 * Creates {@link Transport} Objects
 */
public interface TransportFactory {
    /**
     * Creates a {@link Transport} object
     * @param settings Specifies the Transport's properties
     * @param hostInfo Host to connect to
     */
    Transport create(Settings settings, String hostInfo);
}
