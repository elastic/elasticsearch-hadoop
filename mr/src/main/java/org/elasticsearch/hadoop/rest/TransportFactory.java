package org.elasticsearch.hadoop.rest;

import org.elasticsearch.hadoop.cfg.Settings;

/**
 * Creates {@link Transport} Objects
 */
public interface TransportFactory {
    Transport create(Settings settings, String hostInfo);
}
