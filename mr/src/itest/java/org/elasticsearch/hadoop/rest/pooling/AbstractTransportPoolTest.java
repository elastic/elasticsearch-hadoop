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

import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.mr.security.HadoopUserProvider;
import org.elasticsearch.hadoop.rest.Transport;
import org.elasticsearch.hadoop.security.SecureSettings;
import org.elasticsearch.hadoop.util.SettingsUtils;
import org.elasticsearch.hadoop.util.TestSettings;
import org.junit.Test;

import java.util.UUID;

import static org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_NET_TRANSPORT_POOLING_EXPIRATION_TIMEOUT;
import static org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_SECURITY_USER_PROVIDER_CLASS;

public class AbstractTransportPoolTest {

    @Test
    public void removeOldConnections() throws Exception {
        Settings settings = new TestSettings();
        settings.setProperty(ES_NET_TRANSPORT_POOLING_EXPIRATION_TIMEOUT, "2s");

        String host = SettingsUtils.discoveredOrDeclaredNodes(settings).get(0);

        TransportPool pool = new TransportPool(UUID.randomUUID().toString(), host, settings, new SecureSettings(settings));

        Transport transport1 = null;
        Transport transport2 = null;
        Transport transport3 = null;

        try {
            // Checkout three transports all at once, this should create three pooled transports.
            transport1 = pool.borrowTransport();
            transport2 = pool.borrowTransport();
            transport3 = pool.borrowTransport();

            // Close two of them.
            transport1.close();
            transport2.close();

            // Wait the amount of time to close.
            Thread.sleep(settings.getTransportPoolingExpirationTimeout().millis() + 1000L);

            // Will need to remove 2 connections at this point
            pool.removeOldConnections();

        } finally {
            // Close everything
            if (transport1 != null) {
                transport1.close();
            }

            if (transport2 != null) {
                transport2.close();
            }

            if (transport3 != null) {
                transport3.close();
            }
        }
    }


    @Test
    public void testValidationWithNoUserProvider() throws Exception {
        /*
         * In this test, we make sure that validating PooledTransports does not throw an exception if the resource is bad. Instead, the
         * PooledTransport is replaced.
         */
        Settings settings = new TestSettings();
        settings.setProperty(ES_NET_TRANSPORT_POOLING_EXPIRATION_TIMEOUT, "1s");
        String host = SettingsUtils.discoveredOrDeclaredNodes(settings).get(0);

        TransportPool pool = new TransportPool(UUID.randomUUID().toString(), host, settings, new SecureSettings(settings));

        Transport transport1 = null;

        try {
            transport1 = pool.borrowTransport();
            transport1.close();

            // Wait the amount of time to close.
            Thread.sleep(settings.getTransportPoolingExpirationTimeout().millis() + 1000L);

            transport1 = pool.borrowTransport();
            transport1.close();

            // Will need to remove 2 connections at this point
            pool.removeOldConnections();

        } finally {
            // Close everything
            if (transport1 != null) {
                transport1.close();
            }
        }

    }

    @Test
    public void testValidationWithUserProvider() throws Exception {
        /*
         * In this test, we make sure that validating PooledTransports does not throw an exception if the resource is bad when we have set
         * a ES_SECURITY_USER_PROVIDER_CLASS. Instead, the PooledTransport is replaced. Previously this would incorrectly throw an
         * exception.
         */
        Settings settings = new TestSettings();
        settings.setProperty(ES_NET_TRANSPORT_POOLING_EXPIRATION_TIMEOUT, "1s");
        settings.setProperty(ES_SECURITY_USER_PROVIDER_CLASS, HadoopUserProvider.class.getName());
        String badHost = "127.0.0.1:11111"; //intentionally not a real host so that validation fails

        TransportPool pool = new TransportPool(UUID.randomUUID().toString(), badHost, settings, new SecureSettings(settings));

        Transport transport1 = null;

        try {
            transport1 = pool.borrowTransport();
            transport1.close();

            // Wait the amount of time to close.
            Thread.sleep(settings.getTransportPoolingExpirationTimeout().millis() + 1000L);

            // The following will throw an exception reported in https://github.com/elastic/elasticsearch-hadoop/issues/1362 without the fix
            transport1 = pool.borrowTransport();
            transport1.close();

            // Will need to remove 2 connections at this point
            pool.removeOldConnections();

        } finally {
            // Close everything
            if (transport1 != null) {
                transport1.close();
            }
        }
    }
}