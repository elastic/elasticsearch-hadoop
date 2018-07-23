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

package org.elasticsearch.hadoop.security;

import java.io.IOException;

import org.elasticsearch.hadoop.EsHadoopException;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.security.KeystoreWrapper.KeystoreBuilder;

/**
 * Loads a keystore to retrieve secure settings, falling back to the settings object if the
 * keystore is not configured or the property cannot be found.
 */
public class SecureSettings {

    private final Settings settings;
    private KeystoreWrapper keystoreWrapper;

    public SecureSettings(Settings settings) {
        this.settings = settings;

        String keystoreLocation = settings.getProperty(ConfigurationOptions.ES_KEYSTORE_LOCATION);
        if (keystoreLocation != null) {
            KeystoreBuilder builder = KeystoreWrapper.loadStore(keystoreLocation);
            try {
                this.keystoreWrapper = builder.build();
            } catch (EsHadoopSecurityException e) {
                throw new EsHadoopException("Could not load keystore", e);
            } catch (IOException e) {
                throw new EsHadoopException("Could not load keystore", e);
            }
        } else {
            this.keystoreWrapper = null;
        }
    }

    /**
     *
     * @param key property name
     * @return secure property value or null
     */
    public String getSecureProperty(String key) {
        String value = null;
        if (keystoreWrapper != null) {
            try {
                value = keystoreWrapper.getSecureSetting(key);
            } catch (EsHadoopSecurityException e) {
                throw new EsHadoopException("Could not read secure setting [" + key + "]", e);
            }
        }
        if (value == null) {
            value = settings.getProperty(key);
        }
        return value;
    }
}
