/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticsearch.hadoop.cfg;

import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

public abstract class ConfigUtils implements InternalConfigurationOptions {

    public static String detectHostPortAddress(Configuration cfg) {
        if (cfg == null) {
            return detectHostPortAddress(null, 0, cfg);
        }
        String address = cfg.get(INTERNAL_ES_TARGET_URI);
        return !StringUtils.isBlank(address) ? address : detectHostPortAddress(null, 0, cfg);
    }

    public static String detectHostPortAddress(String host, int port, Configuration cfg) {
        String h = !StringUtils.isBlank(host) ? host : (cfg == null ? ES_HOST_DEFAULT : cfg.get(ES_HOST, ES_HOST_DEFAULT));
        int p = (port > 0) ? port : Integer.valueOf(cfg == null ? ES_PORT_DEFAULT : cfg.get(ES_PORT, ES_PORT_DEFAULT));
        return new StringBuilder("http://").append(h).append(":").append(p).append("/").toString();
    }

    public static String detectHostPortAddress(String host, int port, Properties cfg) {
        String h = !StringUtils.isBlank(host) ? host : (cfg == null ? ES_HOST_DEFAULT : cfg.getProperty(ES_HOST, ES_HOST_DEFAULT));
        int p = (port > 0) ? port : Integer.valueOf(cfg == null ? ES_PORT_DEFAULT : cfg.getProperty(ES_PORT, ES_PORT_DEFAULT));
        return new StringBuilder("http://").append(h).append(":").append(p).append("/").toString();
    }
}
