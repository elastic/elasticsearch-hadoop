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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Defines the constants for determining the type of authentication to Elasticsearch we will be using.
 */
public enum AuthenticationMethod {

    /**
     * Simple mode uses no security features to authenticate to the Elasticsearch cluster
     */
    SIMPLE("simple"),

    /**
     * Basic mode uses basic HTTP authentication using a username and password to authenticate to the Elasticsearch cluster
     */
    BASIC("basic"),

    /**
     * PKI mode uses certificates that are provided to Elasticsearch during the SSL negotiation phase to determine identity.
     */
    PKI("pki"),

    /**
     * Kerberos mode uses SPNEGO via HTTP to authenticate to the Elasticsearch cluster
     */
    KERBEROS("kerberos");

    private final static Map<String, AuthenticationMethod> REGISTRY = new HashMap<String, AuthenticationMethod>(4);
    static {
        REGISTRY.put(SIMPLE.value, SIMPLE);
        REGISTRY.put(BASIC.value, BASIC);
        REGISTRY.put(PKI.value, PKI);
        REGISTRY.put(KERBEROS.value, KERBEROS);
    }

    /**
     * Look up an AuthenticationMethod based on the given value of a setting.
     * @param value The setting string
     * @return An AuthenticationMethod if the string is valid, or null if none could be found.
     */
    public static AuthenticationMethod get(String value) {
        return REGISTRY.get(value);
    }

    /**
     * @return a List of available setting values
     */
    public static List<String> getAvailableMethods() {
        return Arrays.asList(SIMPLE.value, BASIC.value, PKI.value, KERBEROS.value);
    }

    private final String value;

    AuthenticationMethod(String value) {
        this.value = value;
    }

    /**
     * The configuration value that corresponds to this method
     * @return the settings string representation of this method
     */
    public String getValue() {
        return value;
    }
}
