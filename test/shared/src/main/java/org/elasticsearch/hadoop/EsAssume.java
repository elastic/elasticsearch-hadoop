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

package org.elasticsearch.hadoop;

import org.elasticsearch.hadoop.rest.RestUtils;
import org.elasticsearch.hadoop.util.EsMajorVersion;

import static org.junit.Assume.assumeTrue;

/**
 * Class for bringing together common assume calls into one place
 */
public class EsAssume {

    public static void versionOnOrAfter(EsMajorVersion version, String message) {
        assumeTrue(message, getVersion().onOrAfter(version));
    }

    public static void versionOn(EsMajorVersion version, String message) {
        assumeTrue(message, getVersion().onOrAfter(version));
    }

    public static void versionOnOrBefore(EsMajorVersion version, String message) {
        assumeTrue(message, getVersion().onOrBefore(version));
    }

    private static EsMajorVersion getVersion() {
        try (RestUtils.ExtendedRestClient versionTestingClient = new RestUtils.ExtendedRestClient()) {
            return versionTestingClient.remoteEsVersion();
        }
    }
}
