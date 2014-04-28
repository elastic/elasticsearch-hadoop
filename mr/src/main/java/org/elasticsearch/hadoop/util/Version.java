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
package org.elasticsearch.hadoop.util;

import java.util.Properties;

import org.apache.commons.logging.LogFactory;


public abstract class Version {

    private static final String UNKNOWN = "Unknown";
    private static final String VER;
    private static final String HASH;
    private static final String SHORT_HASH;

    public static boolean printed = false;

    static {
        Properties build = new Properties();
        try {
            build = IOUtils.propsFromString(IOUtils.asString(Version.class.getResourceAsStream("/esh-build.properties")));
        } catch (Exception ex) {
            // ignore if no build info was found
        }
        VER = build.getProperty("version", UNKNOWN);
        HASH = build.getProperty("hash", UNKNOWN);
        SHORT_HASH = HASH.length() > 10 ? HASH.substring(0, 10) : HASH;
    }

    public static String version() {
        return "v" + versionNumber() + "[" + versionHashShort() + "]";
    }

    public static String versionNumber() {
        return VER;
    }

    public static String versionHash() {
        return HASH;
    }

    public static String versionHashShort() {
        return SHORT_HASH;
    }

    public static void logVersion() {
        if (!printed) {
            printed = true;
            LogFactory.getLog(Version.class).info("Elasticsearch for Hadoop " + version());
        }
    }
}
