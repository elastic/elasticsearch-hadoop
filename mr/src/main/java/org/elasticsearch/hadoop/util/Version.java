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

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.LogFactory;


public abstract class Version {

    private static final String UNKNOWN = "Unknown";
    private static final String VER;
    private static final String HASH;
    private static final String SHORT_HASH;

    public static boolean printed = false;

    static {
        // check classpath
        String target = Version.class.getName().replace(".", "/").concat(".class");
        Enumeration<URL> res = null;

        try {
            res = Version.class.getClassLoader().getResources(target);
        } catch (IOException ex) {
            LogFactory.getLog(Version.class).warn("Cannot detect ES-Hadoop jar; it typically indicates a deployment issue...");
        }

        if (res != null) {
            List<URL> urls = Collections.list(res);
            Map<String, List<URL>> normalized = new LinkedHashMap<String, List<URL>>();

            for (URL url : urls) {
                try {
                    String canonicalPath = IOUtils.toCanonicalFilePath(url);
                    List<URL> pathURLs = normalized.get(canonicalPath);
                    if (pathURLs == null) {
                        pathURLs = new ArrayList<URL>();
                        normalized.put(canonicalPath, pathURLs);
                    }
                    pathURLs.add(url);
                } catch (URISyntaxException e) {
                    String message = "Could not parse classpath resource URI: " + url.toString();
                    LogFactory.getLog(Version.class).fatal(message, e);
                    throw new Error(message, e);
                } catch (IOException e) {
                    String message = "Could not retrieve canonical path to classpath resource: " + url.toString();
                    LogFactory.getLog(Version.class).fatal(message, e);
                    throw new Error(message, e);
                }
            }

            int foundJars = 0;
            if (normalized.size() > 1) {
                StringBuilder sb = new StringBuilder("Multiple ES-Hadoop versions detected in the classpath; please use only one\n");
                for (List<URL> pathURLs : normalized.values()) {
                    String path = pathURLs.get(0).toString();
                    if (path.contains("jar:")) {
                        foundJars++;
                        sb.append(path.replace("!/" + target, ""));
                        sb.append("\n");
                    }
                }
                if (foundJars > 1) {
                    LogFactory.getLog(Version.class).fatal(sb);
                    throw new Error(sb.toString());
                }
            }
        }

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
        return "v" + versionNumber() + " [" + versionHashShort() + "]";
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
            LogFactory.getLog(Version.class).info("Elasticsearch Hadoop " + version());

            // Check java version
            String javaVersion = System.getProperty("java.version");
            if (javaVersion.startsWith("1.")) {
                if (!javaVersion.startsWith("1.8.")) {
                    LogFactory.getLog(Version.class).warn("Using java version " + javaVersion + " is deprecated in Elasticsearch Hadoop");
                }
            }
        }
    }
}
