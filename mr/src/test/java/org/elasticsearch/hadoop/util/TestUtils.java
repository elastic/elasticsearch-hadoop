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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.elasticsearch.hadoop.mr.HadoopCfgUtils;

public class TestUtils {

    public static final String ES_LOCAL_PORT = "es.hadoop.testing.local.es.port";

    public static boolean delete(File file) {
        if (file == null || !file.exists()) {
            return false;
        }

        boolean result = true;
        if (file.isDirectory()) {
            String[] children = file.list();
            for (int i = 0; i < children.length; i++) {
                result &= delete(new File(file, children[i]));
            }
        }
        return file.delete() & result;
    }

    public static boolean isWindows() {
        return System.getProperty("os.name").toLowerCase(Locale.ROOT).startsWith("win");
    }

    public static byte[] fromInputStream(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int length;
        try {
            while ((length = in.read(buffer)) != -1)
                out.write(buffer, 0, length);
        } finally {
            if (in != null) {
                in.close(); // call this in a finally block
            }
        }

        return out.toByteArray();
    }


    public static String sampleQueryDsl() {
        // use the URI here to be properly loaded
        return localUri("/org/elasticsearch/hadoop/integration/query.dsl");
    }

    public static String sampleQueryUri() {
        return localUri("/org/elasticsearch/hadoop/integration/query.uri");
    }

    public static String sampleArtistsJson() {
        return localResource("/artists.json");
    }

    public static String sampleArtistsJson(Configuration cfg) {
        return (HadoopCfgUtils.isLocal(cfg) ? localResource("/artists.json") : "/artists.json");
    }

    public static String sampleArtistsDat() {
        return localResource("/artists.dat");
    }

    public static String sampleArtistsDat(Configuration cfg) {
        return (HadoopCfgUtils.isLocal(cfg) ? localResource("/artists.dat") : "/artists.dat");
    }

    public static String gibberishDat(Configuration cfg) {
        return (HadoopCfgUtils.isLocal(cfg) ? localResource("/gibberish.dat") : "/gibberish.dat");
    }

    public static String gibberishJson(Configuration cfg) {
        return (HadoopCfgUtils.isLocal(cfg) ? localResource("/gibberish.json") : "/gibberish.json");
    }


    private static String localResource(String resource) {
        return TestSettings.class.getResource(resource).getFile();
    }

    private static String localUri(String resource) {
        return TestSettings.class.getResource(resource).toString();
    }
}