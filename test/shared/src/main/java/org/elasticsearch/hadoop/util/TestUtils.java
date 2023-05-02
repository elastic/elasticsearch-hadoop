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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.mr.HadoopCfgUtils;
import org.elasticsearch.hadoop.rest.RestClient;
import org.elasticsearch.hadoop.serialization.EsHadoopSerializationException;

import javax.xml.bind.DatatypeConverter;

import static org.elasticsearch.hadoop.util.IOUtils.close;

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

    public static ClusterInfo getEsClusterInfo() {
        RestClient client = new RestClient(new TestSettings());
        try {
            return client.mainInfo();
        } finally {
            client.close();
        }
    }

    public static String resource(String index, String type, EsMajorVersion testVersion) {
        if (TestUtils.isTypelessVersion(testVersion)) {
            return index;
        } else {
            return index + "/" + type;
        }
    }

    public static String docEndpoint(String index, String type, EsMajorVersion testVersion) {
        if (TestUtils.isTypelessVersion(testVersion)) {
            return index + "/_doc";
        } else {
            return index + "/" + type;
        }
    }

    public static boolean isTypelessVersion(EsMajorVersion version) {
        // Types have been deprecated in 7.0.0, and will be removed at a later date
        return version.onOrAfter(EsMajorVersion.V_7_X);
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

    public static String serializeToBase64(Serializable object) {
        if (object == null) {
            return StringUtils.EMPTY;
        }
        FastByteArrayOutputStream baos = new FastByteArrayOutputStream();
        ObjectOutputStream oos = null;
        try {
            oos = new ObjectOutputStream(baos);
            oos.writeObject(object);
        } catch (IOException ex) {
            throw new EsHadoopSerializationException("Cannot serialize object " + object, ex);
        } finally {
            close(oos);
        }
        return DatatypeConverter.printBase64Binary(baos.bytes().bytes());
    }

    @SuppressWarnings("unchecked")
    public static <T extends Serializable> T deserializeFromBase64(String data) {
        if (!StringUtils.hasLength(data)) {
            return null;
        }

        byte[] rawData = DatatypeConverter.parseBase64Binary(data);
        ObjectInputStream ois = null;
        try {
            ois = new ObjectInputStream(new FastByteArrayInputStream(rawData));
            Object o = ois.readObject();
            return (T) o;
        } catch (ClassNotFoundException ex) {
            throw new EsHadoopIllegalStateException("cannot deserialize object", ex);
        } catch (IOException ex) {
            throw new EsHadoopSerializationException("cannot deserialize object", ex);
        } finally {
            close(ois);
        }
    }
}