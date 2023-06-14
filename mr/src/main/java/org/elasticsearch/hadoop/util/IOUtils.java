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

import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.serialization.EsHadoopSerializationException;
import org.elasticsearch.hadoop.thirdparty.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.hadoop.thirdparty.codehaus.jackson.map.SerializationConfig;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.net.JarURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;

/**
 * Utility class used internally
 */
public abstract class IOUtils {

    private final static Field BYTE_ARRAY_BUFFER;

    static {
        BYTE_ARRAY_BUFFER = ReflectionUtils.findField(ByteArrayInputStream.class, "buf");
        ReflectionUtils.makeAccessible(BYTE_ARRAY_BUFFER);
    }

    private static final ObjectMapper mapper = new ObjectMapper().configure(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS, false);

    /**
     * This method serializes object into a json String using jackson. The object must support jackson serialization.
     */
    public static String serializeToJsonString(Object object) {
        if (object == null) {
            return StringUtils.EMPTY;
        }
        final String json;
        try {
            json = mapper.writeValueAsString(object);
        } catch (IOException ex) {
            throw new EsHadoopSerializationException("Cannot serialize object: " + object, ex);
        }
        return json;
    }

    /**
     * This method deserializes a String that was created by serializeToJsonString
     */
    public static <T> T deserializeFromJsonString(String data, Class<T> clazz) {
        if (!StringUtils.hasLength(data)) {
            return null;
        }
        final T object;
        try {
            object =  mapper.readValue(data, clazz);
        } catch (IOException e) {
            throw new EsHadoopSerializationException("Cannot deserialize string: [" + data + "]", e);
        }
        return object;
    }

    public static String propsToString(Properties props) {
        StringWriter sw = new StringWriter();
        if (props != null) {
            try {
                props.store(sw, "");
            } catch (IOException ex) {
                throw new EsHadoopIllegalArgumentException(ex);
            }
        }
        return sw.toString();
    }

    public static Properties propsFromString(String source) {
        Properties copy = new Properties();
        if (source != null) {
            try {
                copy.load(new StringReader(source));
            } catch (IOException ex) {
                throw new EsHadoopIllegalArgumentException(ex);
            }
        }
        return copy;
    }

    public static BytesArray asBytes(InputStream in) throws IOException {
        BytesArray ba = unwrapStreamBuffer(in);
        if (ba != null) {
            return ba;
        }
        return asBytes(new BytesArray(in.available()), in);
    }

    public static BytesArray asBytes(BytesArray ba, InputStream in) throws IOException {
        BytesArray buf = unwrapStreamBuffer(in);
        if (buf != null) {
            ba.bytes(buf);
            return ba;
        }

        FastByteArrayOutputStream bos = new FastByteArrayOutputStream(ba);
        byte[] buffer = new byte[1024];
        int read = 0;
        try {
            while ((read = in.read(buffer)) != -1) {
                bos.write(buffer, 0, read);
            }

        } finally {
            try {
                in.close();
            } catch (IOException ex) {
                // ignore
            }
            // non needed but used to avoid the warnings
            bos.close();
        }
        return bos.bytes();
    }

    public static String asString(InputStream in) throws IOException {
        return asBytes(in).toString();
    }

    public static String asStringAlways(InputStream in) {
        if (in == null) {
            return StringUtils.EMPTY;
        }
        try {
            return asBytes(in).toString();
        } catch (IOException ex) {
            return StringUtils.EMPTY;
        }
    }

    public static InputStream open(String resource, ClassLoader loader) {
        if (loader == null) {
            loader = Thread.currentThread().getContextClassLoader();
        }

        if (loader == null) {
            loader = IOUtils.class.getClassLoader();
        }

        try {
            // no prefix means classpath
            if (!resource.contains(":")) {
                return loader.getResourceAsStream(resource);
            }
            return new URL(resource).openStream();
        } catch (IOException ex) {
            throw new EsHadoopIllegalArgumentException(String.format("Cannot open stream for resource %s", resource), ex);
        }
    }

    public static InputStream open(String location) {
        return open(location, null);
    }

    public static void close(Closeable closable) {
        if (closable != null) {
            try {
                closable.close();
            } catch (IOException e) {
                // silently ignore
            }
        }
    }

    private static byte[] byteArrayInputStreamInternalBuffer(ByteArrayInputStream bais) {
        return ReflectionUtils.getField(BYTE_ARRAY_BUFFER, bais);
    }

    private static BytesArray unwrapStreamBuffer(InputStream in) {
        if (in instanceof FastByteArrayInputStream) {
            return ((FastByteArrayInputStream) in).data;
        }

        if (in instanceof ByteArrayInputStream) {
            return new BytesArray(byteArrayInputStreamInternalBuffer((ByteArrayInputStream) in));
        }
        return null;
    }

    /**
     * Convert either a file or jar url into a local canonical file, or null if the file is a different scheme.
     * @param fileURL the url to resolve to a canonical file.
     * @return null if given URL is null, not using the jar scheme, or not using the file scheme. Otherwise, returns the
     * String path to the local canonical file.
     * @throws URISyntaxException If the given URL cannot be transformed into a URI
     * @throws IOException If the jar cannot be read or if the canonical file cannot be determined
     */
    public static String toCanonicalFilePath(URL fileURL) throws URISyntaxException, IOException {
        if (fileURL == null) {
            return null;
        }

        // Only handle jar: and file: schemes
        if (!"jar".equals(fileURL.getProtocol()) && !"file".equals(fileURL.getProtocol())) {
            return null;
        }

        // Parse the jar file location from the jar url. Doesn't open any resources.
        if ("jar".equals(fileURL.getProtocol())) {
            JarURLConnection jarURLConnection = (JarURLConnection) fileURL.openConnection();
            fileURL = jarURLConnection.getJarFileURL();
        }
        /*
         * Ordinarily at this point we would have a URL with a "file" protocal. But Spring boot puts the es-hadoop jar is inside of the
         * spring boot jar like:
         * jar:file:/some/path/outer.jar!/BOOT-INF/lib/elasticsearch-hadoop-7.17.0.jar!/org/elasticsearch/hadoop/util/Version.class
         * And spring boot has its own custom URLStreamHandler which returns a URL with a "jar" protocol from the previous call to
         * getJarFileURL() (the default JDK URLStreamHandler does not do this). So this next check is Spring Boot specific.
         */
        final String springBootInnerJarFilePath;
        if ("jar".equals(fileURL.getProtocol())) {
            JarURLConnection jarURLConnection = (JarURLConnection) fileURL.openConnection();
            springBootInnerJarFilePath = jarURLConnection.getEntryName();
            fileURL = jarURLConnection.getJarFileURL();
        } else {
            springBootInnerJarFilePath = null;
        }

        String canonicalString;
        if ("file".equals(fileURL.getProtocol())) {
            URI fileURI = fileURL.toURI();
            File file = new File(fileURI);

            // Use filesystem to resolve any sym links or dots in the path to
            // a singular unique file path
            File canonicalFile = file.getCanonicalFile();
            canonicalString = canonicalFile.toURI().toString();
            if (springBootInnerJarFilePath != null) {
                canonicalString = "jar:" + canonicalString + "!/" + springBootInnerJarFilePath;
            }
        } else {
            /*
             * In the event that some custom classloader is doing strange things and we don't have a file URL here, better to output
             * whatever URL it gives us rather than fail
             */
            canonicalString = fileURL.toString();
        }
        return canonicalString;
    }
}