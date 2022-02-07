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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.JarURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.util.jar.JarFile;

import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class IOUtilsTest {
    @Test
    public void openResource() throws Exception {
        InputStream inputStream = IOUtils.open("org/elasticsearch/hadoop/util/textdata.txt");
        assertNotNull(inputStream);

        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        assertEquals("Hello World. This is used by IOUtilsTest.", reader.readLine());
    }

    @Test
    public void openFile() throws Exception {
        File tempFile = File.createTempFile("textdata", "txt");
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tempFile)));
        writer.write("Hello World. This is used by IOUtilsTest.");
        writer.close();

        InputStream inputStream = IOUtils.open(tempFile.toURI().toURL().toString());
        assertNotNull(inputStream);

        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        assertEquals("Hello World. This is used by IOUtilsTest.", reader.readLine());
    }

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void openNonExistingFile() throws Exception {
        InputStream inputStream = IOUtils.open("file:///This/Doesnt/Exist");
        fail("Shouldn't pass");
    }

    @Test
    public void testToCanonicalFile() throws Exception {
        String file = "file:/some/path/org/elasticsearch/hadoop/util/Version.class";
        URL url = new URL(file);
        String canonicalFilePath = IOUtils.toCanonicalFilePath(url);
        assertEquals(file, canonicalFilePath);

        url = new URL("jar:file:/some/path/elasticsearch-hadoop-7.17.0.jar!/org/elasticsearch/hadoop/util/Version.class");
        canonicalFilePath = IOUtils.toCanonicalFilePath(url);
        assertEquals("file:/some/path/elasticsearch-hadoop-7.17.0.jar", canonicalFilePath);

        url = new URL("file:/some/path/../path/org/elasticsearch/hadoop/util/Version.class");
        canonicalFilePath = IOUtils.toCanonicalFilePath(url);
        assertEquals("file:/some/path/org/elasticsearch/hadoop/util/Version.class", canonicalFilePath);
    }

    @Test
    public void testToCanonicalFileSpringBoot() throws Exception {
        String jarWithinJarPath = "file:/some/path/outer.jar!/BOOT-INF/lib/elasticsearch-hadoop-7.17.0.jar";
        String file = jarWithinJarPath + "!/org/elasticsearch/hadoop/util/Version.class";
        URL url = new URL("jar", "", -1, file, new SpringBootURLStreamHandler(jarWithinJarPath) );
        String canonicalFilePath = IOUtils.toCanonicalFilePath(url);
        assertEquals("jar:" + jarWithinJarPath, canonicalFilePath);
    }

    /**
     * This class simulates what Spring Boot's URLStreamHandler does.
     */
    private static class SpringBootURLStreamHandler extends URLStreamHandler {
        private final String jarWithinJarPath;
        public SpringBootURLStreamHandler(String jarWithinJarPath) {
            this.jarWithinJarPath = jarWithinJarPath;
        }

        @Override
        protected URLConnection openConnection(URL url) throws IOException {
            return new JarURLConnection(url) {
                @Override
                public JarFile getJarFile() throws IOException {
                    return null;
                }

                @Override
                public void connect() throws IOException {
                }

                @Override
                public URL getJarFileURL() {
                    try {
                        return new URL("jar:" + jarWithinJarPath);
                    } catch (MalformedURLException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
        }
    }
}