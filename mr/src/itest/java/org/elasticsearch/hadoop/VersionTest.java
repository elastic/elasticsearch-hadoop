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

import java.io.File;
import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.elasticsearch.hadoop.util.OsUtil;
import org.elasticsearch.hadoop.util.StringUtils;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.AssumptionViolatedException;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class VersionTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testNormalize() throws Exception {

        List<URL> urls = new ArrayList<URL>();
        urls.add(new URL("jar:file:/tmp/mesos/slaves/3014350f-cd05-44af-9c9c-3974bdeed86e-S1/frameworks/3014350f-cd05-44af-9c9c-3974bdeed86e-0016/executors/3014350f-cd05-44af-9c9c-3974bdeed86e-S1/runs/3697bc14-f0bd-48b9-8599-9683867eec63/elasticsearch-spark_2.10-2.2.0-m1.jar!/"));
        urls.add(new URL("jar:file:/tmp/mesos/slaves/3014350f-cd05-44af-9c9c-3974bdeed86e-S1/frameworks/3014350f-cd05-44af-9c9c-3974bdeed86e-0016/executors/3014350f-cd05-44af-9c9c-3974bdeed86e-S1/runs/3697bc14-f0bd-48b9-8599-9683867eec63/./elasticsearch-spark_2.10-2.2.0-m1.jar!/"));

        Set<String> normalized = new LinkedHashSet<String>();

        for (URL url : urls) {
            // try normalization first
            String norm = StringUtils.normalize(url.toString());
            System.out.println(norm);
            normalized.add(norm);
        }

        System.out.println(normalized);
        assertEquals(1, normalized.size());
    }

    @Test
    public void testWindowsNormalizeCanonicalPath() throws Exception {
        Assume.assumeTrue("Windows Only Test", OsUtil.IS_OS_WINDOWS);

        // null begets null
        assertNull(toCanonicalFile(null));
        // Windows backslashes converted to forward slashes
        assertEquals("file:/C:/Windows/System32/", toCanonicalFile(new File("C:\\Windows\\System32\\").toURI().toURL()));
        // first colon that is found is treated as part of the path element name if the prefix contains a slash
        assertEquals("file:/C:/f/ile:/test", toCanonicalFile(new URL("file:/C:/f/ile:/test/")));
        // ensure this works with backslashes too
        assertEquals("file:/C:/f/ile:/test", toCanonicalFile(new File("C:\\f\\ile:\\test\\").toURI().toURL()));
        // colon denotes a prefix for the file if no slash is in the prefix
        assertEquals("file:/C:/test", toCanonicalFile(new File("C:\\test\\").toURI().toURL()));
        // ensure this works for jar urls
        assertEquals("file:/C:/test", toCanonicalFile(new URL("jar:file:/C:/test/!/some/package/Hello.class")));
        // Ignore dots
        assertEquals("file:/C:/test/test", toCanonicalFile(new URL("file:/C:/test/./test/")));
        // Remove parent dots
        assertEquals("file:/C:/test", toCanonicalFile(new URL("file:/C:/test/test/..")));

        // Windows jvm lacks the ability to link without special privileges.
        // FS Links are less common on Windows, so skip.
    }

    @Test
    public void testNormalizeCanonicalPaths() throws Exception {
        Assume.assumeFalse("Non Windows Test Only", OsUtil.IS_OS_WINDOWS);
        // null begets null
        assertNull(toCanonicalFile(null));
        // first colon that is found is treated as part of the path element name if the prefix contains a slash
//        assertEquals("f/ile:/test", StringUtils.normalize("f/ile:/test/"));
//        assertEquals("f/ile:/test", toCanonicalFile(new URL("f/ile:/test/")));
        // colon denotes a prefix for the file if no slash is in the prefix
        assertEquals("file:/test", toCanonicalFile(new URL("file:/test/")));
        // ensure this works for jar urls
        assertEquals("file:/test", toCanonicalFile(new URL("jar:file:/test/!/some/package/Hello.class")));
        // Ignore dots
        assertEquals("file:/test/test", toCanonicalFile(new URL("file:/test/./test/")));
        // Remove parent dots
        assertEquals("file:/test", toCanonicalFile(new URL("file:/test/test/..")));

        // On mac, the /tmp dir is a link to /private/tmp due to legacy weirdness.
        // Both paths get added to the classpath for local YARN node managers, so make sure to use the canonical file names

        // Create a linked directory (like Mac's /tmp dir)
        File linkTest = temporaryFolder.newFolder("linkTest");
        File privateDir = new File(linkTest, "private");
        mkdirSafe(privateDir);
        File privateTmpDir = new File(privateDir, "tmp");
        mkdirSafe(privateTmpDir);
        File privateTmpOtherDir = new File(privateTmpDir, "other");
        mkdirSafe(privateTmpOtherDir);
        File tmpDir = new File(linkTest, "tmp");
        linkDir(privateTmpDir, tmpDir);

        // File in original directory
        File testFile = new File(privateTmpDir, "test");
        createSafe(testFile);

        // Same file but in linked dir
        File linkedTestFile = new File(tmpDir, "test");

        // Same file but referenced in an absolute, but non-canonical manner
        File awkwardTestFile = new File(linkTest.getAbsolutePath() + "/private/tmp/./test");
        File awkwardParentTestFile = new File(linkTest.getAbsolutePath() + "/private/tmp/other/../test");

        // Assert paths are different but canonical resolves as the same
        assertTrue(testFile.exists());
        assertTrue(linkedTestFile.exists());
        assertTrue(awkwardTestFile.exists());
        assertTrue(awkwardParentTestFile.exists());

        assertNotEquals(testFile.getAbsolutePath(), linkedTestFile.getAbsolutePath());
        assertNotEquals(testFile.getAbsolutePath(), awkwardTestFile.getAbsolutePath());
        assertNotEquals(testFile.getAbsolutePath(), awkwardParentTestFile.getAbsolutePath());

        assertEquals(toCanonicalFile(testFile.toURI().toURL()), toCanonicalFile(linkedTestFile.toURI().toURL()));
        assertEquals(toCanonicalFile(testFile.toURI().toURL()), toCanonicalFile(awkwardTestFile.toURI().toURL()));
        assertEquals(toCanonicalFile(testFile.toURI().toURL()), toCanonicalFile(awkwardParentTestFile.toURI().toURL()));

        URL testURL = new URL("jar:file:" + testFile.getAbsolutePath() + "!/some/package/Hello.class");
        URL linkedTestURL = new URL("jar:file:" + linkedTestFile.getAbsolutePath() + "!/some/package/Hello.class");
        URL awkwardTestURL = new URL("jar:file:" + awkwardTestFile.getAbsolutePath() + "!/some/package/Hello.class");
        URL awkwardParentTestURL = new URL("jar:file:" + awkwardParentTestFile.getAbsolutePath() + "!/some/package/Hello.class");

        assertNotEquals(testURL.toString(), linkedTestURL.toString());
        assertNotEquals(testURL.toString(), awkwardTestURL.toString());
        assertNotEquals(testURL.toString(), awkwardParentTestFile.toString());

        assertEquals(toCanonicalFile(testURL), toCanonicalFile(linkedTestURL));
        assertEquals(toCanonicalFile(testURL), toCanonicalFile(awkwardTestURL));
        assertEquals(toCanonicalFile(testURL), toCanonicalFile(awkwardParentTestURL));
    }

    private void createSafe(File testFile) throws IOException {
        if (!testFile.createNewFile()) {
            throw new IOException("Could not create test file " + testFile.toString());
        }
    }

    private void mkdirSafe(File dir) throws IOException {
        if (!dir.mkdirs()) {
            throw new IOException("Could not create " + dir + " folder");
        }
    }

    private void linkDir(File privateTmpDir, File tmpDir) throws IOException {
        try {
            Files.createSymbolicLink(tmpDir.toPath(), privateTmpDir.toPath());
        } catch (UnsupportedOperationException e) {
            throw new AssumptionViolatedException("OS does not support symlinks", e);
        }
    }

    /**
     * Convert either a file or jar url into a local canonical file, or null if the file is a different scheme.
     */
    private String toCanonicalFile(URL fileURL) throws URISyntaxException, IOException {
        if (fileURL == null) {
            return null;
        }

        // Only handle jar: and file: schemes
        if (!fileURL.getProtocol().equals("jar") && !fileURL.getProtocol().equals("file")) {
            return null;
        }

        // Parse the jar file location from the jar url. Doesn't open any resources.
        if (fileURL.getProtocol().equals("jar")) {
            JarURLConnection jarURLConnection = (JarURLConnection) fileURL.openConnection();
            fileURL = jarURLConnection.getJarFileURL();
        }

        URI fileURI = fileURL.toURI();
        File file = new File(fileURI);

        // Use filesystem to resolve any sym links or dots in the path to
        // a singular unique file path
        File canonicalFile = file.getCanonicalFile();

        return canonicalFile.toURI().toString();
    }
}
