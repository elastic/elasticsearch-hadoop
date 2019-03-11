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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

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

}