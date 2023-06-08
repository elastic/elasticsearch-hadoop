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
import org.elasticsearch.hadoop.serialization.FieldType;
import org.elasticsearch.hadoop.serialization.dto.mapping.Field;
import org.elasticsearch.hadoop.serialization.dto.mapping.Mapping;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertArrayEquals;
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
    public void testDeserializeFromJsonString() {
        assertNull(IOUtils.deserializeFromJsonString("", String.class));
        try {
            IOUtils.deserializeFromJsonString("junk", String.class);
            fail("Should have thrown an EsHadoopIllegalArgumentException");
        } catch (EsHadoopSerializationException expected) {}
        List<Field> fieldsList = new ArrayList<>();
        fieldsList.add(new Field("%s", FieldType.TEXT));
        Mapping mapping = new Mapping("*", "*", fieldsList);
        Mapping roundTripMapping = IOUtils.deserializeFromJsonString(IOUtils.serializeToJsonString(mapping), Mapping.class);
        assertEquals(mapping, roundTripMapping);
        String[] filters = new String[]{"{\"exists\":{\"field\":\"id\"}}", "{\"match\":{\"id\":1}}"};
        String[] roundTripFilters = IOUtils.deserializeFromJsonString(IOUtils.serializeToJsonString(filters), String[].class);
        assertArrayEquals(filters, roundTripFilters);
    }
}
