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
package org.elasticsearch.hadoop.rest;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.hadoop.serialization.ScrollReader;
import org.elasticsearch.hadoop.serialization.builder.JdkValueReader;
import org.elasticsearch.hadoop.serialization.dto.mapping.Field;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class ScrollReaderJsonTest {

    private boolean readMetadata = false;
    private String metadataField;
    private boolean readAsJson = true;
    private ObjectMapper mapper;

    public ScrollReaderJsonTest(boolean readMetadata, String metadataField) {
        this.readMetadata = readMetadata;
        this.metadataField = metadataField;
        this.mapper = new ObjectMapper();
    }

    @Test
    public void testScrollWithFields() throws IOException {
        ScrollReader reader = new ScrollReader(new JdkValueReader(), null, readMetadata, metadataField, readAsJson);
        InputStream stream = getClass().getResourceAsStream("scroll-fields.json");
        List<Object[]> read = reader.read(stream).getHits();
        assertEquals(3, read.size());
        Object[] objects = read.get(0);
        String doc = objects[1].toString();
        Map value = mapper.readValue(doc, Map.class);
        assertTrue(value.containsKey("fields"));
        if (readMetadata) {
            assertTrue(value.containsKey(metadataField));
            assertEquals("23hrGo7VRCyao8lB9Uu5Kw", ((Map) value.get(metadataField)).get("_id"));
        }
    }

    @Test
    public void testScrollWithMatchedQueries() throws IOException {
        ScrollReader reader = new ScrollReader(new JdkValueReader(), null, readMetadata, metadataField, readAsJson);
        InputStream stream = getClass().getResourceAsStream("scroll-matched-queries.json");
        List<Object[]> read = reader.read(stream).getHits();
        assertEquals(3, read.size());
        Object[] objects = read.get(1);
        String doc = objects[1].toString();

        Map value = mapper.readValue(doc, Map.class);
        assertTrue(value.containsKey("links"));
        if (readMetadata) {
            assertTrue(value.containsKey(metadataField));
            assertEquals(Arrays.asList("myquery"), ((Map) value.get(metadataField)).get("matched_queries"));
        }
    }

    @Test
    public void testScrollWithNestedFields() throws IOException {
        InputStream stream = getClass().getResourceAsStream("scroll-source-mapping.json");
        Field fl = Field.parseField(new ObjectMapper().readValue(stream, Map.class));
        ScrollReader reader = new ScrollReader(new JdkValueReader(), fl, readMetadata, metadataField, readAsJson);
        stream = getClass().getResourceAsStream("scroll-source.json");

        List<Object[]> read = reader.read(stream).getHits();

        assertEquals(3, read.size());
        Object[] objects = read.get(2);
        String doc = objects[1].toString();

        Map value = mapper.readValue(doc, Map.class);
        assertTrue(value.containsKey("links"));
        // the raw json is returned which ignored mapping
        assertEquals("125", ((Map) value.get("links")).get("number"));
    }

    @Test
    public void testScrollWithSource() throws IOException {
        ScrollReader reader = new ScrollReader(new JdkValueReader(), null, readMetadata, metadataField, readAsJson);
        InputStream stream = getClass().getResourceAsStream("scroll-source.json");
        List<Object[]> read = reader.read(stream).getHits();
        assertEquals(3, read.size());
        Object[] objects = read.get(0);

        String doc = objects[1].toString();
        Map value = mapper.readValue(doc, Map.class);
        assertEquals("source", value.get("source"));
        if (readMetadata) {
            assertTrue(value.containsKey(metadataField));
            assertEquals("23hrGo7VRCyao8lB9Uu5Kw", ((Map) value.get(metadataField)).get("_id"));
        }
    }

    @Test
    public void testScrollWithoutSource() throws IOException {
        ScrollReader reader = new ScrollReader(new JdkValueReader(), null, readMetadata, metadataField, readAsJson);
        InputStream stream = getClass().getResourceAsStream("empty-source.json");
        List<Object[]> read = reader.read(stream).getHits();
        assertEquals(2, read.size());
        Object[] objects = read.get(1);
        String doc = objects[1].toString();

        Map value = mapper.readValue(doc, Map.class);

        if (!readMetadata) {
            assertTrue(value.isEmpty());
        }
        else {
            assertTrue(value.containsKey(metadataField));
            assertEquals("PTi2NxdDRxmXhv6S8DgIeQ", ((Map) value.get(metadataField)).get("_id"));
        }
    }

    @Test
    public void testScrollMultiValueList() throws IOException {
        ScrollReader reader = new ScrollReader(new JdkValueReader(), null, readMetadata, metadataField, readAsJson);
        InputStream stream = getClass().getResourceAsStream("list-with-null.json");
        List<Object[]> read = reader.read(stream).getHits();
        assertEquals(1, read.size());
        Object[] objects = read.get(0);
        String doc = objects[1].toString();

        System.out.println(doc);

        Map value = mapper.readValue(doc, Map.class);

        assertTrue(value.containsKey("name"));

        List links = (List) value.get("links");
        assertTrue(links.contains(null));

        if (readMetadata) {
            assertTrue(value.containsKey(metadataField));
            assertEquals("aqOqDwYnTA29J1gfy3m8_Q", ((Map) value.get(metadataField)).get("_id"));
        }
    }


    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { Boolean.TRUE, "_metabutu" },
                { Boolean.FALSE, "" },
                { Boolean.TRUE, "_metabutu" },
                { Boolean.FALSE, "" } });
    }
}