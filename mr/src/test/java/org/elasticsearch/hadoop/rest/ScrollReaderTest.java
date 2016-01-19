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
import org.elasticsearch.hadoop.serialization.ScrollReader.ScrollReaderConfig;
import org.elasticsearch.hadoop.serialization.builder.JdkValueReader;
import org.elasticsearch.hadoop.serialization.dto.mapping.Field;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class ScrollReaderTest {

    private boolean readMetadata = false;
    private final String metadataField;
    private final boolean readAsJson = false;
    private final ScrollReaderConfig scrollReaderConfig;
    private ScrollReader reader;

    public ScrollReaderTest(boolean readMetadata, String metadataField) {
        this.readMetadata = readMetadata;
        this.metadataField = metadataField;

        scrollReaderConfig = new ScrollReaderConfig(new JdkValueReader(), null, readMetadata, metadataField, readAsJson, false);
        reader = new ScrollReader(scrollReaderConfig);
    }

    @Test
    public void testScrollWithFields() throws IOException {
        InputStream stream = getClass().getResourceAsStream("scroll-fields.json");
        List<Object[]> read = reader.read(stream).getHits();
        assertEquals(3, read.size());
        Object[] objects = read.get(0);
        assertTrue(((Map) objects[1]).containsKey("fields"));
    }

    @Test
    public void testScrollWithMatchedQueries() throws IOException {
        InputStream stream = getClass().getResourceAsStream("scroll-matched-queries.json");
        List<Object[]> read = reader.read(stream).getHits();
        assertEquals(3, read.size());
        Object[] objects = read.get(0);
        assertTrue(((Map) objects[1]).containsKey("foo"));
    }

    @Test
    public void testScrollWithNestedFields() throws IOException {
        InputStream stream = getClass().getResourceAsStream("scroll-source-mapping.json");
        Field fl = Field.parseField(new ObjectMapper().readValue(stream, Map.class));

        scrollReaderConfig.rootField = fl;
        reader = new ScrollReader(scrollReaderConfig);

        stream = getClass().getResourceAsStream("scroll-source.json");
        List<Object[]> read = reader.read(stream).getHits();

        assertEquals(3, read.size());
        Object[] objects = read.get(0);
        assertTrue(((Map) objects[1]).containsKey("source"));
        Map map = (Map) read.get(2)[1];
        Map number = (Map) map.get("links");
        Object value = number.get("number");
        assertNotNull(value);
        assertTrue(value instanceof Short);
        assertEquals(Short.valueOf((short) 125), value);
    }

    @Test
    public void testScrollWithSource() throws IOException {
        reader = new ScrollReader(scrollReaderConfig);
        InputStream stream = getClass().getResourceAsStream("scroll-source.json");
        List<Object[]> read = reader.read(stream).getHits();
        assertEquals(3, read.size());
        Object[] objects = read.get(0);
        assertTrue(((Map) objects[1]).containsKey("source"));
    }

    @Test
    public void testScrollWithoutSource() throws IOException {
        reader = new ScrollReader(scrollReaderConfig);
        InputStream stream = getClass().getResourceAsStream("empty-source.json");
        List<Object[]> read = reader.read(stream).getHits();
        assertEquals(2, read.size());
        Object[] objects = read.get(0);
        if (readMetadata) {
            assertTrue(((Map) objects[1]).containsKey(metadataField));
        }
        else {
            assertTrue(((Map) objects[1]).isEmpty());
        }
    }

    @Test
    public void testScrollMultiValueList() throws IOException {
        reader = new ScrollReader(scrollReaderConfig);
        InputStream stream = getClass().getResourceAsStream("list-with-null.json");
        List<Object[]> read = reader.read(stream).getHits();
        assertEquals(1, read.size());
        Object[] objects = read.get(0);
        Map map = (Map) read.get(0)[1];
        List links = (List) map.get("links");
        assertTrue(links.contains(null));
    }


    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { Boolean.TRUE, "_metabutu"},
                { Boolean.FALSE, "" } });
    }
}