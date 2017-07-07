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
import org.elasticsearch.hadoop.serialization.dto.mapping.FieldParser;
import org.elasticsearch.hadoop.serialization.dto.mapping.MappingSet;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.elasticsearch.hadoop.serialization.dto.mapping.FieldParser.parseMapping;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
        MappingSet fl = FieldParser.parseMapping(new ObjectMapper().readValue(stream, Map.class));

        scrollReaderConfig.resolvedMapping = fl.getResolvedView();
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

    @Test
    public void testScrollWithJoinField() throws Exception {
        Map value = new ObjectMapper().readValue(getClass().getResourceAsStream("scroll-join-source-mapping.json"), Map.class);
        MappingSet mappings = parseMapping(value);
        // Make our own scroll reader, that ignores unmapped values like the rest of the code
        ScrollReader myReader = new ScrollReader(new ScrollReaderConfig(new JdkValueReader(), mappings.getResolvedView(), readMetadata, metadataField, readAsJson, false));

        InputStream stream = getClass().getResourceAsStream("scroll-join-source.json");
        List<Object[]> read = myReader.read(stream).getHits();
        assertEquals(7, read.size());

        // Elastic
        {
            Object[] row1 = read.get(0);
            assertTrue(((Map) row1[1]).containsKey("id"));
            assertEquals("1", ((Map) row1[1]).get("id"));
            assertTrue(((Map) row1[1]).containsKey("company"));
            assertEquals("Elastic", ((Map) row1[1]).get("company"));
            assertTrue(((Map) row1[1]).containsKey("joiner"));
            Object joinfield1 = ((Map) row1[1]).get("joiner");
            assertTrue(joinfield1 instanceof Map);
            assertTrue(((Map) joinfield1).containsKey("name"));
            assertEquals("company", ((Map) joinfield1).get("name"));
            assertFalse(((Map) joinfield1).containsKey("parent"));
        }

        // kimchy
        {
            Object[] row2 = read.get(1);
            assertTrue(((Map) row2[1]).containsKey("id"));
            assertEquals("10", ((Map) row2[1]).get("id"));
            assertTrue(((Map) row2[1]).containsKey("name"));
            assertEquals("kimchy", ((Map) row2[1]).get("name"));
            assertTrue(((Map) row2[1]).containsKey("joiner"));
            Object joinfield2 = ((Map) row2[1]).get("joiner");
            assertTrue(joinfield2 instanceof Map);
            assertTrue(((Map) joinfield2).containsKey("name"));
            assertEquals("employee", ((Map) joinfield2).get("name"));
            assertTrue(((Map) joinfield2).containsKey("parent"));
            assertEquals("1", ((Map) joinfield2).get("parent"));
        }

        // Fringe Cafe
        {
            Object[] row3 = read.get(2);
            assertTrue(((Map) row3[1]).containsKey("id"));
            assertEquals("2", ((Map) row3[1]).get("id"));
            assertTrue(((Map) row3[1]).containsKey("company"));
            assertEquals("Fringe Cafe", ((Map) row3[1]).get("company"));
            assertTrue(((Map) row3[1]).containsKey("joiner"));
            Object joinfield3 = ((Map) row3[1]).get("joiner");
            assertTrue(joinfield3 instanceof Map);
            assertTrue(((Map) joinfield3).containsKey("name"));
            assertEquals("company", ((Map) joinfield3).get("name"));
            assertFalse(((Map) joinfield3).containsKey("parent"));
        }

        // April Ryan
        {
            Object[] row4 = read.get(3);
            assertTrue(((Map) row4[1]).containsKey("id"));
            assertEquals("20", ((Map) row4[1]).get("id"));
            assertTrue(((Map) row4[1]).containsKey("name"));
            assertEquals("April Ryan", ((Map) row4[1]).get("name"));
            assertTrue(((Map) row4[1]).containsKey("joiner"));
            Object joinfield4 = ((Map) row4[1]).get("joiner");
            assertTrue(joinfield4 instanceof Map);
            assertTrue(((Map) joinfield4).containsKey("name"));
            assertEquals("employee", ((Map) joinfield4).get("name"));
            assertTrue(((Map) joinfield4).containsKey("parent"));
            assertEquals("2", ((Map) joinfield4).get("parent"));
        }

        // Charlie
        {
            Object[] row5 = read.get(4);
            assertTrue(((Map) row5[1]).containsKey("id"));
            assertEquals("21", ((Map) row5[1]).get("id"));
            assertTrue(((Map) row5[1]).containsKey("name"));
            assertEquals("Charlie", ((Map) row5[1]).get("name"));
            assertTrue(((Map) row5[1]).containsKey("joiner"));
            Object joinfield5 = ((Map) row5[1]).get("joiner");
            assertTrue(joinfield5 instanceof Map);
            assertTrue(((Map) joinfield5).containsKey("name"));
            assertEquals("employee", ((Map) joinfield5).get("name"));
            assertTrue(((Map) joinfield5).containsKey("parent"));
            assertEquals("2", ((Map) joinfield5).get("parent"));
        }

        // WATI corp
        {
            Object[] row6 = read.get(5);
            assertTrue(((Map) row6[1]).containsKey("id"));
            assertEquals("3", ((Map) row6[1]).get("id"));
            assertTrue(((Map) row6[1]).containsKey("company"));
            assertEquals("WATIcorp", ((Map) row6[1]).get("company"));
            assertTrue(((Map) row6[1]).containsKey("joiner"));
            Object joinfield6 = ((Map) row6[1]).get("joiner");
            assertTrue(joinfield6 instanceof Map);
            assertTrue(((Map) joinfield6).containsKey("name"));
            assertEquals("company", ((Map) joinfield6).get("name"));
            assertFalse(((Map) joinfield6).containsKey("parent"));
        }

        // Alvin Peats
        {
            Object[] row7 = read.get(6);
            assertTrue(((Map) row7[1]).containsKey("id"));
            assertEquals("30", ((Map) row7[1]).get("id"));
            assertTrue(((Map) row7[1]).containsKey("name"));
            assertEquals("Alvin Peats", ((Map) row7[1]).get("name"));
            assertTrue(((Map) row7[1]).containsKey("joiner"));
            Object joinfield5 = ((Map) row7[1]).get("joiner");
            assertTrue(joinfield5 instanceof Map);
            assertTrue(((Map) joinfield5).containsKey("name"));
            assertEquals("employee", ((Map) joinfield5).get("name"));
            assertTrue(((Map) joinfield5).containsKey("parent"));
            assertEquals("3", ((Map) joinfield5).get("parent"));
        }
    }

    @Test
    public void testScrollWithMultipleTypes() throws Exception {
        Map value = new ObjectMapper().readValue(getClass().getResourceAsStream("scroll-multi-type-source-mapping.json"), Map.class);
        MappingSet mappings = parseMapping(value);
        // Make our own scroll reader, that ignores unmapped values like the rest of the code
        ScrollReader myReader = new ScrollReader(new ScrollReaderConfig(new JdkValueReader(), mappings.getResolvedView(), readMetadata, metadataField, readAsJson, false));

        InputStream stream = getClass().getResourceAsStream("scroll-multi-type-source.json");
        List<Object[]> read = myReader.read(stream).getHits();
        assertEquals(3, read.size());
        Object[] row1 = read.get(0);
        assertTrue(((Map) row1[1]).containsKey("field1"));
        assertEquals("value1", ((Map) row1[1]).get("field1"));
        assertTrue(((Map) row1[1]).containsKey("field2"));
        assertEquals("value2", ((Map) row1[1]).get("field2"));

        Object[] row2 = read.get(1);
        assertTrue(((Map) row2[1]).containsKey("field3"));
        assertEquals("value3", ((Map) row2[1]).get("field3"));

        Object[] row3 = read.get(2);
        assertTrue(((Map) row3[1]).containsKey("field4"));
        assertEquals("value4", ((Map) row3[1]).get("field4"));
    }

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { Boolean.TRUE, "_metabutu"},
                { Boolean.FALSE, "" } });
    }
}