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
package org.elasticsearch.hadoop.serialization;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.hadoop.rest.EsHadoopParsingException;
import org.elasticsearch.hadoop.serialization.builder.JdkValueReader;
import org.elasticsearch.hadoop.serialization.dto.mapping.FieldParser;
import org.elasticsearch.hadoop.serialization.dto.mapping.MappingSet;
import org.elasticsearch.hadoop.util.TestSettings;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.elasticsearch.hadoop.serialization.dto.mapping.FieldParser.parseMapping;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class ScrollReaderJsonTest {

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { Boolean.TRUE, "_metabutu", Boolean.TRUE },
                { Boolean.FALSE, "", Boolean.TRUE } });
    }

    private boolean readMetadata = false;
    private final String metadataField;
    private final boolean readAsJson;
    private final ObjectMapper mapper;

    private ScrollReader reader;

    public ScrollReaderJsonTest(boolean readMetadata, String metadataField, boolean readAsJson) {
        this.readMetadata = readMetadata;
        this.readAsJson = readAsJson;
        this.metadataField = metadataField;
        this.mapper = new ObjectMapper();
        reader = new ScrollReader(getScrollCfg());
    }

    private ScrollReaderConfigBuilder getScrollCfg() {
        return ScrollReaderConfigBuilder.builder(new JdkValueReader(), new TestSettings())
                .setReadMetadata(readMetadata)
                .setMetadataName(metadataField)
                .setReturnRawJson(readAsJson)
                .setIgnoreUnmappedFields(true);
    }

    private String scrollData(String testDataSet) {
        return "scrollReaderTestData/" + testDataSet + "/scroll.json";
    }

    private String mappingData(String testDataSet) {
        return "scrollReaderTestData/" + testDataSet + "/mapping.json";
    }

    @Test
    public void testScrollWithFields() throws IOException {

        InputStream stream = getClass().getResourceAsStream(scrollData("fields"));
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
        InputStream stream = getClass().getResourceAsStream(scrollData("matched-queries"));
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
        InputStream stream = getClass().getResourceAsStream(mappingData("source"));
        MappingSet fl = FieldParser.parseMapping(new ObjectMapper().readValue(stream, Map.class));

        ScrollReaderConfigBuilder scrollCfg = getScrollCfg().setResolvedMapping(fl.getResolvedView());
        reader = new ScrollReader(scrollCfg);
        stream = getClass().getResourceAsStream(scrollData("source"));

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
        InputStream stream = getClass().getResourceAsStream(scrollData("source"));
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
        InputStream stream = getClass().getResourceAsStream(scrollData("empty-source"));
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
        InputStream stream = getClass().getResourceAsStream(scrollData("list"));
        List<Object[]> read = reader.read(stream).getHits();
        assertEquals(1, read.size());
        Object[] objects = read.get(0);
        String doc = objects[1].toString();

        Map value = mapper.readValue(doc, Map.class);

        assertTrue(value.containsKey("name"));

        List links = (List) value.get("links");
        assertTrue(links.contains(null));

        if (readMetadata) {
            assertTrue(value.containsKey(metadataField));
            assertEquals("aqOqDwYnTA29J1gfy3m8_Q", ((Map) value.get(metadataField)).get("_id"));
        }
    }

    @Test
    public void testScrollWithJoinField() throws Exception {
        Map value = new ObjectMapper().readValue(getClass().getResourceAsStream(mappingData("join")), Map.class);
        MappingSet mappings = parseMapping(value);
        // Make our own scroll reader, that ignores unmapped values like the rest of the code
        ScrollReaderConfigBuilder scrollCfg = getScrollCfg().setResolvedMapping(mappings.getResolvedView()).setIgnoreUnmappedFields(false);
        ScrollReader myReader = new ScrollReader(scrollCfg);

        InputStream stream = getClass().getResourceAsStream(scrollData("join"));
        List<Object[]> read = myReader.read(stream).getHits();
        assertEquals(7, read.size());

        {
            String row = (String) read.get(0)[1];
            assertTrue(row.contains("\"joiner\": \"company\""));
        }

        {
            String row = (String) read.get(1)[1];
            assertTrue(row.contains("joiner"));
            assertTrue(row.contains("\"name\": \"employee\""));
            assertTrue(row.contains("\"parent\": \"1\""));
        }

        {
            String row = (String) read.get(2)[1];
            assertTrue(row.contains("\"joiner\": \"company\""));
        }

        {
            String row = (String) read.get(3)[1];
            assertTrue(row.contains("joiner"));
            assertTrue(row.contains("\"name\": \"employee\""));
            assertTrue(row.contains("\"parent\": \"2\""));
        }

        {
            String row = (String) read.get(4)[1];
            assertTrue(row.contains("joiner"));
            assertTrue(row.contains("\"name\": \"employee\""));
            assertTrue(row.contains("\"parent\": \"2\""));
        }

        {
            String row = (String) read.get(5)[1];
            assertTrue(row.contains("\"joiner\": \"company\""));
        }

        {
            String row = (String) read.get(6)[1];
            assertTrue(row.contains("joiner"));
            assertTrue(row.contains("\"name\": \"employee\""));
            assertTrue(row.contains("\"parent\": \"3\""));
        }
    }

    @Test(expected = EsHadoopParsingException.class)
    public void testScrollWithParsingValueException() throws IOException {
        InputStream stream = getClass().getResourceAsStream(mappingData("numbers-as-strings"));
        MappingSet fl = FieldParser.parseMapping(new ObjectMapper().readValue(stream, Map.class));
        ScrollReaderConfigBuilder scrollCfg = getScrollCfg()
                .setResolvedMapping(fl.getResolvedView())
                .setReturnRawJson(false);
        // parsing the doc (don't just read it as json) yields parsing exception
        reader = new ScrollReader(scrollCfg);
        stream = getClass().getResourceAsStream(scrollData("numbers-as-strings"));
        List<Object[]> read = reader.read(stream).getHits();
    }
}