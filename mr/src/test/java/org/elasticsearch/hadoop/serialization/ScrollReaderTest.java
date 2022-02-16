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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.elasticsearch.hadoop.EsHadoopException;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.handler.ErrorCollector;
import org.elasticsearch.hadoop.handler.EsHadoopAbortHandlerException;
import org.elasticsearch.hadoop.handler.HandlerResult;
import org.elasticsearch.hadoop.rest.EsHadoopParsingException;
import org.elasticsearch.hadoop.serialization.builder.JdkValueReader;
import org.elasticsearch.hadoop.serialization.dto.mapping.FieldParser;
import org.elasticsearch.hadoop.serialization.dto.mapping.MappingSet;
import org.elasticsearch.hadoop.serialization.handler.read.DeserializationErrorHandler;
import org.elasticsearch.hadoop.serialization.handler.read.DeserializationFailure;
import org.elasticsearch.hadoop.serialization.handler.read.impl.DeserializationHandlerLoader;
import org.elasticsearch.hadoop.util.ObjectUtils;
import org.elasticsearch.hadoop.util.TestSettings;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class ScrollReaderTest {

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { Boolean.TRUE, "_metabutu"},
                { Boolean.FALSE, "" } });
    }

    private boolean readMetadata = false;
    private final String metadataField;
    private final boolean readAsJson = false;

    private ScrollReader reader;

    public ScrollReaderTest(boolean readMetadata, String metadataField) {
        this.readMetadata = readMetadata;
        this.metadataField = metadataField;

        reader = new ScrollReader(getScrollReaderCfg());
    }

    private ScrollReaderConfigBuilder getScrollReaderCfg() {
        return ScrollReaderConfigBuilder.builder(new JdkValueReader(), new TestSettings())
                .setReadMetadata(readMetadata)
                .setMetadataName(metadataField)
                .setReturnRawJson(readAsJson)
                .setIgnoreUnmappedFields(false)
                .setIncludeFields(Collections.<String>emptyList())
                .setExcludeFields(Collections.<String>emptyList())
                .setIncludeArrayFields(Collections.<String>emptyList());
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
        assertTrue(((Map) objects[1]).containsKey("fields"));
    }

    @Test
    public void testScrollWithMatchedQueries() throws IOException {
        InputStream stream = getClass().getResourceAsStream(scrollData("matched-queries"));
        List<Object[]> read = reader.read(stream).getHits();
        assertEquals(3, read.size());
        Object[] objects = read.get(0);
        assertTrue(((Map) objects[1]).containsKey("foo"));
    }

    @Test
    public void testScrollWithNestedFields() throws IOException {
        MappingSet fl = getMappingSet("source");

        ScrollReaderConfigBuilder scrollReaderConfig = getScrollReaderCfg().setResolvedMapping(fl.getResolvedView());
        reader = new ScrollReader(scrollReaderConfig);

        InputStream stream = getClass().getResourceAsStream(scrollData("source"));
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
        reader = new ScrollReader(getScrollReaderCfg());
        InputStream stream = getClass().getResourceAsStream(scrollData("source"));
        List<Object[]> read = reader.read(stream).getHits();
        assertEquals(3, read.size());
        Object[] objects = read.get(0);
        assertTrue(((Map) objects[1]).containsKey("source"));
    }

    @Test
    public void testScrollWithoutSource() throws IOException {
        reader = new ScrollReader(getScrollReaderCfg());
        InputStream stream = getClass().getResourceAsStream(scrollData("empty-source"));
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
        reader = new ScrollReader(getScrollReaderCfg());
        InputStream stream = getClass().getResourceAsStream(scrollData("list"));
        List<Object[]> read = reader.read(stream).getHits();
        assertEquals(1, read.size());
        Object[] objects = read.get(0);
        Map map = (Map) read.get(0)[1];
        List links = (List) map.get("links");
        assertTrue(links.contains(null));
    }

    @Test
    public void testScrollWithJoinField() throws Exception {
        MappingSet mappings = getMappingSet("join");
        // Make our own scroll reader, that ignores unmapped values like the rest of the code
        ScrollReaderConfigBuilder scrollCfg = getScrollReaderCfg().setResolvedMapping(mappings.getResolvedView());
        ScrollReader myReader = new ScrollReader(scrollCfg);

        InputStream stream = getClass().getResourceAsStream(scrollData("join"));
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
        MappingSet mappings = getLegacyMappingSet("multi-type");
        // Make our own scroll reader, that ignores unmapped values like the rest of the code
        ScrollReaderConfigBuilder scrollCfg = getScrollReaderCfg().setResolvedMapping(mappings.getResolvedView());
        ScrollReader myReader = new ScrollReader(scrollCfg);

        InputStream stream = getClass().getResourceAsStream(scrollData("multi-type"));
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

    /**
     * Loads and parses the mapping response content with include_type_name set to true to simulate older ES cluster response formats.
     * @param s mapping file name
     * @return MappingSet with type names loaded
     */
    private MappingSet getLegacyMappingSet(String s) {
        return FieldParser.parseTypedMappings(JsonUtils.asMap(getClass().getResourceAsStream(mappingData(s))));
    }

    /**
     * Loads and parses the mapping response content located in the resource file based on the given string
     * @param s the mapping file name
     * @return MappingSet that has been loaded from the response body in that resource file.
     */
    private MappingSet getMappingSet(String s) {
        return FieldParser.parseTypelessMappings(JsonUtils.asMap(getClass().getResourceAsStream(mappingData(s))));
    }

    @Test
    public void testScrollWithNestedFieldAndArrayIncludes() throws IOException {
        MappingSet mappings = getMappingSet("nested-data");

        InputStream stream = getClass().getResourceAsStream(scrollData("nested-data"));

        Settings testSettings = new TestSettings();
        testSettings.setProperty(ConfigurationOptions.ES_READ_FIELD_AS_ARRAY_INCLUDE, "a.b.d:2,a.b.f");
        testSettings.setProperty(ConfigurationOptions.ES_READ_METADATA, "" + readMetadata);
        testSettings.setProperty(ConfigurationOptions.ES_READ_METADATA_FIELD, "" + metadataField);
        testSettings.setProperty(ConfigurationOptions.ES_OUTPUT_JSON, "" + readAsJson);

        JdkValueReader valueReader = ObjectUtils.instantiate(JdkValueReader.class.getName(), testSettings);

        ScrollReader reader = new ScrollReader(ScrollReaderConfigBuilder.builder(valueReader, mappings.getResolvedView(), testSettings));

        ScrollReader.Scroll scroll = reader.read(stream);
        // First document in scroll has a single object on the nested field.
        assertEquals("yo", JsonUtils.query("a").get("b").get(0).get("c").apply(scroll.getHits().get(0)[1]));
        // Second document in scroll has a nested document field
        assertEquals("hello", JsonUtils.query("a").get("b").get(0).get("c").apply(scroll.getHits().get(1)[1]));
        // Third document in scroll has a nested document with an array field. This array field should be padded to meet
        // the configured array depth in the include setting above. This should be independent of the fact that the nested
        // object will be added to a different array further up the call stack.
        assertEquals("howdy", JsonUtils.query("a").get("b").get(0).get("d").get(0).get(0).apply(scroll.getHits().get(2)[1]));
        assertEquals("partner", JsonUtils.query("a").get("b").get(0).get("d").get(0).get(1).apply(scroll.getHits().get(2)[1]));

        // Fourth document has some nested arrays next to more complex datatypes
        assertEquals(1L, JsonUtils.query("a").get("b").get(0).get("f").get(0).apply(scroll.getHits().get(3)[1]));
        assertEquals(ISODateTimeFormat.dateParser().parseDateTime("2015-01-01").toDate(), JsonUtils.query("a").get("b").get(0).get("e").apply(scroll.getHits().get(3)[1]));
        assertEquals(3L, JsonUtils.query("a").get("b").get(1).get("f").get(0).apply(scroll.getHits().get(3)[1]));
    }

    @Test
    public void testScrollWithObjectFieldAndArrayIncludes() throws IOException {
        MappingSet mappings = getMappingSet("object-fields");

        InputStream stream = getClass().getResourceAsStream(scrollData("object-fields"));

        Settings testSettings = new TestSettings();
        testSettings.setProperty(ConfigurationOptions.ES_READ_FIELD_AS_ARRAY_INCLUDE, "a.b.d:2,a.b,a.b.f");
        testSettings.setProperty(ConfigurationOptions.ES_READ_METADATA, "" + readMetadata);
        testSettings.setProperty(ConfigurationOptions.ES_READ_METADATA_FIELD, "" + metadataField);
        testSettings.setProperty(ConfigurationOptions.ES_OUTPUT_JSON, "" + readAsJson);

        JdkValueReader valueReader = ObjectUtils.instantiate(JdkValueReader.class.getName(), testSettings);

        ScrollReader reader = new ScrollReader(ScrollReaderConfigBuilder.builder(valueReader, mappings.getResolvedView(), testSettings));

        ScrollReader.Scroll scroll = reader.read(stream);
        // First document in scroll has a single object on the nested field.
        assertEquals("yo", JsonUtils.query("a").get("b").get(0).get("c").apply(scroll.getHits().get(0)[1]));
        // Second document in scroll has a nested document field
        assertEquals("hello", JsonUtils.query("a").get("b").get(0).get("c").apply(scroll.getHits().get(1)[1]));
        // Third document in scroll has a nested document with an array field. This array field should be padded to meet
        // the configured array depth in the include setting above. This should be independent of the fact that the nested
        // object will be added to a different array further up the call stack.
        assertEquals("howdy", JsonUtils.query("a").get("b").get(0).get("d").get(0).get(0).apply(scroll.getHits().get(2)[1]));
        assertEquals("partner", JsonUtils.query("a").get("b").get(0).get("d").get(0).get(1).apply(scroll.getHits().get(2)[1]));

        // Fourth document has some nested arrays next to more complex datatypes
        assertEquals(1L, JsonUtils.query("a").get("b").get(0).get("f").get(0).apply(scroll.getHits().get(3)[1]));
        assertEquals(ISODateTimeFormat.dateParser().parseDateTime("2015-01-01").toDate(), JsonUtils.query("a").get("b").get(0).get("e").apply(scroll.getHits().get(3)[1]));
        assertEquals(3L, JsonUtils.query("a").get("b").get(1).get("f").get(0).apply(scroll.getHits().get(3)[1]));
    }

    @Test
    public void testScrollWithNestedArrays() throws IOException {
        MappingSet mappings = getMappingSet("nested-list");

        InputStream stream = getClass().getResourceAsStream(scrollData("nested-list"));

        Settings testSettings = new TestSettings();
        testSettings.setProperty(ConfigurationOptions.ES_READ_FIELD_AS_ARRAY_INCLUDE, "a:3");
        testSettings.setProperty(ConfigurationOptions.ES_READ_METADATA, "" + readMetadata);
        testSettings.setProperty(ConfigurationOptions.ES_READ_METADATA_FIELD, "" + metadataField);
        testSettings.setProperty(ConfigurationOptions.ES_OUTPUT_JSON, "" + readAsJson);

        JdkValueReader valueReader = ObjectUtils.instantiate(JdkValueReader.class.getName(), testSettings);


        ScrollReader reader = new ScrollReader(ScrollReaderConfigBuilder.builder(valueReader, mappings.getResolvedView(), testSettings));

        ScrollReader.Scroll scroll = reader.read(stream);
        // Case of already correctly nested array data
        assertEquals(1L, JsonUtils.query("a").get(0).get(0).get(0).apply(scroll.getHits().get(0)[1]));
        // Case of insufficiently nested array data
        assertEquals(9L, JsonUtils.query("a").get(0).get(0).get(0).apply(scroll.getHits().get(1)[1]));
        // Case of singleton data that is not nested in ANY array levels.
        assertEquals(10L, JsonUtils.query("a").get(0).get(0).get(0).apply(scroll.getHits().get(2)[1]));
    }

    @Test
    public void testScrollWithEmptyrrays() throws IOException {
        MappingSet mappings = getMappingSet("empty-list");
        InputStream stream = getClass().getResourceAsStream(scrollData("empty-list"));
        Settings testSettings = new TestSettings();
        testSettings.setProperty(ConfigurationOptions.ES_READ_FIELD_AS_ARRAY_INCLUDE, "status_code");
        testSettings.setProperty(ConfigurationOptions.ES_READ_METADATA, "" + readMetadata);
        testSettings.setProperty(ConfigurationOptions.ES_READ_METADATA_FIELD, "" + metadataField);
        testSettings.setProperty(ConfigurationOptions.ES_OUTPUT_JSON, "" + readAsJson);
        JdkValueReader valueReader = ObjectUtils.instantiate(JdkValueReader.class.getName(), testSettings);
        ScrollReader reader = new ScrollReader(ScrollReaderConfigBuilder.builder(valueReader, mappings.getResolvedView(), testSettings));
        ScrollReader.Scroll scroll = reader.read(stream);
        // The first entry is null. The second is an array with the single element '123'. And the third is an empty array
        assertNull(JsonUtils.query("status_code").apply(scroll.getHits().get(0)[1]));
        assertEquals(123L, JsonUtils.query("status_code").get(0).apply(scroll.getHits().get(1)[1]));
        assertEquals(Collections.emptyList(), JsonUtils.query("status_code").apply(scroll.getHits().get(2)[1]));
    }

    @Test(expected = EsHadoopParsingException.class)
    public void testScrollWithBreakOnInvalidMapping() throws IOException {
        MappingSet mappings = getMappingSet("numbers-as-strings");

        InputStream stream = getClass().getResourceAsStream(scrollData("numbers-as-strings"));

        Settings testSettings = new TestSettings();
        testSettings.setProperty(ConfigurationOptions.ES_READ_METADATA, "" + readMetadata);
        testSettings.setProperty(ConfigurationOptions.ES_READ_METADATA_FIELD, "" + metadataField);
        testSettings.setProperty(ConfigurationOptions.ES_OUTPUT_JSON, "" + readAsJson);

        JdkValueReader valueReader = ObjectUtils.instantiate(JdkValueReader.class.getName(), testSettings);

        ScrollReader reader = new ScrollReader(ScrollReaderConfigBuilder.builder(valueReader, mappings.getResolvedView(), testSettings));

        reader.read(stream);
        fail("Should not be able to parse string as long");
    }

    @Test(expected = EsHadoopException.class)
    public void testScrollWithThrowingErrorHandler() throws IOException {
        MappingSet mappings = getMappingSet("numbers-as-strings");

        InputStream stream = getClass().getResourceAsStream(scrollData("numbers-as-strings"));

        Settings testSettings = new TestSettings();
        testSettings.setProperty(ConfigurationOptions.ES_READ_METADATA, "" + readMetadata);
        testSettings.setProperty(ConfigurationOptions.ES_READ_METADATA_FIELD, "" + metadataField);
        testSettings.setProperty(ConfigurationOptions.ES_OUTPUT_JSON, "" + readAsJson);
        testSettings.setProperty(DeserializationHandlerLoader.ES_READ_DATA_ERROR_HANDLERS , "throw");
        testSettings.setProperty(DeserializationHandlerLoader.ES_READ_DATA_ERROR_HANDLER + ".throw" , ExceptionThrowingHandler.class.getName());

        JdkValueReader valueReader = ObjectUtils.instantiate(JdkValueReader.class.getName(), testSettings);

        ScrollReader reader = new ScrollReader(ScrollReaderConfigBuilder.builder(valueReader, mappings.getResolvedView(), testSettings));

        reader.read(stream);
        fail("Should not be able to parse string as long");
    }

    @Test(expected = EsHadoopParsingException.class)
    public void testScrollWithThrowingAbortErrorHandler() throws IOException {
        MappingSet mappings = getMappingSet("numbers-as-strings");

        InputStream stream = getClass().getResourceAsStream(scrollData("numbers-as-strings"));

        Settings testSettings = new TestSettings();
        testSettings.setProperty(ConfigurationOptions.ES_READ_METADATA, "" + readMetadata);
        testSettings.setProperty(ConfigurationOptions.ES_READ_METADATA_FIELD, "" + metadataField);
        testSettings.setProperty(ConfigurationOptions.ES_OUTPUT_JSON, "" + readAsJson);
        testSettings.setProperty(DeserializationHandlerLoader.ES_READ_DATA_ERROR_HANDLERS , "throw");
        testSettings.setProperty(DeserializationHandlerLoader.ES_READ_DATA_ERROR_HANDLER + ".throw" , AbortingExceptionThrowingHandler.class.getName());

        JdkValueReader valueReader = ObjectUtils.instantiate(JdkValueReader.class.getName(), testSettings);

        ScrollReader reader = new ScrollReader(ScrollReaderConfigBuilder.builder(valueReader, mappings.getResolvedView(), testSettings));

        reader.read(stream);
        fail("Should not be able to parse string as long");
    }

    @Test(expected = EsHadoopException.class)
    public void testScrollWithNeverendingHandler() throws IOException {
        MappingSet mappings = getMappingSet("numbers-as-strings");

        InputStream stream = getClass().getResourceAsStream(scrollData("numbers-as-strings"));

        Settings testSettings = new TestSettings();
        testSettings.setProperty(ConfigurationOptions.ES_READ_METADATA, "" + readMetadata);
        testSettings.setProperty(ConfigurationOptions.ES_READ_METADATA_FIELD, "" + metadataField);
        testSettings.setProperty(ConfigurationOptions.ES_OUTPUT_JSON, "" + readAsJson);
        testSettings.setProperty(DeserializationHandlerLoader.ES_READ_DATA_ERROR_HANDLERS , "evil");
        testSettings.setProperty(DeserializationHandlerLoader.ES_READ_DATA_ERROR_HANDLER + ".evil" , NeverSurrenderHandler.class.getName());

        JdkValueReader valueReader = ObjectUtils.instantiate(JdkValueReader.class.getName(), testSettings);

        ScrollReader reader = new ScrollReader(ScrollReaderConfigBuilder.builder(valueReader, mappings.getResolvedView(), testSettings));

        reader.read(stream);
        fail("Should not be able to parse string as long");
    }

    @Test
    public void testScrollWithIgnoringHandler() throws IOException {
        MappingSet mappings = getMappingSet("numbers-as-strings");

        InputStream stream = getClass().getResourceAsStream(scrollData("numbers-as-strings"));

        Settings testSettings = new TestSettings();
        testSettings.setProperty(ConfigurationOptions.ES_READ_METADATA, "" + readMetadata);
        testSettings.setProperty(ConfigurationOptions.ES_READ_METADATA_FIELD, "" + metadataField);
        testSettings.setProperty(ConfigurationOptions.ES_OUTPUT_JSON, "" + readAsJson);
        testSettings.setProperty(DeserializationHandlerLoader.ES_READ_DATA_ERROR_HANDLERS , "skipskipskip");
        testSettings.setProperty(DeserializationHandlerLoader.ES_READ_DATA_ERROR_HANDLER + ".skipskipskip" , NothingToSeeHereHandler.class.getName());

        JdkValueReader valueReader = ObjectUtils.instantiate(JdkValueReader.class.getName(), testSettings);

        ScrollReader reader = new ScrollReader(ScrollReaderConfigBuilder.builder(valueReader, mappings.getResolvedView(), testSettings));

        ScrollReader.Scroll scroll = reader.read(stream);

        assertThat(scroll.getTotalHits(), equalTo(196L));
        assertThat(scroll.getHits(), is(empty()));
    }

    @Test
    public void testScrollWithHandlersThatPassWithMessages() throws IOException {
        MappingSet mappings = getMappingSet("numbers-as-strings");

        InputStream stream = getClass().getResourceAsStream(scrollData("numbers-as-strings"));

        Settings testSettings = new TestSettings();
        testSettings.setProperty(ConfigurationOptions.ES_READ_METADATA, "" + readMetadata);
        testSettings.setProperty(ConfigurationOptions.ES_READ_METADATA_FIELD, "" + metadataField);
        testSettings.setProperty(ConfigurationOptions.ES_OUTPUT_JSON, "" + readAsJson);
        testSettings.setProperty(DeserializationHandlerLoader.ES_READ_DATA_ERROR_HANDLERS , "marco,polo,skip");
        testSettings.setProperty(DeserializationHandlerLoader.ES_READ_DATA_ERROR_HANDLER + ".marco" , MarcoHandler.class.getName());
        testSettings.setProperty(DeserializationHandlerLoader.ES_READ_DATA_ERROR_HANDLER + ".polo" , PoloHandler.class.getName());
        testSettings.setProperty(DeserializationHandlerLoader.ES_READ_DATA_ERROR_HANDLER + ".skip" , NothingToSeeHereHandler.class.getName());

        JdkValueReader valueReader = ObjectUtils.instantiate(JdkValueReader.class.getName(), testSettings);

        ScrollReader reader = new ScrollReader(ScrollReaderConfigBuilder.builder(valueReader, mappings.getResolvedView(), testSettings));

        ScrollReader.Scroll scroll = reader.read(stream);

        assertThat(scroll.getTotalHits(), equalTo(196L));
        assertThat(scroll.getHits(), is(empty()));
    }

    @Test
    public void testScrollWithHandlersThatCorrectsError() throws IOException {
        MappingSet mappings = getMappingSet("numbers-as-strings");

        InputStream stream = getClass().getResourceAsStream(scrollData("numbers-as-strings"));

        Settings testSettings = new TestSettings();
        testSettings.setProperty(ConfigurationOptions.ES_READ_METADATA, "" + readMetadata);
        testSettings.setProperty(ConfigurationOptions.ES_READ_METADATA_FIELD, "" + metadataField);
        testSettings.setProperty(ConfigurationOptions.ES_OUTPUT_JSON, "" + readAsJson);
        testSettings.setProperty(DeserializationHandlerLoader.ES_READ_DATA_ERROR_HANDLERS , "fix");
        testSettings.setProperty(DeserializationHandlerLoader.ES_READ_DATA_ERROR_HANDLER + ".fix" , CorrectingHandler.class.getName());

        JdkValueReader valueReader = ObjectUtils.instantiate(JdkValueReader.class.getName(), testSettings);

        ScrollReader reader = new ScrollReader(ScrollReaderConfigBuilder.builder(valueReader, mappings.getResolvedView(), testSettings));

        ScrollReader.Scroll scroll = reader.read(stream);

        assertThat(scroll.getTotalHits(), equalTo(196L));
        assertThat(scroll.getHits().size(), equalTo(1));
        assertEquals(4L, JsonUtils.query("number").apply(scroll.getHits().get(0)[1]));
    }

    @Test
    public void testNoScrollIdFromFrozenIndex() throws IOException {
        MappingSet mappings = getMappingSet("numbers-as-strings"); // The schema doesn't matter since there's no data
        InputStream stream = getClass().getResourceAsStream(scrollData("no-scroll-id"));
        Settings testSettings = new TestSettings();
        testSettings.setProperty(ConfigurationOptions.ES_READ_METADATA, "" + readMetadata);
        testSettings.setProperty(ConfigurationOptions.ES_READ_METADATA_FIELD, "" + metadataField);
        testSettings.setProperty(ConfigurationOptions.ES_OUTPUT_JSON, "" + readAsJson);
        testSettings.setProperty(DeserializationHandlerLoader.ES_READ_DATA_ERROR_HANDLERS , "fix");
        testSettings.setProperty(DeserializationHandlerLoader.ES_READ_DATA_ERROR_HANDLER + ".fix" , CorrectingHandler.class.getName());
        JdkValueReader valueReader = ObjectUtils.instantiate(JdkValueReader.class.getName(), testSettings);
        ScrollReader reader = new ScrollReader(ScrollReaderConfigBuilder.builder(valueReader, mappings.getResolvedView(), testSettings));
        ScrollReader.Scroll scroll = reader.read(stream);
        assertNull(scroll);
    }

    /**
     * Case: Handler throws random Exceptions
     * Outcome: Processing fails fast.
     */
    public static class ExceptionThrowingHandler extends DeserializationErrorHandler {
        @Override
        public HandlerResult onError(DeserializationFailure entry, ErrorCollector<byte[]> collector) throws Exception {
            throw new IllegalArgumentException("Whoopsie!");
        }
    }

    /**
     * Case: Handler throws exception, wrapped in abort based exception
     * Outcome: Exception is collected and used as the reason for aborting that specific document.
     */
    public static class AbortingExceptionThrowingHandler extends DeserializationErrorHandler {
        @Override
        public HandlerResult onError(DeserializationFailure entry, ErrorCollector<byte[]> collector) throws Exception {
            throw new EsHadoopAbortHandlerException("Abort the handler!!");
        }
    }

    /**
     * Case: Evil or incorrect handler causes infinite loop.
     */
    public static class NeverSurrenderHandler extends DeserializationErrorHandler {
        @Override
        public HandlerResult onError(DeserializationFailure entry, ErrorCollector<byte[]> collector) throws Exception {
            return collector.retry(); // NEVER GIVE UP
        }
    }

    /**
     * Case: Handler acks the failure and expects the processing to move along.
     */
    public static class NothingToSeeHereHandler extends DeserializationErrorHandler {
        @Override
        public HandlerResult onError(DeserializationFailure entry, ErrorCollector<byte[]> collector) throws Exception {
            return HandlerResult.HANDLED; // Move along.
        }
    }

    /**
     * Case: Handler passes on the failure, setting a "message for why"
     */
    public static class MarcoHandler extends DeserializationErrorHandler {
        @Override
        public HandlerResult onError(DeserializationFailure entry, ErrorCollector<byte[]> collector) throws Exception {
            return collector.pass("MARCO!");
        }
    }

    /**
     * Case: Handler checks the pass messages and ensures that they have been set.
     * Outcome: If set, it acks and continues, and if not, it aborts.
     */
    public static class PoloHandler extends DeserializationErrorHandler {
        @Override
        public HandlerResult onError(DeserializationFailure entry, ErrorCollector<byte[]> collector) throws Exception {
            if (entry.previousHandlerMessages().contains("MARCO!")) {
                return collector.pass("POLO!");
            }
            throw new EsHadoopAbortHandlerException("FISH OUT OF WATER!");
        }
    }

    /**
     * Case: Handler somehow knows how to fix data.
     * Outcome: Data is deserialized correctly.
     */
    public static class CorrectingHandler extends DeserializationErrorHandler {
        @Override
        public HandlerResult onError(DeserializationFailure entry, ErrorCollector<byte[]> collector) throws Exception {
            entry.getException().printStackTrace();
            return collector.retry("{\"_index\":\"pig\",\"_type\":\"tupleartists\",\"_id\":\"23hrGo7VRCyao8lB9Uu5Kw\",\"_score\":0.0,\"_source\":{\"number\":4}}".getBytes());
        }
    }
}