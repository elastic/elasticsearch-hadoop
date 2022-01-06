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
package org.elasticsearch.hadoop.serialization.dto.mapping;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.thirdparty.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.lang.System.out;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.elasticsearch.hadoop.serialization.FieldType.BINARY;
import static org.elasticsearch.hadoop.serialization.FieldType.BOOLEAN;
import static org.elasticsearch.hadoop.serialization.FieldType.BYTE;
import static org.elasticsearch.hadoop.serialization.FieldType.DATE;
import static org.elasticsearch.hadoop.serialization.FieldType.DATE_NANOS;
import static org.elasticsearch.hadoop.serialization.FieldType.DOUBLE;
import static org.elasticsearch.hadoop.serialization.FieldType.FLOAT;
import static org.elasticsearch.hadoop.serialization.FieldType.GEO_POINT;
import static org.elasticsearch.hadoop.serialization.FieldType.HALF_FLOAT;
import static org.elasticsearch.hadoop.serialization.FieldType.INTEGER;
import static org.elasticsearch.hadoop.serialization.FieldType.JOIN;
import static org.elasticsearch.hadoop.serialization.FieldType.KEYWORD;
import static org.elasticsearch.hadoop.serialization.FieldType.LONG;
import static org.elasticsearch.hadoop.serialization.FieldType.NESTED;
import static org.elasticsearch.hadoop.serialization.FieldType.OBJECT;
import static org.elasticsearch.hadoop.serialization.FieldType.SCALED_FLOAT;
import static org.elasticsearch.hadoop.serialization.FieldType.SHORT;
import static org.elasticsearch.hadoop.serialization.FieldType.STRING;
import static org.elasticsearch.hadoop.serialization.FieldType.TEXT;
import static org.elasticsearch.hadoop.serialization.FieldType.WILDCARD;

import static org.elasticsearch.hadoop.serialization.dto.mapping.MappingUtils.findTypos;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assume.assumeThat;
import static org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class MappingTest {

    @Parameters
    public static Collection<Object[]> getParameters() {
        List<Object[]> parameters = new ArrayList<Object[]>();
        parameters.add(new Object[]{false, "typed"});
        parameters.add(new Object[]{true, "typeless"});
        return parameters;
    }

    boolean typeless;
    String mappingDirectory;

    public MappingTest(boolean typeless, String mappingTypes) {
        this.typeless = typeless;
        this.mappingDirectory = mappingTypes;
    }

    private MappingSet getMappingsForResource(String resource) throws IOException {
        Map value = new ObjectMapper().readValue(getClass().getResourceAsStream(mappingDirectory + "/" + resource), Map.class);
        return FieldParser.parseMappings(value, !typeless);
    }

    private InputStream getResourceStream(String resource) {
        return getClass().getResourceAsStream(mappingDirectory + "/" + resource);
    }

    private Mapping ensureAndGet(String index, String potentialTypeName, MappingSet mappingSet) {
        if (typeless) {
            Mapping mapping = mappingSet.getMapping(index, MappingSet.TYPELESS_MAPPING_NAME);
            assertNotNull(mapping);
            assertEquals(index, mapping.getIndex());
            assertEquals(MappingSet.TYPELESS_MAPPING_NAME, mapping.getType());
            return mapping;
        } else {
            Mapping mapping = mappingSet.getMapping(index, potentialTypeName);
            assertNotNull(mapping);
            assertEquals(index, mapping.getIndex());
            assertEquals(potentialTypeName, mapping.getType());
            return mapping;
        }
    }

    @Test
    public void testNestedObjectParsing() throws Exception {
        MappingSet mappings = getMappingsForResource("multi_level_field_with_same_name.json");
        Mapping mapping = ensureAndGet("index", "artiststimestamp", mappings);
        Field[] properties = mapping.getFields();
        Field first = properties[0];
        assertEquals("date", first.name());
        assertEquals(DATE, first.type());
        Field second = properties[1];
        assertEquals(OBJECT, second.type());
        assertEquals("links", second.name());
        Field[] secondProps = second.properties();
        assertEquals("url", secondProps[0].name());
        assertEquals(STRING, secondProps[0].type());
    }

    @Test
    public void testBasicParsing() throws Exception {
        getMappingsForResource("basic.json");
    }

    @Test
    public void testPrimitivesParsing() throws Exception {
        MappingSet mappings = getMappingsForResource("primitives.json");
        Mapping mapping = ensureAndGet("index", "primitives", mappings);
        Field[] props = mapping.getFields();
        assertEquals(16, props.length);
        assertEquals("field01", props[0].name());
        assertEquals(BOOLEAN, props[0].type());
        assertEquals("field02", props[1].name());
        assertEquals(BYTE, props[1].type());
        assertEquals("field03", props[2].name());
        assertEquals(SHORT, props[2].type());
        assertEquals("field04", props[3].name());
        assertEquals(INTEGER, props[3].type());
        assertEquals("field05", props[4].name());
        assertEquals(LONG, props[4].type());
        assertEquals("field06", props[5].name());
        assertEquals(FLOAT, props[5].type());
        assertEquals("field07", props[6].name());
        assertEquals(DOUBLE, props[6].type());
        assertEquals("field08", props[7].name());
        assertEquals(STRING, props[7].type());
        assertEquals("field09", props[8].name());
        assertEquals(DATE, props[8].type());
        assertEquals("field10", props[9].name());
        assertEquals(BINARY, props[9].type());
        assertEquals("field11", props[10].name());
        assertEquals(TEXT, props[10].type());
        assertEquals("field12", props[11].name());
        assertEquals(KEYWORD, props[11].type());
        assertEquals("field13", props[12].name());
        assertEquals(HALF_FLOAT, props[12].type());
        assertEquals("field14", props[13].name());
        assertEquals(SCALED_FLOAT, props[13].type());
        assertEquals("field15", props[14].name());
        assertEquals(DATE_NANOS, props[14].type());
        assertEquals("field16", props[15].name());
        assertEquals(WILDCARD, props[15].type());
    }

    @Test
    public void testGeoParsingWithOptions() throws Exception {
        MappingSet mappings = getMappingsForResource("geo.json");
        Mapping mapping = ensureAndGet("index", "restaurant", mappings);
        out.println(mapping);
        Field[] props = mapping.getFields();
        assertEquals(1, props.length);
        assertEquals("location", props[0].name());
        assertEquals(GEO_POINT, props[0].type());
    }

    @Test
    public void testCompletionParsing() throws Exception {
        MappingSet mappings = getMappingsForResource("completion.json");
        Mapping mapping = ensureAndGet("index", "song", mappings);
        Field[] props = mapping.getFields();
        assertEquals(1, props.length);
        assertEquals("name", props[0].name());
    }

    @Test
    public void testIpParsing() throws Exception {
        MappingSet mappings = getMappingsForResource("ip.json");
        Mapping mapping = ensureAndGet("index", "client", mappings);
        assertEquals(1, mapping.getFields().length);
    }

    @Test
    public void testUnsupportedParsing() throws Exception {
        MappingSet mappings = getMappingsForResource("attachment.json");
        Mapping mapping = ensureAndGet("index", "person", mappings);
        assertEquals(0, mapping.getFields().length);
    }

    @Test
    public void testFieldValidation() throws Exception {
        MappingSet mappings = getMappingsForResource("multi_level_field_with_same_name.json");
        Mapping mapping = ensureAndGet("index", "artiststimestamp", mappings);

        List<String>[] findFixes = findTypos(singletonList("nam"), mapping);
        assertThat(findFixes[1], contains("name"));

        findFixes = findTypos(singletonList("link.url"), mapping);
        assertThat(findFixes[1], contains("links.url"));

        findFixes = findTypos(singletonList("ulr"), mapping);
        assertThat(findFixes[1], contains("links.url"));

        findFixes = findTypos(singletonList("likn"), mapping);
        assertThat(findFixes[1], contains("links"));

        findFixes = findTypos(singletonList("_uid"), mapping);
        assertThat(findFixes, is(nullValue()));
    }

    @Test
    public void testFieldInclude() throws Exception {
        MappingSet mappings = getMappingsForResource("multi_level_field_with_same_name.json");
        Mapping mapping = ensureAndGet("index", "artiststimestamp", mappings);

        Mapping filtered = mapping.filter(singleton("*a*e"), Collections.<String>emptyList());

        assertThat(mapping.getIndex(), is(filtered.getIndex()));
        assertThat(mapping.getType(), is(filtered.getType()));

        Field[] props = filtered.getFields();

        assertThat(props.length, is(2));
        assertThat(props[0].name(), is("date"));
        assertThat(props[1].name(), is("name"));
    }

    @Test
    public void testFieldNestedInclude() throws Exception {
        MappingSet mappings = getMappingsForResource("multi_level_field_with_same_name.json");
        Mapping mapping = ensureAndGet("index", "artiststimestamp", mappings);

        Mapping filtered = mapping.filter(singleton("links.image.url"), Collections.<String>emptyList());

        assertThat(mapping.getIndex(), is(filtered.getIndex()));
        assertThat(mapping.getType(), is(filtered.getType()));

        Field[] props = filtered.getFields();

        assertThat(props.length, is(1));
        assertThat(props[0].name(), is("links"));
        assertThat(props[0].properties().length, is(1));
        assertThat(props[0].properties()[0].name(), is("image"));
        assertThat(props[0].properties()[0].properties().length, is(1));
        assertThat(props[0].properties()[0].properties()[0].name(), is("url"));
    }

    @Test
    public void testFieldExclude() throws Exception {
        MappingSet mappings = getMappingsForResource("nested_arrays_mapping.json");
        Mapping mapping = ensureAndGet("index", "nested-array-exclude", mappings);

        Mapping filtered = mapping.filter(Collections.<String>emptyList(), singleton("nested.bar"));

        assertThat(mapping.getIndex(), is(filtered.getIndex()));
        assertThat(mapping.getType(), is(filtered.getType()));

        Field[] props = filtered.getFields();

        assertThat(props.length, is(2));
        assertThat(props[0].name(), is("foo"));
        assertThat(props[1].name(), is("nested"));
        assertThat(props[1].properties().length, is(1));
        assertThat(props[1].properties()[0].name(), is("what"));
    }

    @Test
    public void testNestedMapping() throws Exception {
        MappingSet mappings = getMappingsForResource("nested-mapping.json");
        Mapping mapping = ensureAndGet("index", "company", mappings);

        Field[] properties = mapping.getFields();
        assertEquals(3, properties.length);
        Field first = properties[0];
        assertEquals("name", first.name());
        assertEquals(STRING, first.type());
        Field second = properties[1];
        assertEquals("description", second.name());
        assertEquals(STRING, second.type());
        Field nested = properties[2];
        assertEquals("employees", nested.name());
        assertEquals(NESTED, nested.type());
        Field[] nestedProps = nested.properties();
        assertEquals("name", nestedProps[0].name());
        assertEquals(LONG, nestedProps[1].type());
    }

    @Test
    public void testMappingWithFieldsNamedPropertiesAndType() throws Exception {
        MappingSet mappings = getMappingsForResource("mapping_with_fields_named_properties_and_type.json");
        Mapping mapping = ensureAndGet("index", "properties", mappings);
        assertEquals("field1", mapping.getFields()[0].name());
        assertEquals(STRING, mapping.getFields()[0].type());
        assertEquals("properties", mapping.getFields()[1].name());
        assertEquals(OBJECT, mapping.getFields()[1].type());
        assertEquals("subfield1", mapping.getFields()[1].properties()[0].name());
        assertEquals(STRING, mapping.getFields()[1].properties()[0].type());
        assertEquals("subfield2", mapping.getFields()[1].properties()[1].name());
        assertEquals(STRING, mapping.getFields()[1].properties()[1].type());
        assertEquals("field2", mapping.getFields()[2].name());
        assertEquals(OBJECT, mapping.getFields()[2].type());
        assertEquals("subfield3", mapping.getFields()[2].properties()[0].name());
        assertEquals(STRING, mapping.getFields()[2].properties()[0].type());
        assertEquals("properties", mapping.getFields()[2].properties()[1].name());
        assertEquals(STRING, mapping.getFields()[2].properties()[1].type());
        assertEquals("type", mapping.getFields()[2].properties()[2].name());
        assertEquals(OBJECT, mapping.getFields()[2].properties()[2].type());
        assertEquals("properties", mapping.getFields()[2].properties()[2].properties()[0].name());
        assertEquals(STRING, mapping.getFields()[2].properties()[2].properties()[1].type());
        assertEquals("subfield5", mapping.getFields()[2].properties()[2].properties()[1].name());
        assertEquals(OBJECT, mapping.getFields()[2].properties()[2].properties()[0].type());
        assertEquals("properties", mapping.getFields()[2].properties()[2].properties()[0].properties()[0].name());
        assertEquals(STRING, mapping.getFields()[2].properties()[2].properties()[0].properties()[0].type());
        assertEquals("subfield4", mapping.getFields()[2].properties()[2].properties()[0].properties()[1].name());
        assertEquals(STRING, mapping.getFields()[2].properties()[2].properties()[0].properties()[1].type());
    }

    @Test
    public void testJoinField() throws Exception {
        MappingSet mappings = getMappingsForResource("join-type.json");

        Mapping mapping = ensureAndGet("index", "join", mappings);
        assertEquals("id", mapping.getFields()[0].name());
        assertEquals(KEYWORD, mapping.getFields()[0].type());
        assertEquals("company", mapping.getFields()[1].name());
        assertEquals(TEXT, mapping.getFields()[1].type());
        assertEquals("name", mapping.getFields()[2].name());
        assertEquals(TEXT, mapping.getFields()[2].type());
        assertEquals("joiner", mapping.getFields()[3].name());
        assertEquals(JOIN, mapping.getFields()[3].type());
        assertEquals("name", mapping.getFields()[3].properties()[0].name());
        assertEquals(KEYWORD, mapping.getFields()[3].properties()[0].type());
        assertEquals("parent", mapping.getFields()[3].properties()[1].name());
        assertEquals(KEYWORD, mapping.getFields()[3].properties()[1].type());
    }


    @Test
    public void testMultipleFields() throws Exception {
        assumeThat("Cannot read multiple types when using typeless mappings", typeless, is(false));
        MappingSet mappings = getMappingsForResource("multiple-types.json");

        assertNotNull(ensureAndGet("index", "type1", mappings));
        assertNotNull(ensureAndGet("index", "type2", mappings));

        Mapping mapping = mappings.getResolvedView();
        assertEquals("*", mapping.getIndex());
        assertEquals("*", mapping.getType());
        assertEquals("field1", mapping.getFields()[0].name());
        assertEquals(KEYWORD, mapping.getFields()[0].type());
        assertEquals("field2", mapping.getFields()[1].name());
        assertEquals(FLOAT, mapping.getFields()[1].type());
        assertEquals("field3", mapping.getFields()[2].name());
        assertEquals(INTEGER, mapping.getFields()[2].type());
    }

    @Test
    public void testMultipleIndexMultipleFields() throws Exception {
        MappingSet mappings = getMappingsForResource("multiple-indices-multiple-types.json");

        assertNotNull(ensureAndGet("index1", "type1", mappings));
        assertNotNull(ensureAndGet("index2", "type2", mappings));

        Mapping mapping = mappings.getResolvedView();
        assertEquals("*", mapping.getIndex());
        assertEquals("*", mapping.getType());
        assertEquals("field1", mapping.getFields()[0].name());
        assertEquals(KEYWORD, mapping.getFields()[0].type());
        assertEquals("field2", mapping.getFields()[1].name());
        assertEquals(FLOAT, mapping.getFields()[1].type());
        assertEquals("field3", mapping.getFields()[2].name());
        assertEquals(INTEGER, mapping.getFields()[2].type());
    }

    @Test
    public void testDynamicTemplateIndex() throws Exception {
        MappingSet mappings = getMappingsForResource("dynamic-template.json");

        Mapping mapping = ensureAndGet("index", "friend", mappings);
        assertEquals("hobbies", mapping.getFields()[0].name());
        assertEquals(TEXT, mapping.getFields()[0].type());
        assertEquals("job", mapping.getFields()[1].name());
        assertEquals(TEXT, mapping.getFields()[1].type());
        assertEquals("name", mapping.getFields()[2].name());
        assertEquals(TEXT, mapping.getFields()[2].type());
    }

    @Test
    public void testMultipleIndexMultipleUpcastableFields() throws Exception {
        MappingSet mappings = getMappingsForResource("multiple-indices-multiple-upcastable-types.json");

        assertNotNull(ensureAndGet("index1", "type1", mappings));
        assertNotNull(ensureAndGet("index2", "type2", mappings));

        Mapping mapping = mappings.getResolvedView();
        assertEquals("*", mapping.getIndex());
        assertEquals("*", mapping.getType());

        assertEquals("field1_keyword", mapping.getFields()[0].name());
        assertEquals("field2_keyword", mapping.getFields()[1].name());
        assertEquals("field3_keyword", mapping.getFields()[2].name());
        assertEquals("field4_integer", mapping.getFields()[3].name());
        assertEquals("field5_keyword", mapping.getFields()[4].name());
        assertEquals("field6_float", mapping.getFields()[5].name());
        assertEquals("field7_keyword", mapping.getFields()[6].name());
        assertEquals("field8_float", mapping.getFields()[7].name());
        assertEquals("field9_integer", mapping.getFields()[8].name());

        assertEquals(KEYWORD, mapping.getFields()[0].type());
        assertEquals(KEYWORD, mapping.getFields()[1].type());
        assertEquals(KEYWORD, mapping.getFields()[2].type());
        assertEquals(INTEGER, mapping.getFields()[3].type());
        assertEquals(KEYWORD, mapping.getFields()[4].type());
        assertEquals(FLOAT, mapping.getFields()[5].type());
        assertEquals(KEYWORD, mapping.getFields()[6].type());
        assertEquals(FLOAT, mapping.getFields()[7].type());
        assertEquals(INTEGER, mapping.getFields()[8].type());
    }

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testMultipleIndexMultipleConflictingFields() throws Exception {
        MappingSet mappings = getMappingsForResource("multiple-indices-multiple-conflicting-types.json");

        assertNotNull(ensureAndGet("index1", "type1", mappings));
        assertNotNull(ensureAndGet("index2", "type2", mappings));

        Mapping mapping = mappings.getResolvedView();
        assertEquals("*", mapping.getIndex());
        assertEquals("*", mapping.getType());
        assertEquals("field1", mapping.getFields()[0].name());
        assertEquals(KEYWORD, mapping.getFields()[0].type());
        assertEquals("field3", mapping.getFields()[1].name());
        assertEquals(FLOAT, mapping.getFields()[1].type());
        assertEquals("field4", mapping.getFields()[2].name());
        assertEquals(INTEGER, mapping.getFields()[2].type());
    }
}