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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.hadoop.serialization.FieldType;
import org.junit.Test;

import static org.elasticsearch.hadoop.serialization.dto.mapping.FieldParser.parseMapping;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;


public class MappingTest {

    @Test
    public void testNestedObjectParsing() throws Exception {
        Map value = new ObjectMapper().readValue(getClass().getResourceAsStream("multi_level_field_with_same_name.json"), Map.class);
        MappingSet mappings = parseMapping(value);
        Mapping mapping = mappings.getMapping("index", "artiststimestamp");
        assertEquals("artiststimestamp", mapping.getName());
        Field[] properties = mapping.getFields();
        Field first = properties[0];
        assertEquals("date", first.name());
        assertEquals(FieldType.DATE, first.type());
        Field second = properties[1];
        assertEquals(FieldType.OBJECT, second.type());
        assertEquals("links", second.name());
        Field[] secondProps = second.properties();
        assertEquals("url", secondProps[0].name());
        assertEquals(FieldType.STRING, secondProps[0].type());
    }

    @Test
    public void testBasicParsing() throws Exception {
        Map value = new ObjectMapper().readValue(getClass().getResourceAsStream("basic.json"), Map.class);
        MappingSet fl = parseMapping(value);
    }

    @Test
    public void testPrimitivesParsing() throws Exception {
        Map value = new ObjectMapper().readValue(getClass().getResourceAsStream("primitives.json"), Map.class);
        MappingSet mappings = parseMapping(value);
        Mapping mapping = mappings.getMapping("index", "primitives");
        assertEquals("primitives", mapping.getName());
        Field[] props = mapping.getFields();
        assertEquals(14, props.length);
        assertEquals("field01", props[0].name());
        assertEquals(FieldType.BOOLEAN, props[0].type());
        assertEquals("field02", props[1].name());
        assertEquals(FieldType.BYTE, props[1].type());
        assertEquals("field03", props[2].name());
        assertEquals(FieldType.SHORT, props[2].type());
        assertEquals("field04", props[3].name());
        assertEquals(FieldType.INTEGER, props[3].type());
        assertEquals("field05", props[4].name());
        assertEquals(FieldType.LONG, props[4].type());
        assertEquals("field06", props[5].name());
        assertEquals(FieldType.FLOAT, props[5].type());
        assertEquals("field07", props[6].name());
        assertEquals(FieldType.DOUBLE, props[6].type());
        assertEquals("field08", props[7].name());
        assertEquals(FieldType.STRING, props[7].type());
        assertEquals("field09", props[8].name());
        assertEquals(FieldType.DATE, props[8].type());
        assertEquals("field10", props[9].name());
        assertEquals(FieldType.BINARY, props[9].type());
        assertEquals("field11", props[10].name());
        assertEquals(FieldType.TEXT, props[10].type());
        assertEquals("field12", props[11].name());
        assertEquals(FieldType.KEYWORD, props[11].type());
        assertEquals("field13", props[12].name());
        assertEquals(FieldType.HALF_FLOAT, props[12].type());
        assertEquals("field14", props[13].name());
        assertEquals(FieldType.SCALED_FLOAT, props[13].type());
    }

    @Test
    public void testGeoParsingWithOptions() throws Exception {
        Map value = new ObjectMapper().readValue(getClass().getResourceAsStream("geo.json"), Map.class);
        MappingSet mappings = parseMapping(value);
        Mapping mapping = mappings.getMapping("index", "restaurant");
        System.out.println(mapping);
        assertEquals("restaurant", mapping.getName());
        Field[] props = mapping.getFields();
        assertEquals(1, props.length);
        assertEquals("location", props[0].name());
        assertEquals(FieldType.GEO_POINT, props[0].type());
    }

    @Test
    public void testCompletionParsing() throws Exception {
        Map value = new ObjectMapper().readValue(getClass().getResourceAsStream("completion.json"), Map.class);
        MappingSet mappings = parseMapping(value);
        Mapping mapping = mappings.getMapping("index", "song");
        assertEquals("song", mapping.getName());
        Field[] props = mapping.getFields();
        assertEquals(1, props.length);
        assertEquals("name", props[0].name());
    }

    @Test
    public void testIpParsing() throws Exception {
        Map value = new ObjectMapper().readValue(getClass().getResourceAsStream("ip.json"), Map.class);
        MappingSet mappings = parseMapping(value);
        Mapping mapping = mappings.getMapping("index", "client");
        assertEquals(1, mapping.getFields().length);
    }

    @Test
    public void testUnsupportedParsing() throws Exception {
        Map value = new ObjectMapper().readValue(getClass().getResourceAsStream("attachment.json"), Map.class);
        MappingSet mappings = parseMapping(value);
        Mapping mapping = mappings.getMapping("index", "person");
        assertEquals("person", mapping.getName());
        assertEquals(0, mapping.getFields().length);
    }

    @Test
    public void testFieldValidation() throws Exception {
        Map value = new ObjectMapper().readValue(getClass().getResourceAsStream("multi_level_field_with_same_name.json"), Map.class);
        MappingSet mappings = parseMapping(value);
        Mapping mapping = mappings.getMapping("index", "artiststimestamp");

        List<String>[] findFixes = MappingUtils.findTypos(Collections.singletonList("nam"), mapping);
        assertThat(findFixes[1], contains("name"));

        findFixes = MappingUtils.findTypos(Collections.singletonList("link.url"), mapping);
        assertThat(findFixes[1], contains("links.url"));

        findFixes = MappingUtils.findTypos(Collections.singletonList("ulr"), mapping);
        assertThat(findFixes[1], contains("links.url"));

        findFixes = MappingUtils.findTypos(Collections.singletonList("likn"), mapping);
        assertThat(findFixes[1], contains("links"));

        findFixes = MappingUtils.findTypos(Collections.singletonList("_uid"), mapping);
        assertThat(findFixes, is(nullValue()));
    }

    @Test
    public void testFieldInclude() throws Exception {
        Map value = new ObjectMapper().readValue(getClass().getResourceAsStream("multi_level_field_with_same_name.json"), Map.class);
        MappingSet mappings = parseMapping(value);
        Mapping mapping = mappings.getMapping("index", "artiststimestamp");

        Mapping filtered = mapping.filter(Collections.singleton("*a*e"), Collections.<String> emptyList());

        assertThat(mapping.getName(), is(filtered.getName()));

        Field[] props = filtered.getFields();

        assertThat(props.length, is(2));
        assertThat(props[0].name(), is("date"));
        assertThat(props[1].name(), is("name"));
    }

    @Test
    public void testFieldExclude() throws Exception {
        Map value = new ObjectMapper().readValue(getClass().getResourceAsStream("nested_arrays_mapping.json"), Map.class);
        MappingSet mappings = parseMapping(value);
        Mapping mapping = mappings.getMapping("index", "nested-array-exclude");

        Mapping filtered = mapping.filter(Collections.<String> emptyList(), Collections.singleton("nested.bar"));

        assertThat(mapping.getName(), is(filtered.getName()));

        Field[] props = filtered.getFields();

        assertThat(props.length, is(2));
        assertThat(props[0].name(), is("foo"));
        assertThat(props[1].name(), is("nested"));
        assertThat(props[1].properties().length, is(1));
        assertThat(props[1].properties()[0].name(), is("what"));
    }

    @Test
    public void testNestedMapping() throws Exception {
        Map value = new ObjectMapper().readValue(getClass().getResourceAsStream("nested-mapping.json"), Map.class);
        MappingSet mappings = parseMapping(value);
        Mapping mapping = mappings.getMapping("index", "company");

        assertEquals("company", mapping.getName());
        Field[] properties = mapping.getFields();
        assertEquals(3, properties.length);
        Field first = properties[0];
        assertEquals("name", first.name());
        assertEquals(FieldType.STRING, first.type());
        Field second = properties[1];
        assertEquals("description", second.name());
        assertEquals(FieldType.STRING, second.type());
        Field nested = properties[2];
        assertEquals("employees", nested.name());
        assertEquals(FieldType.NESTED, nested.type());
        Field[] nestedProps = nested.properties();
        assertEquals("name", nestedProps[0].name());
        assertEquals(FieldType.LONG, nestedProps[1].type());
    }

    @Test
    public void testMappingWithFieldsNamedPropertiesAndType() throws Exception {
        Map value = new ObjectMapper().readValue(getClass().getResourceAsStream("mapping_with_fields_named_properties_and_type.json"), Map.class);
        MappingSet mappings = parseMapping(value);
        Mapping mapping = mappings.getMapping("index", "type");
        assertEquals("type", mapping.getName());
        assertEquals("field1", mapping.getFields()[0].name());
        assertEquals(FieldType.STRING, mapping.getFields()[0].type());
        assertEquals("properties", mapping.getFields()[1].name());
        assertEquals(FieldType.OBJECT, mapping.getFields()[1].type());
        assertEquals("subfield1", mapping.getFields()[1].properties()[0].name());
        assertEquals(FieldType.STRING, mapping.getFields()[1].properties()[0].type());
        assertEquals("subfield2", mapping.getFields()[1].properties()[1].name());
        assertEquals(FieldType.STRING, mapping.getFields()[1].properties()[1].type());
        assertEquals("field2", mapping.getFields()[2].name());
        assertEquals(FieldType.OBJECT, mapping.getFields()[2].type());
        assertEquals("subfield3", mapping.getFields()[2].properties()[0].name());
        assertEquals(FieldType.STRING, mapping.getFields()[2].properties()[0].type());
        assertEquals("properties", mapping.getFields()[2].properties()[1].name());
        assertEquals(FieldType.STRING, mapping.getFields()[2].properties()[1].type());
        assertEquals("type", mapping.getFields()[2].properties()[2].name());
        assertEquals(FieldType.OBJECT, mapping.getFields()[2].properties()[2].type());
        assertEquals("properties", mapping.getFields()[2].properties()[2].properties()[0].name());
        assertEquals(FieldType.STRING, mapping.getFields()[2].properties()[2].properties()[1].type());
        assertEquals("subfield5", mapping.getFields()[2].properties()[2].properties()[1].name());
        assertEquals(FieldType.OBJECT, mapping.getFields()[2].properties()[2].properties()[0].type());
        assertEquals("properties", mapping.getFields()[2].properties()[2].properties()[0].properties()[0].name());
        assertEquals(FieldType.STRING, mapping.getFields()[2].properties()[2].properties()[0].properties()[0].type());
        assertEquals("subfield4", mapping.getFields()[2].properties()[2].properties()[0].properties()[1].name());
        assertEquals(FieldType.STRING, mapping.getFields()[2].properties()[2].properties()[0].properties()[1].type());
    }

    @Test
    public void testJoinField() throws Exception {
        Map value = new ObjectMapper().readValue(getClass().getResourceAsStream("join-type.json"), Map.class);
        MappingSet mappings = parseMapping(value);

        Mapping mapping = mappings.getMapping("index", "join");
        assertEquals("join", mapping.getName());
        assertEquals("id", mapping.getFields()[0].name());
        assertEquals(FieldType.KEYWORD, mapping.getFields()[0].type());
        assertEquals("company", mapping.getFields()[1].name());
        assertEquals(FieldType.TEXT, mapping.getFields()[1].type());
        assertEquals("name", mapping.getFields()[2].name());
        assertEquals(FieldType.TEXT, mapping.getFields()[2].type());
        assertEquals("joiner", mapping.getFields()[3].name());
        assertEquals(FieldType.JOIN, mapping.getFields()[3].type());
        assertEquals("name", mapping.getFields()[3].properties()[0].name());
        assertEquals(FieldType.KEYWORD, mapping.getFields()[3].properties()[0].type());
        assertEquals("parent", mapping.getFields()[3].properties()[1].name());
        assertEquals(FieldType.KEYWORD, mapping.getFields()[3].properties()[1].type());
    }


    @Test
    public void testMultipleFields() throws Exception {
        Map value = new ObjectMapper().readValue(getClass().getResourceAsStream("multiple-types.json"), Map.class);
        MappingSet mappings = parseMapping(value);

        assertNotNull(mappings.getMapping("index", "type1"));
        assertNotNull(mappings.getMapping("index", "type2"));

        Mapping mapping = mappings.getResolvedView();
        assertEquals("*", mapping.getName());
        assertEquals("field1", mapping.getFields()[0].name());
        assertEquals(FieldType.KEYWORD, mapping.getFields()[0].type());
        assertEquals("field2", mapping.getFields()[1].name());
        assertEquals(FieldType.FLOAT, mapping.getFields()[1].type());
        assertEquals("field3", mapping.getFields()[2].name());
        assertEquals(FieldType.INTEGER, mapping.getFields()[2].type());
    }

    @Test
    public void testMultipleIndexMultipleFields() throws Exception {
        Map value = new ObjectMapper().readValue(getClass().getResourceAsStream("multiple-indices-multiple-types.json"), Map.class);
        MappingSet mappings = parseMapping(value);

        assertNotNull(mappings.getMapping("index1", "type1"));
        assertNotNull(mappings.getMapping("index2", "type2"));

        Mapping mapping = mappings.getResolvedView();
        assertEquals("*", mapping.getName());
        assertEquals("field1", mapping.getFields()[0].name());
        assertEquals(FieldType.KEYWORD, mapping.getFields()[0].type());
        assertEquals("field2", mapping.getFields()[1].name());
        assertEquals(FieldType.FLOAT, mapping.getFields()[1].type());
        assertEquals("field3", mapping.getFields()[2].name());
        assertEquals(FieldType.INTEGER, mapping.getFields()[2].type());
    }

    @Test
    public void testDynamicTemplateIndex() throws Exception {
        Map value = new ObjectMapper().readValue(getClass().getResourceAsStream("dynamic-template.json"), Map.class);
        MappingSet mappings = parseMapping(value);

        Mapping mapping = mappings.getMapping("index", "friend");
        assertEquals("friend", mapping.getName());
        assertEquals("hobbies", mapping.getFields()[0].name());
        assertEquals(FieldType.TEXT, mapping.getFields()[0].type());
        assertEquals("job", mapping.getFields()[1].name());
        assertEquals(FieldType.TEXT, mapping.getFields()[1].type());
        assertEquals("name", mapping.getFields()[2].name());
        assertEquals(FieldType.TEXT, mapping.getFields()[2].type());
    }
}