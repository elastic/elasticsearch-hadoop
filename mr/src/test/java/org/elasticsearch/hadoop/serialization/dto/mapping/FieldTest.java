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

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.serialization.FieldType;
import org.junit.Test;


public class FieldTest {

    @Test
    public void testNestedObjectParsing() throws Exception {
        Map value = new ObjectMapper().readValue(getClass().getResourceAsStream("nested.json"), Map.class);
        Field fl = Field.parseField(value);
        assertEquals("artiststimestamp", fl.name());
        Field[] properties = fl.properties();
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
        Field fl = Field.parseField(value);
    }

    @Test
    public void testMultiFieldParsing() throws Exception {
        Map value = new ObjectMapper().readValue(getClass().getResourceAsStream("multi_field.json"), Map.class);
        Field fl = Field.parseField(value);
        assertEquals("tweet", fl.name());
        assertEquals(1, fl.properties().length);
        Field nested = fl.properties()[0];
        assertEquals("name", nested.name());
        assertEquals(FieldType.STRING, nested.type());
    }

    @Test
    public void testMultiFieldWithoutDefaultFieldParsing() throws Exception {
        Map value = new ObjectMapper().readValue(getClass().getResourceAsStream("multi_field_no_default.json"), Map.class);
        Field fl = Field.parseField(value);
        assertEquals("tweet", fl.name());
        assertEquals(1, fl.properties().length);
        Field nested = fl.properties()[0];
        assertEquals("name", nested.name());
        assertEquals(FieldType.STRING, nested.type());
    }

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testMultiFieldWithoutDefaultFieldAndMultiTypesParsing() throws Exception {
        Map value = new ObjectMapper().readValue(
                getClass().getResourceAsStream("multi_field_no_default_multi_types.json"), Map.class);
        Field fl = Field.parseField(value);
        assertEquals("tweet", fl.name());
        assertEquals(1, fl.properties().length);
        Field nested = fl.properties()[0];
        assertEquals("name", nested.name());
        assertEquals(FieldType.STRING, nested.type());
    }

    @Test
    public void testCompletionParsing() throws Exception {
        Map value = new ObjectMapper().readValue(getClass().getResourceAsStream("completion.json"), Map.class);
        Field fl = Field.parseField(value);
        assertEquals("song", fl.name());
        Field[] props = fl.properties();
        assertEquals(1, props.length);
        assertEquals("name", props[0].name());
    }

    @Test
    public void testGeolocationParsing() throws Exception {
        Map value = new ObjectMapper().readValue(getClass().getResourceAsStream("geo.json"), Map.class);
        Field fl = Field.parseField(value);
        assertEquals(1, fl.properties().length);
    }

    @Test
    public void testIpParsing() throws Exception {
        Map value = new ObjectMapper().readValue(getClass().getResourceAsStream("ip.json"), Map.class);
        Field fl = Field.parseField(value);
        assertEquals(1, fl.properties().length);
    }

    @Test
    public void testUnsupportedParsing() throws Exception {
        Map value = new ObjectMapper().readValue(getClass().getResourceAsStream("attachment.json"), Map.class);
        Field fl = Field.parseField(value);
        assertEquals("person", fl.name());
        assertEquals(0, fl.properties().length);
    }

    @Test
    public void testFieldValidation() throws Exception {
        Map value = new ObjectMapper().readValue(getClass().getResourceAsStream("nested.json"), Map.class);
        Field fl = Field.parseField(value);

        List<String>[] findFixes = MappingUtils.findTypos(Collections.singletonList("nam"), fl);
        assertThat(findFixes[1], contains("name"));

        findFixes = MappingUtils.findTypos(Collections.singletonList("link.url"), fl);
        assertThat(findFixes[1], contains("links.url"));

        findFixes = MappingUtils.findTypos(Collections.singletonList("ulr"), fl);
        assertThat(findFixes[1], contains("links.url"));

        findFixes = MappingUtils.findTypos(Collections.singletonList("likn"), fl);
        assertThat(findFixes[1], contains("links"));

        findFixes = MappingUtils.findTypos(Collections.singletonList("_uid"), fl);
        assertThat(findFixes, is(nullValue()));
    }
    
    @Test
    public void testFieldInclude() throws Exception {
        Map value = new ObjectMapper().readValue(getClass().getResourceAsStream("nested.json"), Map.class);
        Field fl = Field.parseField(value);

        Field filtered = MappingUtils.filter(fl, Collections.singleton("*a*e"), Collections.<String> emptyList());
        
        assertThat(fl.name(), is(filtered.name()));
        assertThat(fl.type(), is(filtered.type()));
        
        Field[] props = filtered.properties();

        assertThat(props.length, is(2));
        assertThat(props[0].name(), is("date"));
        assertThat(props[1].name(), is("name"));
    }

}