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
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.hadoop.rest.dto.mapping.Field;
import org.elasticsearch.hadoop.serialization.ScrollReader;
import org.elasticsearch.hadoop.serialization.builder.JdkValueReader;
import org.junit.Test;

import static org.junit.Assert.*;

public class ScrollReaderTest {

    @Test
    public void testScrollWithFields() throws IOException {
        ScrollReader reader = new ScrollReader(new JdkValueReader(), null);
        InputStream stream = getClass().getResourceAsStream("scroll-fields.json");
        List<Object[]> read = reader.read(stream);
        assertEquals(3, read.size());
        Object[] objects = read.get(0);
        assertTrue(((Map) objects[1]).containsKey("fields"));
    }

    @Test
    public void testScrollWithNestedFields() throws IOException {
        InputStream stream = getClass().getResourceAsStream("scroll-source-mapping.json");
        Field fl = Field.parseField(new ObjectMapper().readValue(stream, Map.class));
        ScrollReader reader = new ScrollReader(new JdkValueReader(), fl);
        stream = getClass().getResourceAsStream("scroll-source.json");

        List<Object[]> read = reader.read(stream);

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
        ScrollReader reader = new ScrollReader(new JdkValueReader(), null);
        InputStream stream = getClass().getResourceAsStream("scroll-source.json");
        List<Object[]> read = reader.read(stream);
        assertEquals(3, read.size());
        Object[] objects = read.get(0);
        assertTrue(((Map) objects[1]).containsKey("source"));
    }

    @Test
    public void testScrollWithoutSource() throws IOException {
        ScrollReader reader = new ScrollReader(new JdkValueReader(), null);
        InputStream stream = getClass().getResourceAsStream("empty-source.json");
        List<Object[]> read = reader.read(stream);
        assertEquals(2, read.size());
        Object[] objects = read.get(0);
        assertTrue(((Map) objects[1]).isEmpty());
    }
}
