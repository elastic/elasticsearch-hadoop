/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticsearch.hadoop.rest;

import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.hadoop.rest.dto.mapping.Field;
import org.junit.Test;

public class FieldTest {

    @Test
    public void testNestedObjectParsing() throws Exception {
        Map value = new ObjectMapper().readValue(getClass().getResourceAsStream("nested.json"), Map.class);
        Field fl = Field.parseField(value);
        System.out.println(fl);
    }

    @Test
    public void testBasicParsing() throws Exception {
        Map value = new ObjectMapper().readValue(getClass().getResourceAsStream("basic.json"), Map.class);
        Field fl = Field.parseField(value);
        System.out.println(fl);
    }

    @Test
    public void testMultiFieldParsing() throws Exception {
        Map value = new ObjectMapper().readValue(getClass().getResourceAsStream("multi_field.json"), Map.class);
        Field fl = Field.parseField(value);
        System.out.println(fl);
    }

    @Test
    public void testCompletionParsing() throws Exception {
        Map value = new ObjectMapper().readValue(getClass().getResourceAsStream("completion.json"), Map.class);
        Field fl = Field.parseField(value);
        System.out.println(fl);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGeolocationParsing() throws Exception {
        Map value = new ObjectMapper().readValue(getClass().getResourceAsStream("geo.json"), Map.class);
        Field fl = Field.parseField(value);
        System.out.println(fl);
    }

    @Test
    public void testUnsupportedParsing() throws Exception {
        Map value = new ObjectMapper().readValue(getClass().getResourceAsStream("attachment.json"), Map.class);
        Field fl = Field.parseField(value);
        System.out.println(fl);
    }
}
