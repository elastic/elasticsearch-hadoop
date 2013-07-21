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
package org.elasticsearch.hadoop.serialization;

import java.util.Arrays;

import org.elasticsearch.hadoop.mr.WritableValueReader;
import org.elasticsearch.hadoop.serialization.json.JacksonJsonParser;
import org.junit.Before;
import org.junit.Test;

public class WritableTypeFromJsonTest {

    private ValueReader vr = new WritableValueReader();

    @Before
    public void start() {
		vr = new WritableValueReader();
    }

    @Test
    public void testNull() {
        writableTypeFromJson("null");
    }

    @Test
    public void testString() {
        writableTypeFromJson("\"someText\"");
    }

    @Test
    public void testInteger() {
        writableTypeFromJson("123");
    }

    @Test
    public void testLong() {
        writableTypeFromJson("321");
    }

    @Test
    public void testDouble() {
        writableTypeFromJson("12.3e8");
    }

    @Test
    public void testFloat() {
        writableTypeFromJson("1.3");
    }

    @Test
    public void testBoolean() {
        writableTypeFromJson("true");
    }

    @Test
    public void testByteArray() {
        writableTypeFromJson(Arrays.toString("byte array".getBytes()));
    }

    @Test
    public void testArray() {
        writableTypeFromJson("[ \"one\" ,\"two\"]");
    }

    @Test
    public void testMap() {
        writableTypeFromJson("{ one:1, two:2 }");
    }

    private void writableTypeFromJson(String json) {
        Object consume = ContentConsumer.consumer(new JacksonJsonParser(json.getBytes()), vr).consume(Object.class);
        System.out.println(consume);
    }
}