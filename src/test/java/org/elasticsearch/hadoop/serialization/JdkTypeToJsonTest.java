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
import java.util.Collections;

import org.elasticsearch.hadoop.serialization.json.JacksonJsonGenerator;
import org.elasticsearch.hadoop.util.FastByteArrayOutputStream;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class JdkTypeToJsonTest {

    private Generator toJson;
    private static FastByteArrayOutputStream out;

    @BeforeClass
    public static void beforeClass() {
        out = new FastByteArrayOutputStream();
    }

    @Before
    public void start() {
        toJson = new JacksonJsonGenerator(out);
        out.reset();
    }

    @After
    public void after() {
        toJson = null;
        out.reset();
    }

    @AfterClass
    public static void afterClass() {
        out = null;
    }

    @Test
    public void testNull() {
        jdkTypeToJson(null);
    }

    @Test
    public void testString() {
        jdkTypeToJson("some string");
    }

    @Test
    public void testLong() {
        jdkTypeToJson(Long.MAX_VALUE);
    }

    @Test
    public void testInteger() {
        jdkTypeToJson(Integer.MAX_VALUE);
    }

    @Test
    public void testDouble() {
        jdkTypeToJson(Double.MAX_VALUE);
    }

    @Test
    public void testFloat() {
        jdkTypeToJson(Float.MAX_VALUE);
    }

    @Test
    public void testBoolean() {
        jdkTypeToJson(Boolean.TRUE);
    }

    @Test
    public void testByte() {
        jdkTypeToJson(Byte.MAX_VALUE);
    }

    @Test
    public void testByteArray() {
        jdkTypeToJson("byte array".getBytes());
    }

    @Test
    public void testList() {
        jdkTypeToJson(Arrays.asList(new String[] { "one", "two" }));
    }

    @Test
    public void testMap() {
        jdkTypeToJson(Collections.singletonMap("key", "value"));
    }

    private void jdkTypeToJson(Object obj) {
        ContentBuilder.generate(toJson, new JdkValueWriter(false)).value(obj).flush().close();
        System.out.println(out.bytes());
    }
}