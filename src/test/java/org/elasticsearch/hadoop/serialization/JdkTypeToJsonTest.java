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

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;

import org.elasticsearch.hadoop.serialization.builder.ContentBuilder;
import org.elasticsearch.hadoop.serialization.builder.JdkValueWriter;
import org.elasticsearch.hadoop.util.FastByteArrayOutputStream;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class JdkTypeToJsonTest {

    private Generator toJson;
    private static FastByteArrayOutputStream out;

    private static class Ratio extends Number {

        private final long antecedent;
        private final long consequent;

        public Ratio(long antecedent, long consequent) {
            this.antecedent = antecedent;
            this.consequent = consequent;
        }

        @Override
        public int intValue() {
            return (int) doubleValue();
        }

        @Override
        public long longValue() {
            return (long) doubleValue();
        }

        @Override
        public float floatValue() {
            return (float) doubleValue();
        }

        @Override
        public double doubleValue() {
            return new BigDecimal(antecedent).divide(new BigDecimal(consequent)).doubleValue();
        }

        @Override
        public String toString() {
            return antecedent + "/" + consequent;
        }
    }

    @BeforeClass
    public static void beforeClass() {
        out = new FastByteArrayOutputStream();
    }

    @Before
    public void start() {
        out.reset();
    }

    @After
    public void after() {
        out.reset();
    }

    @AfterClass
    public static void afterClass() {
        out = null;
    }

    @Test
    public void testEmptyString() {
        assertEquals("\"  \"", jdkTypeToJson("  "));
    }

    @Test
    public void testNull() {
        assertEquals("null", jdkTypeToJson(null));
    }

    @Test
    public void testString() {
        assertEquals("\"some string\"", jdkTypeToJson("some string"));
    }

    @Test
    public void testLong() {
        assertEquals("9223372036854775807", jdkTypeToJson(Long.MAX_VALUE));
    }

    @Test
    public void testInteger() {
        assertEquals("2147483647", jdkTypeToJson(Integer.MAX_VALUE));
    }

    @Test
    public void testDouble() {
        assertEquals("1.7976931348623157E308", jdkTypeToJson(Double.MAX_VALUE));
    }

    @Test
    public void testFloat() {
        assertEquals("3.4028235E38", jdkTypeToJson(Float.MAX_VALUE));
    }

    @Test
    public void testCustomNumber() {
        assertEquals("1.5", jdkTypeToJson(new Ratio(3, 2)));
    }

    @Test
    public void testBoolean() {
        assertEquals("true", jdkTypeToJson(Boolean.TRUE));
    }

    @Test
    public void testByte() {
        assertEquals("127", jdkTypeToJson(Byte.MAX_VALUE));
    }

    @Test
    public void testByteArray() {
        assertEquals("\"Ynl0ZSBhcnJheQ==\"", jdkTypeToJson("byte array".getBytes()));
    }

    @Test
    public void testList() {
        assertEquals("[\"one\",\"two\"]", jdkTypeToJson(Arrays.asList(new String[] { "one", "two" })));
    }

    @Test
    public void testMap() {
        assertEquals("{\"key\":\"value\"}", jdkTypeToJson(Collections.singletonMap("key", "value")));
    }

    private String jdkTypeToJson(Object obj) {
        ContentBuilder.generate(out, new JdkValueWriter(false)).value(obj).flush().close();
        return out.bytes().toString();
    }
}