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
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.serialization.builder.ContentBuilder;
import org.elasticsearch.hadoop.serialization.builder.JdkValueWriter;
import org.elasticsearch.hadoop.util.FastByteArrayOutputStream;
import org.elasticsearch.hadoop.util.TestSettings;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import static org.hamcrest.CoreMatchers.containsString;

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
    public void testArray() {
        assertEquals("[\"one\",\"two\"]", jdkTypeToJson(new String[] { "one", "two" }));
    }

    @Test
    public void testList() {
        assertEquals("[\"one\",\"two\"]", jdkTypeToJson(Arrays.asList(new String[] { "one", "two" })));
    }

    @Test
    public void testMap() {
        assertEquals("{\"key\":\"value\"}", jdkTypeToJson(Collections.singletonMap("key", "value")));
    }

    @Test
    public void testMapWithFilterInclude() {
        TestSettings cfg = new TestSettings();
        cfg.setProperty("es.mapping.include", "key, nested.a*");

        Map nested = new LinkedHashMap();
        nested.put("aaa", "bbb");
        nested.put("ccc", "ddd");
        nested.put("axx", "zzz");

        Map map = new LinkedHashMap();
        map.put("key", "value");
        map.put("nested", nested);
        assertEquals("{\"key\":\"value\",\"nested\":{\"aaa\":\"bbb\",\"axx\":\"zzz\"}}", jdkTypeToJson(map, cfg));
    }

    @Test
    public void testMapWithFilterExclude() {
        TestSettings cfg = new TestSettings();
        cfg.setProperty("es.mapping.exclude", "key, nested.xxx");

        Map nested = new LinkedHashMap();
        nested.put("aaa", "bbb");
        nested.put("ccc", "ddd");
        nested.put("xxx", "zzz");

        Map map = new LinkedHashMap();
        map.put("key", "value");
        map.put("nested", nested);

        assertEquals("{\"nested\":{\"aaa\":\"bbb\",\"ccc\":\"ddd\"}}", jdkTypeToJson(map, cfg));
    }

    @Test
    public void testDate() {
        Date d = new Date(0);
        assertThat(jdkTypeToJson(d), containsString(new SimpleDateFormat("yyyy-MM-dd").format(d)));
    }

    @Test
    public void testCalendar() {
        Date d = new Date(0);
        Calendar cal = Calendar.getInstance();
        cal.setTime(d);
        assertThat(jdkTypeToJson(cal), containsString(new SimpleDateFormat("yyyy-MM-dd").format(d)));
    }

    @Test
    public void testTimestamp() {
        Date d = new Date(0);
        Timestamp ts = new Timestamp(0);
        assertThat(jdkTypeToJson(ts), containsString(new SimpleDateFormat("yyyy-MM-dd").format(d)));
    }

    @Test(expected = EsHadoopSerializationException.class)
    public void testUnknown() {
        jdkTypeToJson(new Object());
    }

    public void testHandleUnknown() {
        final String message = "True Belief";

        Object obj = new Object() {
            @Override
            public String toString() {
                return message;
            }
        };

        ContentBuilder.generate(out, new JdkValueWriter(true)).value(obj).flush().close();
        assertEquals(message, out.bytes().toString());
    }

    private String jdkTypeToJson(Object obj) {
        ContentBuilder.generate(out, new JdkValueWriter(false)).value(obj).flush().close();
        return out.bytes().toString();
    }

    private String jdkTypeToJson(Object obj, Settings settings) {
        JdkValueWriter jdkWriter = new JdkValueWriter(false);
        jdkWriter.setSettings(settings);
        ContentBuilder.generate(out, jdkWriter).value(obj).flush().close();
        return out.bytes().toString();
    }
}