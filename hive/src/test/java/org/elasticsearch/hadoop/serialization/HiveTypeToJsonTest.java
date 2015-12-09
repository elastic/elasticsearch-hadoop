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
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.hive.HiveType;
import org.elasticsearch.hadoop.hive.HiveValueWriter;
import org.elasticsearch.hadoop.serialization.builder.ContentBuilder;
import org.elasticsearch.hadoop.util.FastByteArrayOutputStream;
import org.elasticsearch.hadoop.util.TestSettings;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.*;

public class HiveTypeToJsonTest {

    private static FastByteArrayOutputStream out;

    public static class MyHiveType extends HiveType {

        public MyHiveType(Object object, ObjectInspector info) {
            super(object, info);
        }

        public MyHiveType(Object object, TypeInfo info) {
            super(object, TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(info));
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
    public void testNull() {
        hiveTypeToJson(new MyHiveType(null, voidTypeInfo));
    }

    @Test
    public void testString() {
        assertEquals("\"some string\"", hiveTypeToJson(new MyHiveType(new Text("some string"), stringTypeInfo)));
    }

    @Test
    public void testLong() {
        assertEquals("9223372036854775807", hiveTypeToJson(new MyHiveType(new LongWritable(Long.MAX_VALUE), longTypeInfo)));
    }

    @Test
    public void testInteger() {
        assertEquals("2147483647", hiveTypeToJson(new MyHiveType(new IntWritable(Integer.MAX_VALUE), intTypeInfo)));
    }

    @Test
    public void testDouble() {
        assertEquals("1.7976931348623157E308", hiveTypeToJson(new MyHiveType(new DoubleWritable(Double.MAX_VALUE), doubleTypeInfo)));
    }

    @Test
    public void testFloat() {
        assertEquals("3.4028235E38", hiveTypeToJson(new MyHiveType(new FloatWritable(Float.MAX_VALUE), floatTypeInfo)));
    }

    @Test
    public void testBoolean() {
        assertEquals("true", hiveTypeToJson(new MyHiveType(new BooleanWritable(Boolean.TRUE), booleanTypeInfo)));
    }

    @Test
    public void testByte() {
        // byte is not recognized by the schema
        assertEquals("127", hiveTypeToJson(new MyHiveType(new ByteWritable(Byte.MAX_VALUE), byteTypeInfo)));
    }

    @Test
    public void testShort() {
        // byte is not recognized by the schema
        assertEquals("32767", hiveTypeToJson(new MyHiveType(new ShortWritable(Short.MAX_VALUE), shortTypeInfo)));
    }

    @Test
    public void testByteArray() {
        assertEquals("\"Ynl0ZSBhcnJheQ==\"", hiveTypeToJson(new MyHiveType(new BytesWritable("byte array".getBytes()), binaryTypeInfo)));
    }

    @Test
    public void testTimestamp() {
        assertTrue(hiveTypeToJson(
                new MyHiveType(new TimestampWritable(new Timestamp(1407239910771l)), timestampTypeInfo)).startsWith(
                        "\"2014-08-0"));
    }

    @Test
    public void testDecimal() {
        assertEquals("\"1\"", hiveTypeToJson(new MyHiveType(new HiveDecimalWritable(HiveDecimal.create(BigDecimal.ONE)),
                decimalTypeInfo)));
    }

    @Test
    public void testList() {
        assertEquals("[\"one\",\"two\"]", hiveTypeToJson(new MyHiveType(
                Arrays.asList(new Object[] { new Text("one"), new Text("two") }), getListTypeInfo(stringTypeInfo))));
    }

    @Test
    public void testMap() {
        assertEquals("{\"1\":\"key\"}", hiveTypeToJson(new MyHiveType(Collections.singletonMap(new IntWritable(1), new Text("key")),
                getMapTypeInfo(intTypeInfo, stringTypeInfo))));
    }

    @Test
    public void testMapWithFilterInclude() {
        TestSettings cfg = new TestSettings();
        cfg.setProperty("es.mapping.include", "a*");

        Map map = new LinkedHashMap();
        map.put(new Text("aaa"), new Text("bbb"));
        map.put(new Text("ccc"), new Text("ddd"));
        map.put(new Text("axx"), new Text("zzz"));

        HiveType type = new MyHiveType(map, getMapTypeInfo(stringTypeInfo, stringTypeInfo));

        assertEquals("{\"aaa\":\"bbb\",\"axx\":\"zzz\"}", hiveTypeToJson(type, cfg));
    }

    @Test
    public void testMapWithFilterExclude() {
        TestSettings cfg = new TestSettings();
        cfg.setProperty("es.mapping.exclude", "xxx");

        Map map = new LinkedHashMap();
        map.put(new Text("aaa"), new Text("bbb"));
        map.put(new Text("ccc"), new Text("ddd"));
        map.put(new Text("xxx"), new Text("zzz"));

        HiveType type = new MyHiveType(map, getMapTypeInfo(stringTypeInfo, stringTypeInfo));

        assertEquals("{\"aaa\":\"bbb\",\"ccc\":\"ddd\"}", hiveTypeToJson(type, cfg));
    }


    @Test
    public void testStruct() {
        List<String> names = Arrays.asList(new String[] { "one", "two" });
        List<TypeInfo> types = Arrays.asList(new TypeInfo[] { stringTypeInfo, intTypeInfo });
        assertEquals("{\"one\":\"first\",\"two\":2}",
                hiveTypeToJson(new MyHiveType(Arrays.asList(new Object[] { new Text("first"), new IntWritable(2) }),
                        getStructTypeInfo(names, types))));
    }

    private String hiveTypeToJson(HiveType obj) {
        ContentBuilder.generate(out, new HiveValueWriter()).value(obj).flush().close();
        return out.bytes().toString();
    }

    private String hiveTypeToJson(HiveType obj, Settings cfg) {
        HiveValueWriter hiveWriter = new HiveValueWriter();
        hiveWriter.setSettings(cfg);
        ContentBuilder.generate(out, hiveWriter).value(obj).flush().close();
        return out.bytes().toString();
    }
}