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

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.elasticsearch.hadoop.hive.HiveType;
import org.elasticsearch.hadoop.hive.HiveValueWriter;
import org.elasticsearch.hadoop.util.FastByteArrayOutputStream;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.*;

public class HiveTypeToJsonTest {

    private static FastByteArrayOutputStream out;

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
        hiveTypeToJson(new HiveType(null, voidTypeInfo));
    }

    @Test
    public void testString() {
        hiveTypeToJson(new HiveType(new Text("some string"), stringTypeInfo));
    }

    @Test
    public void testLong() {
        hiveTypeToJson(new HiveType(new LongWritable(Long.MAX_VALUE), longTypeInfo));
    }

    @Test
    public void testInteger() {
        hiveTypeToJson(new HiveType(new IntWritable(Integer.MAX_VALUE), intTypeInfo));
    }

    @Test
    public void testDouble() {
        hiveTypeToJson(new HiveType(new DoubleWritable(Double.MAX_VALUE), doubleTypeInfo));
    }

    @Test
    public void testFloat() {
        hiveTypeToJson(new HiveType(new FloatWritable(Float.MAX_VALUE), floatTypeInfo));
    }

    @Test
    public void testBoolean() {
        hiveTypeToJson(new HiveType(new BooleanWritable(Boolean.TRUE), booleanTypeInfo));
    }

    @Test
    public void testByte() {
        // byte is not recognized by the schema
        hiveTypeToJson(new HiveType(new ByteWritable(Byte.MAX_VALUE), byteTypeInfo));
    }

    @Test
    public void testShort() {
        // byte is not recognized by the schema
        hiveTypeToJson(new HiveType(new ShortWritable(Short.MAX_VALUE), shortTypeInfo));
    }

    @Test
    public void testByteArray() {
        hiveTypeToJson(new HiveType(new BytesWritable("byte array".getBytes()), binaryTypeInfo));
    }

    @Test
    public void testDecimal() {
        hiveTypeToJson(new HiveType(new HiveDecimalWritable(new HiveDecimal(BigDecimal.ONE)), decimalTypeInfo));
    }

    @Test
    public void testList() {
        hiveTypeToJson(new HiveType(Arrays.asList(new Object[] { new Text("one"), new Text("two") }),
                getListTypeInfo(stringTypeInfo)));
    }

    @Test
    public void testMap() {
        hiveTypeToJson(new HiveType(Collections.singletonMap(new IntWritable(1), new Text("key")), getMapTypeInfo(
                intTypeInfo, stringTypeInfo)));
    }

    @Test
    public void testStruct() {
        List<String> names = Arrays.asList(new String[] { "one", "two" });
        List<TypeInfo> types = Arrays.asList(new TypeInfo[] { stringTypeInfo, intTypeInfo });
        hiveTypeToJson(new HiveType(Arrays.asList(new Object[] { new Text("first"), new IntWritable(2) }), getStructTypeInfo(names, types)));
    }

    private void hiveTypeToJson(HiveType obj) {
        ContentBuilder.generate(out, new HiveValueWriter()).value(obj).flush().close();
        System.out.println(out.bytes());
    }
}