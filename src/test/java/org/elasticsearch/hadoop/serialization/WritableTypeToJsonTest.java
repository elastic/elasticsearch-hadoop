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

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.elasticsearch.hadoop.mr.LinkedMapWritable;
import org.elasticsearch.hadoop.mr.WritableValueWriter;
import org.elasticsearch.hadoop.serialization.builder.ContentBuilder;
import org.elasticsearch.hadoop.util.FastByteArrayOutputStream;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class WritableTypeToJsonTest {

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
        writableTypeToJson(null);
    }

    @Test
    public void testNullWritable() throws Exception {
        writableTypeToJson(NullWritable.get());
    }

    @Test
    public void testString() {
        writableTypeToJson(new Text("some text"));
    }

    @Test
    public void testUTF8() {
        writableTypeToJson(new UTF8("some utf8"));
    }

    @Test
    public void testInteger() {
        writableTypeToJson(new IntWritable(Integer.MAX_VALUE));
    }

    @Test
    public void testLong() {
        writableTypeToJson(new LongWritable(Long.MAX_VALUE));
    }

    @Test
    public void testVInteger() {
        writableTypeToJson(new VIntWritable(Integer.MAX_VALUE));
    }

    @Test
    public void testVLong() {
        writableTypeToJson(new VLongWritable(Long.MAX_VALUE));
    }

    @Test
    public void testDouble() {
        writableTypeToJson(new DoubleWritable(Double.MAX_VALUE));
    }

    @Test
    public void testFloat() {
        writableTypeToJson(new FloatWritable(Float.MAX_VALUE));
    }

    @Test
    public void testBoolean() {
        writableTypeToJson(new BooleanWritable(Boolean.TRUE));
    }

    @Test
    public void testMD5Hash() {
        writableTypeToJson(MD5Hash.digest("md5hash"));
    }

    @Test
    public void testByte() {
        writableTypeToJson(new ByteWritable(Byte.MAX_VALUE));
    }

    @Test
    public void testByteArray() {
        writableTypeToJson(new BytesWritable("byte array".getBytes()));
    }

    @Test
    public void testArray() {
        writableTypeToJson(new ArrayWritable(new String[] { "one", "two" }));
    }

    @Test
    public void testMap() {
        LinkedMapWritable map = new LinkedMapWritable();
        map.put(new Text("key"), new IntWritable(1));
        map.put(new BooleanWritable(Boolean.TRUE), new ArrayWritable(new String[] { "one", "two" }));
        writableTypeToJson(map);
    }

    private void writableTypeToJson(Writable obj) {
        ContentBuilder.generate(out, new WritableValueWriter(false)).value(obj).flush().close();
        System.out.println(out.bytes());
    }
}