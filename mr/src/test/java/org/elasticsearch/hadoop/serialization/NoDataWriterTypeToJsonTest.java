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

import org.apache.hadoop.io.*;
import org.elasticsearch.hadoop.mr.LinkedMapWritable;
import org.elasticsearch.hadoop.serialization.builder.ContentBuilder;
import org.elasticsearch.hadoop.serialization.bulk.DeleteBulkFactory;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.FastByteArrayOutputStream;
import org.junit.*;

@SuppressWarnings("deprecation")
public class NoDataWriterTypeToJsonTest {
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
        writableTypeToJson(null, new BytesArray("null"));
    }

    @Test
    public void testNullWritable() throws Exception {
        writableTypeToJson(NullWritable.get(), new BytesArray("null"));
    }

    @Test
    public void testString() {
        writableTypeToJson(new Text("some text"), new BytesArray("\"some text\"".getBytes()));
    }

    @Test
    public void testUTF8() {
        writableTypeToJson(new UTF8("some utf8"), new BytesArray("\"some utf8\"".getBytes()));
    }

    @Test
    public void testInteger() {
        writableTypeToJson(new IntWritable(Integer.MAX_VALUE), new BytesArray(Integer.toString(Integer.MAX_VALUE)));
    }

    @Test
    public void testLong() {
        writableTypeToJson(new LongWritable(Long.MAX_VALUE), new BytesArray(Long.toString(Long.MAX_VALUE)));
    }

    @Test
    public void testVInteger() {
        writableTypeToJson(new VIntWritable(Integer.MAX_VALUE), new BytesArray(Integer.toString(Integer.MAX_VALUE)));
    }

    @Test
    public void testVLong() {
        writableTypeToJson(new VLongWritable(Long.MAX_VALUE), new BytesArray(Long.toString(Long.MAX_VALUE)));
    }

    @Test
    public void testDouble() {
        writableTypeToJson(new DoubleWritable(Double.MAX_VALUE), new BytesArray(Double.toString(Double.MAX_VALUE)));
    }

    @Test
    public void testFloat() {
        writableTypeToJson(new FloatWritable(Float.MAX_VALUE), new BytesArray(Float.toString(Float.MAX_VALUE)));
    }

    @Test
    public void testBoolean() {
        writableTypeToJson(new BooleanWritable(Boolean.TRUE), new BytesArray("true"));
    }

    @Test
    public void testMD5Hash() {
        writableTypeToJson(MD5Hash.digest("md5hash"), new BytesArray("\"f9d08276bc85d30d578e8883f3c7e843\"".getBytes()));
    }

    @Test
    public void testByte() {
        writableTypeToJson(new ByteWritable(Byte.MAX_VALUE), new BytesArray(Byte.toString(Byte.MAX_VALUE)));
    }

    @Test
    public void testByteArray() {
        writableTypeToJson(new BytesWritable("byte array".getBytes()), new BytesArray("\"Ynl0ZSBhcnJheQ==\""));
    }

    @Test
    public void testArray() {
        writableTypeToJson(new ArrayWritable(new String[]{"one", "two"}), new BytesArray(""));
    }

    @Test
    public void testMap() {
        LinkedMapWritable map = new LinkedMapWritable();
        map.put(new Text("key"), new IntWritable(1));
        map.put(new BooleanWritable(Boolean.TRUE), new ArrayWritable(new String[]{"one", "two"}));
        writableTypeToJson(map, new BytesArray(""));
    }

    private void writableTypeToJson(Writable obj, BytesArray expected) {
        ContentBuilder.generate(out, new DeleteBulkFactory.NoDataWriter()).value(obj).flush().close();
        Assert.assertEquals(expected.toString(), out.bytes().toString());
    }
}
