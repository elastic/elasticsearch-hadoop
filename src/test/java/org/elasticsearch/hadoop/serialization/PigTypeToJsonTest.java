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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.pig.ResourceSchema;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.Utils;
import org.elasticsearch.hadoop.pig.PigTuple;
import org.elasticsearch.hadoop.pig.PigValueWriter;
import org.elasticsearch.hadoop.serialization.builder.ContentBuilder;
import org.elasticsearch.hadoop.util.FastByteArrayOutputStream;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class PigTypeToJsonTest {

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
        pigTypeToJson(createTuple(null, createSchema("name:bytearray")));
    }

    @Test
    public void testString() {
        pigTypeToJson(createTuple("some string", createSchema("name:chararray")));
    }

    @Test
    public void testLong() {
        pigTypeToJson(createTuple(Long.MAX_VALUE, createSchema("name:long")));
    }

    @Test
    public void testInteger() {
        pigTypeToJson(createTuple(Integer.MAX_VALUE, createSchema("name:int")));
    }

    @Test
    public void testDouble() {
        pigTypeToJson(createTuple(Double.MAX_VALUE, createSchema("name:double")));
    }

    @Test
    public void testFloat() {
        pigTypeToJson(createTuple(Float.MAX_VALUE, createSchema("name:float")));
    }

    @Test
    public void testBoolean() {
        pigTypeToJson(createTuple(Boolean.TRUE, createSchema("name:boolean")));
    }

    @Test
    public void testByte() {
        // byte is not recognized by the schema
        pigTypeToJson(createTuple(Byte.MAX_VALUE, createSchema("name:int")));
    }

    @Test
    public void testByteArray() {
        pigTypeToJson(createTuple(new DataByteArray("byte array".getBytes()), createSchema("name:bytearray")));
    }

    @Test
    public void testTuple() {
        pigTypeToJson(createTuple(TupleFactory.getInstance().newTuple(Arrays.asList(new String[] { "one", "two" })),
                createSchema("tuple: (first:chararray, second:chararray)")));
    }

    @Test
    public void testMap() {
        Map<String, Number> map = new LinkedHashMap<String, Number>();
        map.put("one", 1);
        map.put("two", 2);
        map.put("three", 3);
        pigTypeToJson(createTuple(map, createSchema("map: [int]")));
    }

    @Test
    public void testBag() {
        Tuple tuple = TupleFactory.getInstance().newTuple(Arrays.asList(new String[] { "one", "two", "three" }));

        pigTypeToJson(createTuple(new DefaultDataBag(Arrays.asList(new Tuple[] { tuple, tuple, tuple })),
                createSchema("bag: {t:(first:chararray, second:chararray, third: chararray)}")));
    }

    private ResourceSchema createSchema(String schema) {
        try {
            return new ResourceSchema(Utils.getSchemaFromString(schema));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private PigTuple createTuple(Object obj, ResourceSchema schema) {
        PigTuple tuple = new PigTuple(schema);
        tuple.setTuple(TupleFactory.getInstance().newTuple(obj));
        return tuple;
    }

    private void pigTypeToJson(PigTuple obj) {
        ContentBuilder.generate(out, new PigValueWriter()).value(obj).flush().close();
        System.out.println(out.bytes());
    }
}