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
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;

import static org.hamcrest.CoreMatchers.*;

@Ignore
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
    public void testNamedNull() {
        String expected = "[null]";
        assertThat(expected, is(pigTypeToJson(createTuple(null, createSchema("name:bytearray")))));
    }

    @Test
    public void testAnonNull() {
        String expected = "[null]";
        assertThat(expected, is(pigTypeToJson(createTuple(null, createSchema("bytearray")))));
    }

    @Test
    public void testNamedString() {
        String expected = "[\"some string\"]";
        assertThat(expected, is(pigTypeToJson(createTuple("some string", createSchema("name:chararray")))));
    }

    @Test
    public void testAnonString() {
        String expected = "[\"some string\"]";
        assertThat(expected, is(pigTypeToJson(createTuple("some string", createSchema("chararray")))));
    }

    @Test
    public void testLong() {
        String expected = "[" + Long.MAX_VALUE + "]";
        assertThat(expected, is(pigTypeToJson(createTuple(Long.MAX_VALUE, createSchema("name:long")))));
    }

    @Test
    public void testInteger() {
        String expected = "[" + Integer.MAX_VALUE + "]";
        assertThat(expected, is(pigTypeToJson(createTuple(Integer.MAX_VALUE, createSchema("name:int")))));
    }

    @Test
    public void testDouble() {
        String expected = "[" + Double.MAX_VALUE + "]";
        assertThat(expected, is(pigTypeToJson(createTuple(Double.MAX_VALUE, createSchema("name:double")))));
    }

    @Test
    public void testFloat() {
        String expected = "[" + Float.MAX_VALUE + "]";
        assertThat(expected, is(pigTypeToJson(createTuple(Float.MAX_VALUE, createSchema("name:float")))));
    }

    @Test
    public void testBoolean() {
        String expected = "[" + Boolean.TRUE + "]";
        assertThat(expected, is(pigTypeToJson(createTuple(Boolean.TRUE, createSchema("name:boolean")))));
    }

    @Test
    public void testByte() {
        String expected = "[" + Byte.MAX_VALUE + "]";
        // byte is not recognized by the schema
        assertThat(expected, is(pigTypeToJson(createTuple(Byte.MAX_VALUE, createSchema("name:int")))));
    }

    @Test
    public void testByteArray() {
        String expected = "[\"Ynl0ZSBhcnJheQ==\"]";
        assertThat(expected, is(pigTypeToJson(createTuple(new DataByteArray("byte array".getBytes()), createSchema("name:bytearray")))));
    }

    @Test
    public void testNamedTuple() {
        String expected = "[[\"one\",\"two\"]]";
        assertThat(expected, is(pigTypeToJson(createTuple(TupleFactory.getInstance().newTuple(Arrays.asList(new String[] { "one", "two" })),
                createSchema("namedTuple: (first:chararray, second:chararray)")))));
    }

    @Test
    public void testAnonymousTuple() {
        String expected = "[[\"xxx\",\"yyy\",\"zzz\"]]";
        assertThat(expected, is(pigTypeToJson(createTuple(
                TupleFactory.getInstance().newTuple(Arrays.asList(new String[] { "xxx", "yyy", "zzz" })),
                createSchema("anonTuple: (chararray, chararray, chararray)")))));
    }

    @Test
    public void testAnonMap() {
        String expected = "[{\"one\":1,\"two\":2,\"three\":3}]";
        Map<String, Number> map = new LinkedHashMap<String, Number>();
        map.put("one", 1);
        map.put("two", 2);
        map.put("three", 3);
        assertThat(pigTypeToJson(createTuple(map, createSchema("[int]"))), is(expected));
    }

    @Test
    public void testNamedMap() {
        String expected = "[{\"one\":1,\"two\":2,\"three\":3}]";
        Map<String, Number> map = new LinkedHashMap<String, Number>();
        map.put("one", 1);
        map.put("two", 2);
        map.put("three", 3);
        assertThat(pigTypeToJson(createTuple(map, createSchema("map: [int]"))), is(expected));
    }

    @Test
    public void testNamedBag() {
        String expected = "[[[\"one\",\"two\",\"three\"],[\"one\",\"two\",\"three\"],[\"one\",\"two\",\"three\"]]]";

        Tuple tuple = TupleFactory.getInstance().newTuple(Arrays.asList(new String[] { "one", "two", "three" }));
        assertThat(pigTypeToJson(createTuple(new DefaultDataBag(Arrays.asList(new Tuple[] { tuple, tuple, tuple })),
                createSchema("bag: {t:(first:chararray, second:chararray, third: chararray)}"))), is(expected));
    }

    @Test
    public void testBagWithAnonTuple() {
        String expected = "[[[\"xxx\",\"yyy\"],[\"xxx\",\"yyy\"],[\"xxx\",\"yyy\"]]]";

        Tuple tuple = TupleFactory.getInstance().newTuple(Arrays.asList(new String[] { "xxx", "yyy" }));
        assertThat((pigTypeToJson(createTuple(new DefaultDataBag(Arrays.asList(new Tuple[] { tuple, tuple, tuple })),
                createSchema("bag: {t:(chararray, chararray)}")))), is(expected));
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

    private String pigTypeToJson(PigTuple obj) {
        ContentBuilder.generate(out, new PigValueWriter(false)).value(obj).flush().close();
        return out.bytes().toString();
    }
}