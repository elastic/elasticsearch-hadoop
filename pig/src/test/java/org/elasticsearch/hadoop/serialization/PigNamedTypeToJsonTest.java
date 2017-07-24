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
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.pig.PigTuple;
import org.elasticsearch.hadoop.pig.PigValueWriter;
import org.elasticsearch.hadoop.serialization.builder.ContentBuilder;
import org.elasticsearch.hadoop.util.FastByteArrayOutputStream;
import org.elasticsearch.hadoop.util.TestSettings;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

import static org.hamcrest.CoreMatchers.*;

public class PigNamedTypeToJsonTest {

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
        String expected = "{\"name\":null}";
        assertThat(pigTypeToJson(createTuple(null, createSchema("name:bytearray"))), is(expected));
    }

    @Test
    public void testAnonymousNull() {
        String expected = "{\"val_0\":null}";
        assertThat(pigTypeToJson(createTuple(null, createSchema("bytearray"))), is(expected));
    }

    @Test
    public void testNamedString() {
        String expected = "{\"name\":\"some string\"}";
        assertThat(pigTypeToJson(createTuple("some string", createSchema("name:chararray"))), is(expected));
    }

    @Test
    public void testAnonString() {
        String expected = "{\"val_0\":\"some string\"}";
        assertThat(pigTypeToJson(createTuple("some string", createSchema("chararray"))), is(expected));
    }

    @Test
    public void testNamedLong() {
        String expected = "{\"name\":" + Long.MAX_VALUE + "}";
        assertThat(pigTypeToJson(createTuple(Long.MAX_VALUE, createSchema("name:long"))), is(expected));
    }

    @Test
    public void testAnonLong() {
        String expected = "{\"val_0\":" + Long.MAX_VALUE + "}";
        assertThat(pigTypeToJson(createTuple(Long.MAX_VALUE, createSchema("long"))), is(expected));
    }

    @Test
    public void testAnonInteger() {
        String expected = "{\"val_0\":" + Integer.MAX_VALUE + "}";
        assertThat(pigTypeToJson(createTuple(Integer.MAX_VALUE, createSchema("int"))), is(expected));
    }

    @Test
    public void testNamedInteger() {
        String expected = "{\"name\":" + Integer.MAX_VALUE + "}";
        assertThat(pigTypeToJson(createTuple(Integer.MAX_VALUE, createSchema("name:int"))), is(expected));
    }

    @Test
    public void testNamedDouble() {
        String expected = "{\"name\":" + Double.MAX_VALUE + "}";
        assertThat(pigTypeToJson(createTuple(Double.MAX_VALUE, createSchema("name:double"))), is(expected));
    }

    @Test
    public void testAnonDouble() {
        String expected = "{\"val_0\":" + Double.MAX_VALUE + "}";
        assertThat(pigTypeToJson(createTuple(Double.MAX_VALUE, createSchema("double"))), is(expected));
    }

    @Test
    public void testNamedFloat() {
        String expected = "{\"name\":" + Float.MAX_VALUE + "}";
        assertThat(pigTypeToJson(createTuple(Float.MAX_VALUE, createSchema("name:float"))), is(expected));
    }

    @Test
    public void testAnonFloat() {
        String expected = "{\"val_0\":" + Float.MAX_VALUE + "}";
        assertThat(pigTypeToJson(createTuple(Float.MAX_VALUE, createSchema("float"))), is(expected));
    }

    @Test
    public void testAnonBoolean() {
        String expected = "{\"val_0\":" + Boolean.TRUE + "}";
        assertThat(pigTypeToJson(createTuple(Boolean.TRUE, createSchema("boolean"))), is(expected));
    }

    @Test
    public void testNamedBoolean() {
        String expected = "{\"name\":" + Boolean.TRUE + "}";
        assertThat(pigTypeToJson(createTuple(Boolean.TRUE, createSchema("name:boolean"))), is(expected));
    }

    @Test
    public void testNamedByte() {
        String expected = "{\"name\":" + Byte.MAX_VALUE + "}";
        // byte is not recognized by the schema
        assertThat(pigTypeToJson(createTuple(Byte.MAX_VALUE, createSchema("name:int"))), is(expected));
    }

    @Test
    public void testAnonByte() {
        String expected = "{\"val_0\":" + Byte.MAX_VALUE + "}";
        // byte is not recognized by the schema
        assertThat(pigTypeToJson(createTuple(Byte.MAX_VALUE, createSchema("int"))), is(expected));
    }

    @Test
    public void testNamedByteArray() {
        String expected = "{\"name\":\"Ynl0ZSBhcnJheQ==\"}";
        assertThat(pigTypeToJson(createTuple(new DataByteArray("byte array".getBytes()), createSchema("name:bytearray"))), is(expected));
    }

    @Test
    public void testAnonByteArray() {
        String expected = "{\"val_0\":\"Ynl0ZSBhcnJheQ==\"}";
        assertThat(pigTypeToJson(createTuple(new DataByteArray("byte array".getBytes()), createSchema("bytearray"))),
                is(expected));
    }

    @Test
    public void testNamedTuple() {
        String expected = "{\"namedtuple\":[{\"first\":\"one\",\"second\":\"two\"}]}";
        assertThat(pigTypeToJson(createTuple(TupleFactory.getInstance().newTuple(Arrays.asList(new String[] { "one", "two" })),
                createSchema("namedtuple: (first:chararray, second:chararray)"))), is(expected));
    }

    @Test
    public void testNamedTupleWithMixedValues() {
        String expected = "{\"namedtuplewithmixedvalues\":[{\"first\":1,\"second\":\"two\"}]}";
        assertThat(pigTypeToJson(createTuple(TupleFactory.getInstance().newTuple(Arrays.asList(new Object[] { 1, "two" })),
                        createSchema("namedtuplewithmixedvalues: (first:int, second:chararray)"))), is(expected));
    }

    @Test
    public void testAnonTuple() {
        String expected = "{\"anontuple\":[{\"val_0\":\"xxx\",\"val_1\":\"yyy\",\"val_2\":\"zzz\"}]}";
        assertThat(pigTypeToJson(createTuple(
                TupleFactory.getInstance().newTuple(Arrays.asList(new String[] { "xxx", "yyy", "zzz" })),
                        createSchema("anontuple: (chararray, chararray, chararray)"))), is(expected));
    }

    @Test
    public void testNamedMap() {
        String expected = "{\"map\":{\"one\":1,\"two\":2,\"three\":3}}";
        Map<String, Number> map = new LinkedHashMap<String, Number>();
        map.put("one", 1);
        map.put("two", 2);
        map.put("three", 3);
        assertThat(pigTypeToJson(createTuple(map, createSchema("map: [int]"))), is(expected));
    }

    @Test
    public void testAnonMap() {
        String expected = "{\"map_0\":{\"one\":1,\"two\":2,\"three\":3}}";
        Map<String, Number> map = new LinkedHashMap<String, Number>();
        map.put("one", 1);
        map.put("two", 2);
        map.put("three", 3);
        assertThat(pigTypeToJson(createTuple(map, createSchema("[int]"))), is(expected));
    }

    @Test
    public void testNamedBag() {
        String expected = "{\"bag\":[[{\"first\":\"one\",\"second\":\"two\",\"third\":\"three\"}]," +
                                     "[{\"first\":\"one\",\"second\":\"two\",\"third\":\"three\"}]," +
                                     "[{\"first\":\"one\",\"second\":\"two\",\"third\":\"three\"}]]}";

        Tuple tuple = TupleFactory.getInstance().newTuple(Arrays.asList(new String[] { "one", "two", "three" }));
        assertThat(pigTypeToJson(createTuple(new DefaultDataBag(Arrays.asList(new Tuple[] { tuple, tuple, tuple })),
                createSchema("bag: {t:(first:chararray, second:chararray, third: chararray)}"))), is(expected));
    }

    @Test
    public void testBagWithAnonTuple() {
        String expected = "{\"bag_0\":[[{\"val_0\":\"xxx\",\"val_1\":\"yyy\"}]," +
                                       "[{\"val_0\":\"xxx\",\"val_1\":\"yyy\"}]," +
                                       "[{\"val_0\":\"xxx\",\"val_1\":\"yyy\"}]]}";

        Tuple tuple = TupleFactory.getInstance().newTuple(Arrays.asList(new String[] { "xxx", "yyy" }));

        assertThat(pigTypeToJson(createTuple(new DefaultDataBag(Arrays.asList(new Tuple[] { tuple, tuple, tuple })),
                createSchema("{t:(chararray, chararray)}"))), is(expected));
    }

    @Test
    public void testNamedTupleWithExclusion() {
        Settings settings = new TestSettings();
        settings.setProperty(ConfigurationOptions.ES_MAPPING_EXCLUDE, "namedtuple.second");
        String expected = "{\"namedtuple\":[{\"first\":\"one\"}]}";
        assertThat(pigTypeToJson(createTuple(TupleFactory.getInstance().newTuple(Arrays.asList(new String[] { "one", "two" })),
                createSchema("namedtuple: (first:chararray, second:chararray)")), settings), is(expected));
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
        return pigTypeToJson(obj, null);
    }

    private String pigTypeToJson(PigTuple obj, Settings settings) {
        PigValueWriter writer = new PigValueWriter(true);
        if (settings != null) {
            // Make sure to write with use.field.names as true
            settings.setProperty("es.mapping.pig.tuple.use.field.names", "true");
            writer.setSettings(settings);
        }
        ContentBuilder.generate(out, writer).value(obj).flush().close();
        return out.bytes().toString();
    }
}