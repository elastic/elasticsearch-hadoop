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
package org.elasticsearch.hadoop.hive;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.elasticsearch.hadoop.util.WritableUtils;
import org.junit.Test;

import static org.junit.Assert.*;

import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.*;

public class HiveArrayTest {

    @Test
    public void testNull() {
        testToWritableAndBack(voidTypeInfo, null);
    }

    @Test
    public void testString() {
        testToWritableAndBack(stringTypeInfo, "some string");
    }

    @Test
    public void testLong() {
        testToWritableAndBack(longTypeInfo, Long.MAX_VALUE);
    }

    @Test
    public void testInteger() {
        testToWritableAndBack(intTypeInfo, Integer.MAX_VALUE);
    }

    @Test
    public void testDouble() {
        testToWritableAndBack(doubleTypeInfo, Double.MAX_VALUE);
    }

    @Test
    public void testFloat() {
        testToWritableAndBack(floatTypeInfo, Float.MAX_VALUE);
    }

    @Test
    public void testBoolean() {
        testToWritableAndBack(booleanTypeInfo, Boolean.TRUE);
    }

    @Test
    public void testByte() {
        testToWritableAndBack(byteTypeInfo, Byte.MAX_VALUE);
    }

    @Test
    public void testByteArray() {
        testToWritableAndBack(binaryTypeInfo, "byte array".getBytes());
    }

    @Test
    public void testList() {
        TypeInfo type = getListTypeInfo(stringTypeInfo);
        Object data = Arrays.asList(new String[] { "one", "two" });
        Writable w = WritableUtils.toWritable(data);
        assertArrayEquals(((ArrayWritable) w).toStrings(),
                ((ArrayWritable) ESSerDe.hiveToWritable(type, ESSerDe.hiveFromWritable(type, w))).toStrings());
    }

    @Test
    public void testMap() {
        TypeInfo type = getMapTypeInfo(stringTypeInfo, stringTypeInfo);
        Object data = Collections.singletonMap("key", "value");
        Writable w = WritableUtils.toWritable(data);
        assertArrayEquals(((Map) w).values().toArray(),
                ((Map) ESSerDe.hiveToWritable(type, ESSerDe.hiveFromWritable(type, w))).values().toArray());

    }

    private void testToWritableAndBack(TypeInfo type, Object data) {
        Writable w = WritableUtils.toWritable(data);
        assertEquals(w, ESSerDe.hiveToWritable(type, ESSerDe.hiveFromWritable(type, w)));
    }
}
