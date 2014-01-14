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
package org.elasticsearch.hadoop.util;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 *
 */
public class WritableUtilsTest {

    @Test
    public void testNull() {
        testToWritableAndBack(null);
    }

    @Test
    public void testString() {
        testToWritableAndBack("some string");
    }

    @Test
    public void testLong() {
        testToWritableAndBack(Long.MAX_VALUE);
    }

    @Test
    public void testInteger() {
        testToWritableAndBack(Integer.MAX_VALUE);
    }

    @Test
    public void testDouble() {
        testToWritableAndBack(Double.MAX_VALUE);
    }

    @Test
    public void testFloat() {
        testToWritableAndBack(Float.MAX_VALUE);
    }

    @Test
    public void testBoolean() {
        testToWritableAndBack(Boolean.TRUE);
    }

    @Test
    public void testByte() {
        testToWritableAndBack(Byte.MAX_VALUE);
    }

    @Test
    public void testByteArray() {
        testToWritableAndBack("byte array".getBytes());
    }

    @Test
    public void testList() {
        testToWritableAndBack(Arrays.asList(new String[] { "one", "two" }));
    }

    @Test
    public void testMap() {
        testToWritableAndBack(Collections.singletonMap("key", "value"));
    }

    private void testToWritableAndBack(Object obj) {
        assertEquals(obj, WritableUtils.fromWritable(WritableUtils.toWritable(obj)));
    }
}