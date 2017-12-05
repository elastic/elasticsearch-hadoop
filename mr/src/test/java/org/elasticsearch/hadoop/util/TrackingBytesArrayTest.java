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

import java.io.ByteArrayOutputStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


public class TrackingBytesArrayTest {

    private TrackingBytesArray data;

    @Before
    public void init() {
        data = new TrackingBytesArray(new BytesArray(256));
    }

    @After
    public void destroy() {
        data = null;
    }

    @Test
    public void testAddArraySize() throws Exception {
        assertEquals(0, data.length());
        data.copyFrom(new BytesArray("one"));
        assertEquals(3, data.length());
        data.copyFrom(new BytesArray("two"));
        assertEquals(6, data.length());
        data.copyFrom(new BytesArray("three"));
        assertEquals(11, data.length());
    }

    @Test
    public void testAddRefSize() throws Exception {
        BytesRef ref = new BytesRef();
        ref.add(new BytesArray("one"));
        ref.add(new BytesArray("three"));
        data.copyFrom(ref);
        assertEquals(8, data.length());
    }

    @Test
    public void testRemoveSize() throws Exception {
        assertEquals(0, data.length());
        data.copyFrom(new BytesArray("a"));
        data.copyFrom(new BytesArray("bb"));
        data.copyFrom(new BytesArray("ccc"));
        data.copyFrom(new BytesArray("dddd"));
        assertEquals(10, data.length());
        data.remove(1);
        assertEquals(8, data.length());
        data.remove(1);
        assertEquals(5, data.length());
    }

    @Test
    public void testWriteAfterAdding() throws Exception {
        data.copyFrom(new BytesArray("a"));
        data.copyFrom(new BytesArray("bb"));

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        data.writeTo(out);
        assertEquals("abb", out.toString());
    }

    @Test
    public void testWriteAfterRemoving() throws Exception {
        data.copyFrom(new BytesArray("a"));
        data.copyFrom(new BytesArray("bb"));
        data.copyFrom(new BytesArray("ccc"));

        data.remove(1);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        data.writeTo(out);
        assertEquals("accc", out.toString());
    }

    @Test
    public void testPopData() throws Exception {
        assertEquals(0, data.length());
        data.copyFrom(new BytesArray("a"));
        data.copyFrom(new BytesArray("bb"));
        data.copyFrom(new BytesArray("ccc"));
        data.copyFrom(new BytesArray("dddd"));
        assertEquals(10, data.length());
        BytesArray entry = data.pop();
        assertEquals(9, data.length());
        assertEquals(1, entry.length());
        entry = data.pop();
        assertEquals(7, data.length());
        assertEquals(2, entry.length());
    }
}