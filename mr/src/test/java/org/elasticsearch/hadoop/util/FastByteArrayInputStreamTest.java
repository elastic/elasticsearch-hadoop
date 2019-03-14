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

import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FastByteArrayInputStreamTest {

    private FastByteArrayInputStream whole;
    private FastByteArrayInputStream middle;

    @Before
    public void setup() {
        BytesArray all = new BytesArray(new byte[]{1, 2, 3, 4, 5, 6});
        BytesArray middleFour = new BytesArray(all.bytes, 1, 4);
        whole = new FastByteArrayInputStream(all);
        middle = new FastByteArrayInputStream(middleFour);
    }

    @Test
    public void read() throws Exception {
        assertEquals(1, whole.read());
        assertEquals(2, middle.read());
    }

    @Test
    public void read1() throws Exception {
        byte[] array = new byte[6];
        int bytesRead = whole.read(array, 0, array.length);
        assertEquals(6, bytesRead);
        assertArrayEquals(new byte[]{1, 2, 3, 4, 5, 6}, array);

        Arrays.fill(array, (byte)0);
        bytesRead = middle.read(array, 0, array.length);
        assertEquals(4, bytesRead);
        assertArrayEquals(new byte[]{2, 3, 4, 5, 0, 0}, array);
    }

    @Test
    public void skip() throws Exception {
        {
            long skipped = whole.skip(1);
            int value = whole.read();
            assertEquals(1L, skipped);
            assertEquals(2, value);
        }
        {
            long skipped = middle.skip(1);
            int value = middle.read();
            assertEquals(1L, skipped);
            assertEquals(3, value);
        }
    }

    @Test
    public void available() throws Exception {
        assertEquals(6, whole.available());
        assertEquals(4, middle.available());
    }

    @Test
    public void position() throws Exception {
        {
            int pos0 = whole.position();
            whole.skip(1);
            int pos1 = whole.position();
            whole.skip(6);
            int pos2 = whole.position();
            assertEquals(0, pos0);
            assertEquals(1, pos1);
            assertEquals(6, pos2);
        }
        {
            int pos0 = middle.position();
            middle.skip(1);
            int pos1 = middle.position();
            middle.skip(6);
            int pos2 = middle.position();
            assertEquals(0, pos0);
            assertEquals(1, pos1);
            assertEquals(4, pos2);
        }
    }

    @Test
    public void markSupported() throws Exception {
        assertTrue(whole.markSupported());
        assertTrue(middle.markSupported());
    }

    @Test
    public void markAndReset() throws Exception {
        Assume.assumeTrue(whole.markSupported());
        whole.mark(1024);
        int read = whole.read();
        whole.reset();
        int reread = whole.read();
        assertEquals(read, reread);
    }

}