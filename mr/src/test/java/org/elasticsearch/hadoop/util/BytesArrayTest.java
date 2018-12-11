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

import org.junit.Test;

import static org.junit.Assert.*;

public class BytesArrayTest {

    @Test
    public void equalsHashCode() {
        BytesArray a = new BytesArray(10);
        BytesArray b = new BytesArray(20);
        BytesArray c = new BytesArray("");

        assertEquals(a, b);
        assertEquals(b, a);
        assertEquals(a, c);
        assertEquals(b, c);
        assertEquals(c, a);
        assertEquals(c, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertEquals(a.hashCode(), c.hashCode());

        BytesArray d = new BytesArray("test");
        BytesArray e = new BytesArray(20);
        e.add("test");

        assertEquals(d, e);
        assertEquals(e, d);
        assertEquals(e.hashCode(), d.hashCode());
    }
}