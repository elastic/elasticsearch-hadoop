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

public class BytesArrayPoolTest {

    @Test
    public void testAddAndReset() throws Exception {
        BytesArrayPool pool = new BytesArrayPool();

        pool.get().bytes("Test");
        pool.get().bytes("Data");
        pool.get().bytes("Rules");

        BytesRef ref = new BytesRef();

        assertEquals(13, pool.length());
        ref.add(pool);
        assertEquals("TestDataRules", ref.toString());

        BytesRef ref2 = new BytesRef();

        pool.reset();

        pool.get().bytes("New");
        pool.get().bytes("Info");

        assertEquals(7, pool.length());
        ref2.add(pool);
        assertEquals("NewInfo", ref2.toString());
    }
}