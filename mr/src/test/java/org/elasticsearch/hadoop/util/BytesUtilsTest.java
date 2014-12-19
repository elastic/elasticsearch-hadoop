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

import static org.junit.Assert.assertEquals;

public class BytesUtilsTest {

    @Test
    public void testByteCounting() throws Exception {
        // 0 + 1 + 2 + 3 + 4 + 1 + 2 + 1
		String unicode = "$\u00A2\u20AC\ud800\udc48$\u00A2a";
		System.out.println(unicode);

        int[] bytePosition = BytesUtils.charToBytePosition(new BytesArray(unicode), 0, 2, 3, 1, 6, 3, 2, 5, 4);

        assertEquals(0, bytePosition[0]);
        assertEquals(0 + 1 + 2, bytePosition[1]);
        assertEquals(0 + 1 + 2 + 3, bytePosition[2]);
        assertEquals(0 + 1, bytePosition[3]);
        assertEquals(0 + 1 + 2 + 3 + 4 + 1 + 2, bytePosition[4]);
        assertEquals(0 + 1 + 2 + 3, bytePosition[5]);
        assertEquals(0 + 1 + 2, bytePosition[6]);
        assertEquals(0 + 1 + 2 + 3 + 4 + 1, bytePosition[7]);
        assertEquals(0 + 1 + 2 + 3 + 4, bytePosition[8]);
    }
}
