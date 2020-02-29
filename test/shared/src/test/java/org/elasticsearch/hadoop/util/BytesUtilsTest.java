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


// see http://unicode-table.com for a graphical representation
public class BytesUtilsTest {

    @Test
    public void testByteCounting() throws Exception {
        // char   encoding   size
        // 1    = $          1
        // 2    = \u00A2     2
        // 3    = \u20AC     3

        // this falls outside Unicode BMP so we need to special handling
        // this takes two chars - high and low surrogate so
        // \u10348
        // 4    = high s     2
        // 5    = low  s     2
        // 6    = $          1
        // 7    = \u00A2     2
        // 8    = a          1

        // maybe it's LE or BE :)
        char[] chars = Character.toChars(0x10348);

        // thus
        // offset of each char in bytes (based on the offset of the previous char)
        // 0 + 1 + 2 + 3 + (2 + 2) + 1 + 2 + 1
        String unicode = "$\u00A2\u20AC" + chars[0] + chars[1] + "$\u00A2a";

        // 7 unicode points but the non-BMP one takes 1 extra
        assertEquals(8, unicode.length());

        int[] bytePosition = BytesUtils.charToBytePosition(new BytesArray(unicode), 0, 2, 3, 1, 6, 3, 2, 5, 4);

        assertEquals(0, bytePosition[0]);
        assertEquals(0 + 1 + 2, bytePosition[1]);
        assertEquals(0 + 1 + 2 + 3, bytePosition[2]);
        assertEquals(0 + 1, bytePosition[3]);
        assertEquals(0 + 1 + 2 + 3 + (2 + 2) + 1, bytePosition[4]);
        assertEquals(0 + 1 + 2 + 3, bytePosition[5]);
        assertEquals(0 + 1 + 2, bytePosition[6]);
        assertEquals(0 + 1 + 2 + 3 + (2 + 2), bytePosition[7]);
        assertEquals(0 + 1 + 2 + 3 + (2), bytePosition[8]);
    }

    @Test
    public void testAnotherByteCounting() throws Exception {
        // 1 + 2 + 1
        String utf8 = "W\u00FCr";
        int[] bytePosition = BytesUtils.charToBytePosition(new BytesArray(utf8), 0, 1, 2);
        assertEquals(0, bytePosition[0]);
        assertEquals(1, bytePosition[1]);
        assertEquals(1 + 2, bytePosition[2]);

        // symbol G
        char[] chars = Character.toChars(0x1D11E);
        // 3 + 3 + (2 + 2) + 2 + 1
        utf8 = "\uFEF2\uFEE6" + chars[0] + chars[1] + "\u00A2@";

        // 5 code points but 1 takes 2 chars in UTF-16
        assertEquals(5 + 1, utf8.length());

        bytePosition = BytesUtils.charToBytePosition(new BytesArray(utf8), 0, 1, 2, 3, 4, 5);
        assertEquals(0, bytePosition[0]);
        assertEquals(0 + 3, bytePosition[1]);
        assertEquals(0 + 3 + 3, bytePosition[2]);
        assertEquals(0 + 3 + 3 + (2), bytePosition[3]);
        assertEquals(0 + 3 + 3 + (2 + 2), bytePosition[4]);
        assertEquals(0 + 3 + 3 + (2 + 2) + 2, bytePosition[5]);
    }

    @Test
    public void testStreamSize() throws Exception {
        BytesArray input = IOUtils.asBytes(getClass().getResourceAsStream("escape-size.txt"));
        assertEquals(5 * 2, input.size);
    }

    @Test
    public void testByteToChar() throws Exception {
        BytesArray input = IOUtils.asBytes(getClass().getResourceAsStream("escaped-chars.txt"));
        int[] chars = new int[] { 9, 14, 0 };
        int[] bytePositions = BytesUtils.charToBytePosition(input, chars);
        assertEquals(10, bytePositions[0]);
        // LF/CR
        assertEquals((TestUtils.isWindows() ? 19 : 20), bytePositions[1]);
        assertEquals(0, bytePositions[2]);
    }

    // @Test
    public void testByteToCharFromFile() throws Exception {
        BytesArray input = IOUtils.asBytes(getClass().getResourceAsStream("/org/elasticsearch/hadoop/serialization/scrollReaderTestData/matched-queries/scroll.json"));
        int[] chars = new int[] { 555, 558, 1008, 1009, 1649 };
        int[] bytePositions = BytesUtils.charToBytePosition(input, chars);
        assertEquals(570, bytePositions[0]);
        assertEquals(575, bytePositions[1]);
        assertEquals(1071, bytePositions[2]);
        assertEquals(1072, bytePositions[3]);
        //assertEquals(1640, bytePositions[1]);
    }
}