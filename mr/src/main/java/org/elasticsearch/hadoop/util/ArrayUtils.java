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

import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;

// taken from org.apache.lucene.util
public abstract class ArrayUtils {

    /**
     * Returns <tt>true</tt> if the two specified array slices of bytes are
     * <i>equal</i> to one another.  Two array slices are considered equal if both
     * slices contain the same number of elements, and all corresponding pairs
     * of elements in the two slices are equal.  In other words, two slices
     * are equal if they contain the same elements in the same order.  Also,
     * two slice references are considered equal if both are <tt>null</tt>.<p>
     *
     * Adapted from the JVM runtime's Array.equals for use with array slices.
     *
     * @param a one array to be tested for equality
     * @param b the other array to be tested for equality
     * @return <tt>true</tt> if the two arrays are equal
     */
    public static boolean sliceEquals(byte[] a, int offa, int lena, byte[] b, int offb, int lenb) {
        if (a==b)
            return true;
        if (a==null || b==null)
            return false;

        if (lena > a.length || lenb > b.length) {
            throw new ArrayIndexOutOfBoundsException("Given length is greater than array length");
        }

        if (offa >= a.length || offb >= b.length) {
            throw new ArrayIndexOutOfBoundsException("Given offset is is out of bounds");
        }

        if (offa + lena > a.length || offb + lenb > b.length) {
            throw new ArrayIndexOutOfBoundsException("Given offset and length is out of array bounds");
        }

        if (lenb != lena)
            return false;

        for (int i=0; i<lena; i++)
            if (a[offa + i] != b[offb + i])
                return false;

        return true;
    }

    static byte[] grow(byte[] array, int minSize) {
        assert minSize >= 0 : "size must be positive (got " + minSize + "): likely integer overflow?";
        if (array.length < minSize) {
            byte[] newArray = new byte[oversize(minSize, 1)];
            System.arraycopy(array, 0, newArray, 0, array.length);
            return newArray;
        }
        else
            return array;
    }

    static int oversize(int minTargetSize, int bytesPerElement) {

        if (minTargetSize < 0) {
            // catch usage that accidentally overflows int
            throw new EsHadoopIllegalArgumentException("invalid array size " + minTargetSize);
        }

        if (minTargetSize == 0) {
            // wait until at least one element is requested
            return 0;
        }

        // asymptotic exponential growth by 1/8th, favors
        // spending a bit more CPU to not tie up too much wasted
        // RAM:
        int extra = minTargetSize >> 3;

        if (extra < 3) {
            // for very small arrays, where constant overhead of
            // realloc is presumably relatively high, we grow
            // faster
            extra = 3;
        }

        int newSize = minTargetSize + extra;

        // add 7 to allow for worst case byte alignment addition below:
        if (newSize + 7 < 0) {
            // int overflowed -- return max allowed array size
            return Integer.MAX_VALUE;
        }

        if (Constants.JRE_IS_64BIT) {
            // round up to 8 byte alignment in 64bit env
            switch (bytesPerElement) {
            case 4:
                // round up to multiple of 2
                return (newSize + 1) & 0x7ffffffe;
            case 2:
                // round up to multiple of 4
                return (newSize + 3) & 0x7ffffffc;
            case 1:
                // round up to multiple of 8
                return (newSize + 7) & 0x7ffffff8;
            case 8:
                // no rounding
            default:
                // odd (invalid?) size
                return newSize;
            }
        }
        else {
            // round up to 4 byte alignment in 64bit env
            switch (bytesPerElement) {
            case 2:
                // round up to multiple of 2
                return (newSize + 1) & 0x7ffffffe;
            case 1:
                // round up to multiple of 4
                return (newSize + 3) & 0x7ffffffc;
            case 4:
            case 8:
                // no rounding
            default:
                // odd (invalid?) size
                return newSize;
            }
        }
    }
}