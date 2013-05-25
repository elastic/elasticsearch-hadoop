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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Replacement of {@link BytesWritable} that allows direct access to the underlying byte array without copying.
 */
public class FastBytesWritable extends BinaryComparable implements WritableComparable<BinaryComparable> {

    private int size;
    private byte[] bytes;

    public FastBytesWritable() {
        bytes = null;
    }

    /**
     * Create a BytesWritable using the byte array as the initial value.
     * @param bytes This array becomes the backing storage for the object.
     */
    public FastBytesWritable(byte[] bytes, int size) {
        set(bytes, size);
    }

    /**
     * Get the current size of the buffer.
     */
    public int getLength() {
        return size;
    }

    public void set(byte[] bytes, int size) {
        this.bytes = bytes;
        this.size = size;
    }
    /**
     * Get the data from the BytesWritable.
     * @return The data is only valid between 0 and getLength() - 1.
     */
    public byte[] getBytes() {
        return bytes;
    }

    // inherit javadoc
    public void readFields(DataInput in) throws IOException {
        size = in.readInt();
        in.readFully(bytes, 0, size);
    }

    // inherit javadoc
    public void write(DataOutput out) throws IOException {
        out.writeInt(size);
        out.write(bytes, 0, size);
    }

    public int hashCode() {
        return super.hashCode();
    }

    /**
     * Are the two byte sequences equal?
     */
    public boolean equals(Object right_obj) {
        if (right_obj instanceof FastBytesWritable)
            return super.equals(right_obj);
        return false;
    }

    /**
     * Generate the stream of bytes as hex pairs separated by ' '.
     */
    public String toString() {
        StringBuffer sb = new StringBuffer(3 * size);
        for (int idx = 0; idx < size; idx++) {
            // if not the first, put a blank separator in
            if (idx != 0) {
                sb.append(' ');
            }
            String num = Integer.toHexString(0xff & bytes[idx]);
            // if it is only one digit, add a leading 0.
            if (num.length() < 2) {
                sb.append('0');
            }
            sb.append(num);
        }
        return sb.toString();
    }


    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(FastBytesWritable.class);
        }

        /**
         * Compare the buffers in serialized form.
         */
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return compareBytes(b1, s1 + 4, l1 - 4, b2, s2 + 4, l2 - 4);
        }
    }

    static { // register this comparator
        WritableComparator.define(FastBytesWritable.class, new Comparator());
    }
}