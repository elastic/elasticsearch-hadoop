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
package org.elasticsearch.hadoop.hive;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.StringUtils;

/**
 * Replacement of {@link BytesWritable} that allows direct access to the underlying byte array without copying.
 * Used to wrap already json serialized hive entities.
 */
public class HiveBytesArrayWritable extends BinaryComparable implements WritableComparable<BinaryComparable>, Serializable {

    private transient BytesArray ba;

    public HiveBytesArrayWritable() {
        ba = null;
    }

    @Override
    public int getLength() {
        return (ba != null ? ba.length() : 0);
    }

    @Override
    public byte[] getBytes() {
        return (ba != null ? ba.bytes() : BytesArray.EMPTY);
    }

    public void setContent(BytesArray ba) {
        this.ba = ba;
    }

    public BytesArray getContent() {
        return ba;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        byte[] bytes = new byte[size];
        in.readFully(bytes, 0, size);
        ba = new BytesArray(bytes);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        int size = (ba != null ? ba.length() : 0);
        byte[] bytes = (ba != null ? ba.bytes() : BytesArray.EMPTY);

        out.writeInt(size);
        out.write(bytes, 0, size);
    }

    /**
     * Are the two byte sequences equal?
     */
    @Override
    public boolean equals(Object right_obj) {
        if (right_obj instanceof HiveBytesArrayWritable)
            return super.equals(right_obj);
        return false;
    }

    @Override
    public int hashCode() {
        return WritableComparator.hashBytes(ba.bytes(), ba.length());
    }

    /**
     * Generate the stream of bytes as hex pairs separated by ' '.
     */
    @Override
    public String toString() {
        return (ba != null ? StringUtils.asUTFString(ba.bytes(), 0, ba.length()) : "");
    }


    public static class Comparator extends WritableComparator implements Serializable {
        public Comparator() {
            super(HiveBytesArrayWritable.class);
        }

        /**
         * Compare the buffers in serialized form.
         */
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return compareBytes(b1, s1 + 4, l1 - 4, b2, s2 + 4, l2 - 4);
        }
    }

    static { // register this comparator
        WritableComparator.define(HiveBytesArrayWritable.class, new Comparator());
    }
}