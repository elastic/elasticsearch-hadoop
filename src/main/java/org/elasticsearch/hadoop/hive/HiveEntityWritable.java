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
import org.elasticsearch.hadoop.util.StringUtils;

/**
 * Replacement of {@link BytesWritable} that allows direct access to the underlying byte array without copying.
 * Used to wrap already json serialized hive entities.
 */
public class HiveEntityWritable extends BinaryComparable implements WritableComparable<BinaryComparable> {

    private int size;
    private byte[] bytes;
    private byte[] id;
    private int idSize;

    public HiveEntityWritable() {
        bytes = null;
    }

    public int getLength() {
        return size + idSize;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public void setContent(byte[] bytes, int size) {
        this.bytes = bytes;
        this.size = size;
    }

    public void setId(byte[] id) {
        this.id = id;
        this.size += idSize;
    }

    public byte[] getId() {
        return id;
    }

    // inherit javadoc
    public void readFields(DataInput in) throws IOException {
        size = in.readInt();
        in.readFully(bytes, 0, size);
        idSize = in.readInt();
        if (idSize != 0) {
            in.readFully(id, 0, idSize);
        }
    }

    // inherit javadoc
    public void write(DataOutput out) throws IOException {
        out.writeInt(size);
        out.write(bytes, 0, size);
        out.writeInt(idSize);
        if (idSize > 0) {
            out.write(id, 0, idSize);
        }
    }

    public int hashCode() {
        return super.hashCode();
    }

    /**
     * Are the two byte sequences equal?
     */
    public boolean equals(Object right_obj) {
        if (right_obj instanceof HiveEntityWritable)
            return super.equals(right_obj);
        return false;
    }

    /**
     * Generate the stream of bytes as hex pairs separated by ' '.
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (idSize > 0) {
            sb.append("id[");
            sb.append(new String(id, 0, idSize, StringUtils.UTF_8));
            sb.append("]=");
        }
        sb.append(new String(bytes, 0, size, StringUtils.UTF_8));
        return sb.toString();
    }


    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(HiveEntityWritable.class);
        }

        /**
         * Compare the buffers in serialized form.
         */
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return compareBytes(b1, s1 + 4, l1 - 4, b2, s2 + 4, l2 - 4);
        }
    }

    static { // register this comparator
        WritableComparator.define(HiveEntityWritable.class, new Comparator());
    }
}