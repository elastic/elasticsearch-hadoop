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
package org.elasticsearch.hadoop.util;


/**
 * Wrapper class around a bytes array so that it can be passed as reference even if the underlying array is modified.
 * Allows only a part of the array to be used (slicing).
 */
public class BytesArray {

    public static final byte[] EMPTY = new byte[0];

    byte[] bytes = EMPTY;
    int size = 0;

    public BytesArray(int size) {
        this(new byte[size], 0);
    }

    public BytesArray(byte[] data) {
        this.bytes = data;
        this.size = data.length;
    }

    public BytesArray(byte[] data, int size) {
        this.bytes = data;
        this.size = size;
    }

    public byte[] bytes() {
        return bytes;
    }

    public int size() {
        return size;
    }

    public int capacity() {
        return bytes.length;
    }

    public int available() {
        return bytes.length - size;
    }

    public void bytes(byte[] array, int size) {
        this.bytes = array;
        this.size = size;
    }

    public void bytes(String from) {
        UnicodeUtil.UTF16toUTF8(from, 0, from.length(), this);
    }

    public void size(int size) {
        this.size = size;
    }

    public void increment(int delta) {
        this.size += delta;
    }

    @Override
    public String toString() {
        return StringUtils.asUTFString(bytes, size);
    }

    public void reset() {
        size = 0;
    }

    public void copyTo(BytesArray to) {
        System.arraycopy(bytes, 0, to.bytes(), to.size(), size);
        to.increment(size);
    }
}