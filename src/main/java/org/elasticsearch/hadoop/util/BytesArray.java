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
 * Wrapper class around a bytes array that can be used as a reference.
 * Used to allow the array to be resized.
 */
public class BytesArray {

    byte[] bytes;
    int size;

    public BytesArray(int size) {
        this(new byte[size], 0);
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

    public void bytes(byte[] array, int size) {
        this.bytes = array;
        this.size = size;
    }

    public void size(int size) {
        this.size = size;
    }

    public void increment(int delta) {
        this.size += delta;
    }

    @Override
    public String toString() {
        return new String(bytes, 0, size, StringUtils.UTF_8);
    }

    public void reset() {
        size = 0;
    }
}
