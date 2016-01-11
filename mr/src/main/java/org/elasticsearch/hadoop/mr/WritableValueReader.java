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
package org.elasticsearch.hadoop.mr;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.*;
import org.elasticsearch.hadoop.serialization.FieldType;
import org.elasticsearch.hadoop.serialization.builder.JdkValueReader;

public class WritableValueReader extends JdkValueReader {

    @SuppressWarnings("rawtypes")
    @Override
    public Map createMap() {
        return new LinkedMapWritable();
    }

    @Override
    public Object createArray(FieldType type) {
        Class<? extends Writable> arrayType = null;

        switch (type) {
        case NULL:
            arrayType = NullWritable.class;
            break;
        case STRING:
            arrayType = Text.class;
            break;
        case BYTE:
            arrayType = byteType();
            break;
        case SHORT:
            arrayType = shortType();
            break;
        case INTEGER:
            arrayType = IntWritable.class;
            break;
        case TOKEN_COUNT:
        case LONG:
            arrayType = LongWritable.class;
            break;
        case FLOAT:
            arrayType = FloatWritable.class;
            break;
        case DOUBLE:
            arrayType = doubleType();
            break;
        case BOOLEAN:
            arrayType = BooleanWritable.class;
            break;
        case DATE:
            arrayType = dateType();
            break;
        case BINARY:
            arrayType = BytesWritable.class;
            break;
        case OBJECT:
        case NESTED:
            arrayType = LinkedMapWritable.class;
            break;
            // everything else gets translated to String
        default:
            arrayType = Text.class;
        }

        return new WritableArrayWritable(arrayType);
    }

    @Override
    public Object addToArray(Object array, List<Object> value) {
        ((ArrayWritable) array).set(value.toArray(new Writable[value.size()]));
        return array;
    }

    @Override
    protected int arrayDepth(Object potentialArray) {
        int depth = 0;
        for (; potentialArray instanceof ArrayWritable;) {
            depth++;
            Writable[] array = ((ArrayWritable) potentialArray).get();
            if (array.length > 0) {
                potentialArray = array[0];
            }
        }
        return depth;
    }

    @Override
    protected Object wrapArray(Object array, int extraDepth) {
        Writable wrapper = (Writable) array;
        for (int i = 0; i < extraDepth; i++) {
            wrapper = new ArrayWritable(ArrayWritable.class, new Writable[] { wrapper });
        }
        return wrapper;
    }

    protected Class<? extends Writable> dateType() {
        return Text.class;
    }

    protected Class<? extends Writable> doubleType() {
        return DoubleWritable.class;
    }

    protected Class<? extends Writable> byteType() {
        return ByteWritable.class;
    }

    protected Class<? extends Writable> shortType() {
        return WritableCompatUtil.availableShortType();
    }

    @Override
    protected Object binaryValue(byte[] value) {
        return new BytesWritable(value);
    }

    @Override
    protected Object processBoolean(Boolean value) {
        return new BooleanWritable(value);
    }

    @Override
    protected Object parseDate(Long value, boolean richDate) {
        return processLong(value);
    }

    @Override
    protected Object parseDate(String value, boolean richDate) {
        return parseString(value);
    }

    @Override
    protected Object processDouble(Double value) {
        return new DoubleWritable(value);
    }

    @Override
    protected Object processFloat(Float value) {
        return new FloatWritable(value);
    }

    @Override
    protected Object processLong(Long value) {
        return new LongWritable(value);
    }

    @Override
    protected Object processInteger(Integer value) {
        return new IntWritable(value);
    }

    @Override
    protected Object processByte(Byte value) {
        return new ByteWritable(value);
    }

    @Override
    protected Object processShort(Short value) {
        return WritableCompatUtil.availableShortWritable(value);
    }

    @Override
    protected Object parseString(String value) {
        return new Text(value);
    }

    @Override
    protected Object nullValue() {
        return NullWritable.get();
    }
}