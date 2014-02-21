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

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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
            arrayType = DoubleWritable.class;
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
            arrayType = LinkedMapWritable.class;
            break;
        // everything else gets translated to String
        default:
            arrayType = Text.class;
        }

        return new ArrayWritable(arrayType);
    }

    @Override
    public Object addToArray(Object array, List<Object> value) {
        ((ArrayWritable) array).set(value.toArray(new Writable[value.size()]));
        return array;
    }

    protected Class<? extends Writable> dateType() {
        return Text.class;
    }

    @Override
    protected Object binaryValue(byte[] value) {
        return new BytesWritable(value);
    }

    @Override
    protected Object parseBoolean(String value) {
        return new BooleanWritable(Boolean.parseBoolean(value));
    }

    @Override
    protected Object parseDouble(String value) {
        return new DoubleWritable(Double.parseDouble(value));
    }

    @Override
    protected Object parseFloat(String value) {
        return new FloatWritable(Float.parseFloat(value));
    }

    @Override
    protected Object parseLong(String value) {
        return new LongWritable(Long.parseLong(value));
    }

    @Override
    protected Object parseInteger(String value) {
        return new IntWritable(Integer.parseInt(value));
    }

    @Override
    protected Object parseString(String value) {
        return new Text(value);
    }

    @Override
    protected Object nullValue() {
        return NullWritable.get();
    }

    @Override
    protected Object date(String value) {
        return new Text(value);
    }
}