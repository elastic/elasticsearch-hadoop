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
package org.elasticsearch.hadoop.serialization;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BytesWritable;
import org.elasticsearch.hadoop.mr.WritableValueReader;
import org.elasticsearch.hadoop.serialization.builder.ValueReader;

import static org.junit.Assert.*;

public class WritableValueReaderTest extends AbstractValueReaderTest {

    @Override
    public ValueReader createValueReader() {
        return new WritableValueReader();
    }

    @Override
    public void checkNull(Object typeFromJson) {
        assertEquals(NullWritable.get(), typeFromJson);
    }

    @Override
    public void checkEmptyString(Object typeFromJson) {
        assertEquals(NullWritable.get(), typeFromJson);
    }

    @Override
    public void checkString(Object typeFromJson) {
        assertEquals(new Text("someText"), typeFromJson);
    }

    @Override
    public void checkInteger(Object typeFromJson) {
        assertEquals(new IntWritable(Integer.MAX_VALUE), typeFromJson);
    }

    @Override
    public void checkLong(Object typeFromJson) {
        assertEquals(new LongWritable(Long.MAX_VALUE), typeFromJson);
    }

    @Override
    public void checkDouble(Object typeFromJson) {
        assertEquals(new DoubleWritable(Double.MAX_VALUE), typeFromJson);
    }

    @Override
    public void checkFloat(Object typeFromJson) {
        assertEquals(Float.MAX_VALUE + "", typeFromJson + "");
    }

    @Override
    public void checkBoolean(Object typeFromJson) {
        assertEquals(new BooleanWritable(Boolean.TRUE), typeFromJson);
    }

    @Override
    public void checkByteArray(Object typeFromJson, String encode) { assertEquals(new Text(encode), typeFromJson); }

    @Override
    public void checkBinary(Object typeFromJson, byte[] encode) { assertEquals(new BytesWritable(encode), typeFromJson); }
}