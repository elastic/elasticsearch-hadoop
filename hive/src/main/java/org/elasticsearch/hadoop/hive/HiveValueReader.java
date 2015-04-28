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

import java.sql.Timestamp;

import javax.xml.bind.DatatypeConverter;

import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.Writable;
import org.elasticsearch.hadoop.mr.WritableValueReader;

public class HiveValueReader extends WritableValueReader {

    @Override
    protected Class<? extends Writable> dateType() {
        return TimestampWritable.class;
    }

    @Override
    protected Class<? extends Writable> doubleType() {
        return DoubleWritable.class;
    }

    @Override
    protected Class<? extends Writable> byteType() {
        return ByteWritable.class;
    }

    @Override
    protected Class<? extends Writable> shortType() {
        return ShortWritable.class;
    }

    @Override
    protected Object parseDate(Long value, boolean richDate) {
        return (richDate ? new TimestampWritable(new Timestamp(value)) : processLong(value));
    }

    @Override
    protected Object parseDate(String value, boolean richDate) {
        return (richDate ? new TimestampWritable(new Timestamp(DatatypeConverter.parseDateTime(value).getTimeInMillis())) : parseString(value));
    }

    @Override
    protected Object processDouble(Double value) {
        return new DoubleWritable(value);
    }

    @Override
    protected Object processByte(Byte value) {
        return new ByteWritable(value);
    }

    @Override
    protected Object processShort(Short value) {
        return new ShortWritable(value);
    }
}