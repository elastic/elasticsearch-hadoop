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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.io.Writable;
import org.elasticsearch.hadoop.mr.WritableValueReader;
import org.elasticsearch.hadoop.util.ObjectUtils;

public class HiveValueReader extends WritableValueReader {

    private static Log LOG = LogFactory.getLog(HiveValueReader.class);

    private final boolean timestampV2Present;
    private final Class<? extends Writable> timestampClass;

    public HiveValueReader() {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        this.timestampV2Present = ObjectUtils.isClassPresent(HiveConstants.TIMESTAMP_WRITABLE_V2, contextClassLoader);
        if (timestampV2Present) {
            Class<? extends Writable> clazz;
            try {
                @SuppressWarnings("unchecked")
                Class<? extends Writable> c = (Class<? extends Writable>) Class.forName(HiveConstants.TIMESTAMP_WRITABLE_V2);
                clazz = c; // Get around the warning
            } catch (ClassNotFoundException e) {
                LOG.warn("Could not load " + HiveConstants.TIMESTAMP_WRITABLE_V2 + ". Continuing with legacy Timestamp class.");
                clazz = TimestampWritable.class;
            }
            this.timestampClass = clazz;
        } else {
            this.timestampClass = TimestampWritable.class;
        }
    }

    @Override
    protected Class<? extends Writable> dateType() {
        return timestampClass;
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
        if (richDate) {
            return timestampV2Present ? TimestampWritableV2Reader.readTimestampV2(value) : TimestampWritableReader.readTimestamp(value);
        } else {
            return processLong(value);
        }
    }

    @Override
    protected Object parseDate(String value, boolean richDate) {
        if (richDate) {
            long millis = DatatypeConverter.parseDateTime(value).getTimeInMillis();
            return timestampV2Present ? TimestampWritableV2Reader.readTimestampV2(millis) : TimestampWritableReader.readTimestamp(millis);
        } else {
            return parseString(value);
        }
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

    private static class TimestampWritableReader {
        public static Object readTimestamp(Long value) {
            return new TimestampWritable(new Timestamp(value));
        }
    }

    private static class TimestampWritableV2Reader {
        public static Object readTimestampV2(Long value) {
            return new TimestampWritableV2(org.apache.hadoop.hive.common.type.Timestamp.ofEpochMilli(value));
        }
    }
}