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

import java.util.Calendar;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.io.Writable;
import org.elasticsearch.hadoop.mr.WritableValueWriter;
import org.elasticsearch.hadoop.serialization.Generator;

/**
 * Writer for the Hive specific Writable types (specifically from serde2.io package).
 */
public class HiveWritableValueWriter extends WritableValueWriter {

    public HiveWritableValueWriter() {
        super();
    }

    public HiveWritableValueWriter(boolean writeUnknownTypes) {
        super(writeUnknownTypes);
    }

    @Override
    public Result write(Writable writable, Generator generator) {
        if (writable instanceof ByteWritable) {
            generator.writeNumber(((ByteWritable) writable).get());
        }
        else if (writable instanceof DoubleWritable) {
            generator.writeNumber(((DoubleWritable) writable).get());
        }
        else if (writable instanceof ShortWritable) {
            generator.writeNumber(((ShortWritable) writable).get());
        }
        // HiveDecimal - Hive 0.11+
        else if (writable != null && HiveConstants.DECIMAL_WRITABLE.equals(writable.getClass().getName())) {
            generator.writeString(writable.toString());
        }
        // pass the UNIX epoch
        else if (writable instanceof TimestampWritable) {
            long ts = ((TimestampWritable) writable).getTimestamp().getTime();
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(ts);
            generator.writeString(DatatypeConverter.printDateTime(cal));
        }
        // HiveDate - Hive 0.12+
        else if (writable != null && HiveConstants.DATE_WRITABLE.equals(writable.getClass().getName())) {
            generator.writeString(DateWritableWriter.toES(writable));
        }
        // HiveVarcharWritable - Hive 0.12+
        else if (writable != null && HiveConstants.VARCHAR_WRITABLE.equals(writable.getClass().getName())) {
            generator.writeString(writable.toString());
        }
        // HiveChar - Hive 0.13+
        else if (writable != null && HiveConstants.CHAR_WRITABLE.equals(writable.getClass().getName())) {
            generator.writeString(StringUtils.trim(writable.toString()));
        }
        // TimestampWritableV2 - Hive 2.0+
        else if (writable != null && HiveConstants.TIMESTAMP_WRITABLE_V2.equals(writable.getClass().getName())) {
            generator.writeString(TimestampV2Writer.toES(writable));
        }
        // DateWritableV2 - Hive 2.0+
        else if (writable != null && HiveConstants.DATE_WRITABLE_V2.equals(writable.getClass().getName())) {
            generator.writeString(DateWritableWriterV2.toES(writable));
        }
        else {
            return super.write(writable, generator);
        }

        return Result.SUCCESFUL();
    }

    // use nested class to efficiently get a hold of the underlying Date object (w/o doing reparsing, etc...)
    private static abstract class DateWritableWriter {
        static String toES(Object dateWritable) {
            DateWritable dw = (DateWritable) dateWritable;
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(dw.get().getTime());
            return DatatypeConverter.printDate(cal);
        }
    }

    // use nested class to efficiently get a hold of the underlying Date object (w/o doing reparsing, etc...)
    private static abstract class DateWritableWriterV2 {
        static String toES(Object dateWritable) {
            DateWritableV2 dw = (DateWritableV2) dateWritable;
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(dw.get().toEpochMilli());
            return DatatypeConverter.printDate(cal);
        }
    }

    // use nested class to get a hold of the underlying long timestamp
    private static abstract class TimestampV2Writer {
        static String toES(Object tsWriteableV2) {
            TimestampWritableV2 ts = (TimestampWritableV2) tsWriteableV2;
            long t = ts.getTimestamp().toEpochMilli();
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(t);
            return DatatypeConverter.printDateTime(cal);
        }
    }
}
