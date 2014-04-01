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

import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
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
    public boolean write(Writable writable, Generator generator) {
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
        else {
            return super.write(writable, generator);
        }

        return true;
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
}
