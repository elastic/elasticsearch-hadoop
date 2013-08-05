package org.elasticsearch.hadoop.hive;

import java.sql.Timestamp;

import javax.xml.bind.DatatypeConverter;

import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.Writable;
import org.elasticsearch.hadoop.mr.WritableValueReader;

public class HiveValueReader extends WritableValueReader {

    @Override
    protected Object date(String value) {
        return new TimestampWritable(new Timestamp(DatatypeConverter.parseDateTime(value).getTimeInMillis()));
    }

    @Override
    protected Class<? extends Writable> dateType() {
        return TimestampWritable.class;
    }
}