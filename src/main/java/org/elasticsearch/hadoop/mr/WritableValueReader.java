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
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.elasticsearch.hadoop.serialization.FieldType;
import org.elasticsearch.hadoop.serialization.JdkValueReader;

public class WritableValueReader extends JdkValueReader {

    @SuppressWarnings("rawtypes")
    @Override
    public Map createMap() {
        return new MapWritable();
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
            arrayType = MapWritable.class;
            break;
        case IP:
            throw new UnsupportedOperationException();
        }

        return new ArrayWritable(arrayType);
    }

    @Override
    public Object addToArray(Object array, List<Object> values) {
        ((ArrayWritable) array).set(values.toArray(new Writable[values.size()]));
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
    protected Object booleanValue(String value) {
        return new BooleanWritable(Boolean.parseBoolean(value));
    }

    @Override
    protected Object doubleValue(String value) {
        return new DoubleWritable(Double.parseDouble(value));
    }

    @Override
    protected Object floatValue(String value) {
        return new FloatWritable(Float.parseFloat(value));
    }

    @Override
    protected Object longValue(String value) {
        return new LongWritable(Long.parseLong(value));
    }

    @Override
    protected Object intValue(String value) {
        return new IntWritable(Integer.parseInt(value));
    }

    @Override
    protected Object textValue(String value) {
        return new Text(value);
    }

    @Override
    protected Object nullValue(String value) {
        return NullWritable.get();
    }

    @Override
    protected Object date(String value) {
        return new Text(value);
    }
}