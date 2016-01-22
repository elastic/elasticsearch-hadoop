package org.elasticsearch.hadoop.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Field;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.elasticsearch.hadoop.util.ReflectionUtils;

// an array writable implementation that actually is writable
public class WritableArrayWritable extends ArrayWritable {

    static final Field VALUE_CLASS_FIELD;

    static {
        VALUE_CLASS_FIELD = ReflectionUtils.findField(ArrayWritable.class, "valueClass");
        ReflectionUtils.makeAccessible(VALUE_CLASS_FIELD);
    }

    public WritableArrayWritable(Class<? extends Writable> valueClass, Writable[] values) {
        super(valueClass, values);
    }

    public WritableArrayWritable(Class<? extends Writable> valueClass) {
        super(valueClass);
    }

    public WritableArrayWritable(String[] strings) {
        super(strings);
    }

    // constructor used by serialization only
    WritableArrayWritable() {
        super(NullWritable.class);
    }

    public void setValueClass(Class<Writable> valueClass) {
        ReflectionUtils.setField(VALUE_CLASS_FIELD, this, valueClass);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        String valueClass = in.readUTF();
        try {
            ReflectionUtils.setField(VALUE_CLASS_FIELD, this, Class.forName(valueClass, false, getClass().getClassLoader()));
        } catch (ClassNotFoundException ex) {
            throw new IOException("Cannot load class " + valueClass, ex);
        }
        super.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(getValueClass().getName());
        Writable[] writables = get();
        // handle null - ArrayWritable does not
        if (writables == null) {
            out.writeInt(0);
        }
        else {
            super.write(out);
        }
    }
}