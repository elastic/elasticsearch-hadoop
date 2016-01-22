package org.elasticsearch.hadoop.mr;

import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.elasticsearch.hadoop.util.FastByteArrayInputStream;
import org.elasticsearch.hadoop.util.FastByteArrayOutputStream;
import org.junit.Test;

import static org.junit.Assert.*;

import static org.hamcrest.CoreMatchers.*;

public class LinkedMapWritableTest {

    @Test
    public void testMapWithArrayReadWrite() throws Exception {
        LinkedMapWritable written = new LinkedMapWritable();
        ArrayWritable array = new WritableArrayWritable(Text.class);
        array.set(new Writable[] { new Text("one") , new Text("two"), new Text("three")} );
        written.put(new Text("foo"), array);
        FastByteArrayOutputStream out = new FastByteArrayOutputStream();
        DataOutputStream da = new DataOutputStream(out);
        written.write(da);
        da.close();

        LinkedMapWritable read = new LinkedMapWritable();
        read.readFields(new DataInputStream(new FastByteArrayInputStream(out.bytes())));
        assertThat(read.size(), is(written.size()));
        assertThat(read.toString(), is(written.toString()));
    }
    
    @Test
    public void testEmptyArrayReadWrite() throws Exception {
        ArrayWritable array = new WritableArrayWritable(Text.class);
        FastByteArrayOutputStream out = new FastByteArrayOutputStream();
        DataOutputStream da = new DataOutputStream(out);
        array.write(da);
        da.close();

        WritableArrayWritable waw = new WritableArrayWritable(NullWritable.class);
        waw.readFields(new DataInputStream(new FastByteArrayInputStream(out.bytes())));
        assertSame(array.getValueClass(), waw.getValueClass());
    }
}
