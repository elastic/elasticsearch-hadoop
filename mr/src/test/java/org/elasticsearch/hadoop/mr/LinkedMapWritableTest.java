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
