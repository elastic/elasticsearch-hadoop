/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticsearch.hadoop.util;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;

import org.apache.hadoop.io.AbstractMapWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Utility class converting standard objects to and from {@link Writable}s.
 */
public abstract class WritableUtils {

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static Writable toWritable(Object object) {
        if (object instanceof Writable) {
            return (Writable) object;
        }
        if (object == null) {
            return NullWritable.get();
        }
        if (object instanceof String) {
            return new Text((String) object);
        }
        if (object instanceof Long) {
            return new VLongWritable((Long) object);
        }
        if (object instanceof Integer) {
            return new VIntWritable((Integer) object);
        }
        if (object instanceof Byte) {
            return new ByteWritable((Byte) object);
        }
        if (object instanceof Double) {
            return new DoubleWritable((Double) object);
        }
        if (object instanceof Float) {
            return new FloatWritable((Float) object);
        }
        if (object instanceof Boolean) {
            return new BooleanWritable((Boolean) object);
        }
        if (object instanceof byte[]) {
            return new BytesWritable((byte[]) object);
        }
        if (object instanceof List) {
            List<Object> list = (List<Object>) object;
            if (!list.isEmpty()) {
                Object first = list.get(0);
                Writable[] content = new Writable[list.size()];
                for (int i = 0; i < content.length; i++) {
                    content[i] = toWritable(list.get(i));
                }
                return new ArrayWritable(toWritable(first).getClass(), content);
            }
            return new ArrayWritable(NullWritable.class, new Writable[0]);
        }
        if (object instanceof SortedSet) {
            SortedMapWritable smap = new SortedMapWritable();
            SortedSet<Object> set = (SortedSet) object;
            for (Object obj : set) {
                smap.put((WritableComparable) toWritable(obj), NullWritable.get());
            }
            return smap;
        }
        if (object instanceof Set) {
            MapWritable map = new MapWritable();
            Set<Object> set = (Set) object;
            for (Object obj : set) {
                map.put(toWritable(obj), NullWritable.get());
            }
            return map;
        }
        if (object instanceof SortedMap) {
            SortedMapWritable smap = new SortedMapWritable();
            Map<Object, Object> map = (Map) object;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                smap.put((WritableComparable) toWritable(entry.getKey()), toWritable(entry.getValue()));
            }
            return smap;
        }
        if (object instanceof Map) {
            MapWritable result = new MapWritable();
            Map<Object, Object> map = (Map) object;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                result.put(toWritable(entry.getKey()), toWritable(entry.getValue()));
            }
            return result;
        }
        // fall-back to bytearray
        return new BytesWritable(object.toString().getBytes());
    }

    public static Object fromWritable(Writable writable) {
        if (writable == null) {
            return null;
        }
        if (writable instanceof NullWritable) {
            return null;
        }
        if (writable instanceof Text) {
            return ((Text) writable).toString();
        }
        if (writable instanceof IntWritable) {
            return ((IntWritable) writable).get();
        }
        if (writable instanceof VLongWritable) {
            return ((VLongWritable) writable).get();
        }
        if (writable instanceof VIntWritable) {
            return ((VIntWritable) writable).get();
        }
        if (writable instanceof ByteWritable) {
            return ((ByteWritable) writable).get();
        }
        if (writable instanceof DoubleWritable) {
            return ((DoubleWritable) writable).get();
        }
        if (writable instanceof FloatWritable) {
            return ((FloatWritable) writable).get();
        }
        if (writable instanceof BooleanWritable) {
            return ((BooleanWritable) writable).get();
        }
        if (writable instanceof BytesWritable) {
            return ((BytesWritable) writable).getBytes();
        }
        if (writable instanceof ArrayWritable) {
            Writable[] writables = ((ArrayWritable) writable).get();
            List<Object> list = new ArrayList<Object>(writables.length);
            for (Writable wrt : writables) {
                list.add(fromWritable(wrt));
            }
            return list;
        }
        if (writable instanceof AbstractMapWritable) {
            // AbstractMap writable doesn't provide any generic signature so we force it
            @SuppressWarnings("unchecked")
            Map<Writable, Writable> smap = (Map<Writable, Writable>) writable;
            Set<Writable> wkeys = smap.keySet();
            Map<Object, Object> map = (writable instanceof SortedMapWritable ? new TreeMap<Object, Object>() : new LinkedHashMap<Object, Object>(wkeys.size()));

            boolean isSet = true;
            for (Writable wKey : wkeys) {
                Writable wValue = smap.get(wKey);
                if (!(wValue instanceof NullWritable)) {
                    isSet = false;
                }
                map.put(fromWritable(wKey), fromWritable(wValue));
            }

            if (isSet) {
                return map.keySet();
            }
            return map;
        }
        // fall-back to bytearray
        return org.apache.hadoop.io.WritableUtils.toByteArray(writable);
    }
}