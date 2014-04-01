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

import java.lang.reflect.Constructor;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.elasticsearch.hadoop.EsHadoopIllegalStateException;

/**
 * Utility handling the difference in built-in Writable between Hadoop 1 and 2.
 */
@SuppressWarnings("unchecked")
public abstract class WritableCompatUtil {

    private final static Class<Writable> SHORT_WRITABLE;
    private final static Constructor<Writable> SHORT_CTOR;

    static {
        Class<Writable> clazz = null;
        Constructor<Writable> ctor = null;
        try {
            clazz = (Class<Writable>) Class.forName("org.apache.hadoop.io.ShortWritable", false, IntWritable.class.getClassLoader());
            ctor = clazz.getConstructor(short.class);
        } catch (Exception ex) {
        }

        SHORT_WRITABLE = clazz;
        SHORT_CTOR = ctor;
    }


    public static Class<? extends Writable> availableShortType() {
        return (SHORT_WRITABLE != null ? SHORT_WRITABLE : IntWritable.class);
    }

    public static Writable availableShortWritable(short value) {
        if (SHORT_CTOR != null) {
            try {
                return SHORT_CTOR.newInstance(value);
            } catch (Exception e) {
                throw new EsHadoopIllegalStateException(e);
            }
        }
        return new IntWritable(value);
    }

    public static short unwrap(Writable writable) {
        return Short.parseShort(writable.toString());
    }

    public static boolean isShortWritable(Writable writable) {
        return (SHORT_WRITABLE != null && writable != null && SHORT_WRITABLE.isInstance(writable));
    }
}