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
package org.elasticsearch.hadoop.cascading;

import java.lang.reflect.Type;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.elasticsearch.hadoop.mr.LinkedMapWritable;
import org.elasticsearch.hadoop.util.ObjectUtils;

import cascading.cascade.Cascade;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.hadoop.TupleSerializationProps;
import cascading.tuple.type.CoercibleType;
import cascading.util.Util;

abstract class CascadingUtils {

    static boolean COERCION_AVAILABLE = ObjectUtils.isClassPresent("cascading.tuple.type.CoercibleType",
            Cascade.class.getClassLoader());

    static abstract class Coercer {

        static Type[] types(Fields fields) {
            return fields.getTypes();
        }

        static Object coerceIfPossible(Type type, Object value) {
            return (type instanceof CoercibleType<?> ? ((CoercibleType<?>) type).canonical(value) : value);
        }
    }

    public static void addSerializationToken(Object config) {
        Configuration cfg = (Configuration) config;
        String tokens = cfg.get(TupleSerializationProps.SERIALIZATION_TOKENS);

        String lmw = LinkedMapWritable.class.getName();

        // if no tokens are defined, add one starting with 140
        if (tokens == null) {
            cfg.set(TupleSerializationProps.SERIALIZATION_TOKENS, "140=" + lmw);
            LogFactory.getLog(ESTap.class).trace(String.format("Registered Cascading serialization token %s for %s", 140, lmw));
        }
        else {
            // token already registered
            if (tokens.contains(lmw)) {
                return;
            }

            // find token id
            Map<Integer, String> mapping = new LinkedHashMap<Integer, String>();
            tokens = tokens.replaceAll("\\s", ""); // allow for whitespace in token set

            for (String pair : tokens.split(",")) {
                String[] elements = pair.split("=");
                mapping.put(Integer.parseInt(elements[0]), elements[1]);
            }

            for (int id = 140; id < 255; id++) {
                if (!mapping.containsKey(Integer.valueOf(id))) {
                    cfg.set(TupleSerializationProps.SERIALIZATION_TOKENS, Util.join(",", Util.removeNulls(tokens, id + "=" + lmw)));
                    LogFactory.getLog(ESTap.class).trace(String.format("Registered Cascading serialization token %s for %s", id, lmw));
                    return;
                }
            }
        }
    }

    public static Type[] getTypes(Fields fields) {
        return (COERCION_AVAILABLE ? Coercer.types(fields) : null);
    }

    public static void unwrapMap(Map context, Tuple tuple, Fields fields, Type[] types) {
        if (context == null) {
            tuple.add(Tuple.NULL);
            return;
        }

        if (fields == null || types == null) {
            for (Object value: context.values()) {
                tuple.add(value);
            }
            return;
        }

        Iterator<?> iterator = context.values().iterator();

        for (int index = 0; index < fields.size(); index++) {
            Object value = (iterator.hasNext() ? iterator.next() : null);
            tuple.add((COERCION_AVAILABLE ? Coercer.coerceIfPossible(types[index], value) : value));
        }
    }
}