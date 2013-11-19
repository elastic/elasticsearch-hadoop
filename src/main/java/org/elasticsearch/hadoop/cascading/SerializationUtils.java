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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.elasticsearch.hadoop.mr.LinkedMapWritable;

import cascading.tuple.hadoop.TupleSerializationProps;
import cascading.util.Util;

abstract class SerializationUtils {

    public static void addToken(Object config) {
        Configuration cfg = (Configuration) config;
        String tokens = cfg.get(TupleSerializationProps.SERIALIZATION_TOKENS);

        String lmw = LinkedMapWritable.class.getName();

        // if no tokens are defined, add one starting with 140
        if (tokens == null) {
            cfg.set(TupleSerializationProps.SERIALIZATION_TOKENS, Util.join(",", Util.removeNulls(tokens, "140=" + lmw)));
            LogFactory.getLog(ESTap.class).trace(String.format("Registered Cascading serialization token %s for %s", 140, lmw));
        }
        else {
            // token already registered
            if (tokens.contains(lmw)) {
                return;
            }

            // find token id
            Map<Integer, String> mapping = new LinkedHashMap<Integer, String>();
            if (tokens != null) {
                tokens = tokens.replaceAll("\\s", ""); // allow for whitespace in token set

                for (String pair : tokens.split(",")) {
                    String[] elements = pair.split("=");
                    mapping.put(Integer.parseInt(elements[0]), elements[1]);
                }
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
}
