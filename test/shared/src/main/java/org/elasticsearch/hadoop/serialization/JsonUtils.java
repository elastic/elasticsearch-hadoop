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

package org.elasticsearch.hadoop.serialization;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.elasticsearch.hadoop.rest.EsHadoopParsingException;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.FastByteArrayInputStream;

/**
 * A set of utilities to parse JSON in tests, the same way that the RestClient might parse json data.
 */
public final class JsonUtils {

    private static final ObjectMapper MAPPER;

    static {
        MAPPER = new ObjectMapper();
        MAPPER.configure(DeserializationConfig.Feature.USE_ANNOTATIONS, false);
        MAPPER.configure(SerializationConfig.Feature.USE_ANNOTATIONS, false);
    }

    private JsonUtils() { }

    public static Map<String, Object> asMap(String data) {
        return asMap(new BytesArray(data));
    }

    public static Map<String, Object> asMap(BytesArray bytesArray) {
        return asMap(new FastByteArrayInputStream(bytesArray));
    }

    public static Map<String, Object> asMap(InputStream inputStream) {
        Map<String, Object> map;
        try {
            // create parser manually to lower Jackson requirements
            JsonParser jsonParser = MAPPER.getJsonFactory().createJsonParser(inputStream);
            map = MAPPER.readValue(jsonParser, Map.class);
        } catch (IOException ex) {
            throw new EsHadoopParsingException(ex);
        }
        return map;
    }

    public static final class Query {
        private Query parent = null;
        private String field = null;
        private Integer element = null;

        private Query(String field, Query parent) {
            this.field = field;
            this.parent = parent;
        }

        private Query(int element, Query parent) {
            this.element = element;
            this.parent = parent;
        }

        /**
         * Field name to query from this level of an object
         */
        public Query get(String field) {
            Assert.hasText(field, "Cannot query empty field name");
            return new Query(field, this);
        }

        /**
         * Index of the array from the beginning (starting from zero) or from the end (decreasing from -1)
         */
        public Query get(int element) {
            return new Query(element, this);
        }

        /**
         * @return the path of the json field that this query will follow
         */
        public String path() {
            String prefix = "";
            if (parent != null) {
                prefix = parent.path();
            }

            if (field != null) {
                return prefix + "." + field;
            } else if (element != null) {
                return prefix + "[" + element + "]";
            } else {
                return prefix + ".";
            }
        }

        /**
         * Apply this query to the given Map/List structure.
         */
        public Object apply(Object v) {
            // Root
            Object working = v;
            if (parent != null) {
                working = parent.apply(working);
            }

            if (field == null && element < 0) {
                return working;
            }

            try {
                if (field != null) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> obj = (Map<String, Object>) working;
                    return obj.get(field);
                } else {
                    @SuppressWarnings("unchecked")
                    List<Object> arr = (List<Object>) working;
                    if (element < 0) {
                        // Backward index
                        if (arr.size() < (element * -1)) {
                            throw new ArrayIndexOutOfBoundsException("Backward index [" + element + "] cannot be applied to list of size [" + arr.size() + "]");
                        }
                        return arr.get(arr.size() + element); // Negative indexing
                    } else {
                        if (arr.size() <= element) {
                            throw new ArrayIndexOutOfBoundsException("Index [" + element + "] cannot be applied to list of size [" + arr.size() + "]");
                        }
                        return arr.get(element);
                    }
                }
            } catch (ClassCastException cce) {
                String path = parent == null ? "." : parent.path();
                throw new IllegalArgumentException("Incorrect field type found at [" + path + "]", cce);
            }
        }
    }

    public static Query query(String field) {
        return new Query(field, null);
    }

    public static Query query(int element) {
        return new Query(element, null);
    }
}
