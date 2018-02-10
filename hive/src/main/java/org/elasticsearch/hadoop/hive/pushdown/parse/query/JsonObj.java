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
package org.elasticsearch.hadoop.hive.pushdown.parse.query;


import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.elasticsearch.hadoop.hive.pushdown.parse.serializer.JsonObjSerializer;

import java.io.IOException;
import java.util.HashMap;

/**
 * When parsing the elastissearch query, transform its data format
 * and can support multi-layer structure.
 */
@JsonSerialize(using = JsonObjSerializer.class)
public class JsonObj extends HashMap<String, Object> {
    protected transient String key = null;

    public JsonObj() {
    }

    public JsonObj(String key, Object value) {
        this();

        boolean canPut = true;
        if (value instanceof String) {
            if (StringUtils.isEmpty((String) value)) {
                canPut = false;
            }
        }

        if (canPut)
            put(key, value);
    }

    public JsonObj addByKey(JsonObj obj) {
        if (!obj.isEmpty())
            put(obj.getKey(), obj);
        return this;
    }

    public JsonObj add(String dftKey, JsonObj obj) {
        if (obj.isEmpty())
            return this;

        if (obj.getKey() != null) {
            obj = new JsonObj().addByKey(obj);
        }
        put(dftKey, obj);
        return this;
    }

    public JsonObj getJsonObj(String key) {
        return (JsonObj) get(key);
    }

    public String getString(String key) {
        return (String) get(key);
    }

    public String getKey() {
        return key;
    }

    public JsonObj setKey(String key) {
        this.key = key;
        return this;
    }

    @Override
    public String toString() {
        try {
            return new ObjectMapper().writeValueAsString(this);
        } catch (IOException e) {
        }
        return null;
    }

    public String toQuery() {
        if (getKey() != null) {
            JsonObj jsonObj = new JsonObj().addByKey(this);
            return jsonObj.toString();
        } else {
            return toString();
        }
    }
}
