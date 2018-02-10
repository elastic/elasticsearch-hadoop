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

import org.elasticsearch.hadoop.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Or operator
 * <p>
 * It is equal to or operation in elasticsearch 1.X syntax. And using it before elasticsearch 5.X version
 */
public class OrJson extends JsonObj {
    public OrJson() {
        setKey("or");
    }

    public List<JsonObj> getList(String key) {
        JsonObj bool = this;
        List<JsonObj> l = (List<JsonObj>) (bool.get(key));
        if (l == null && ("filters".equals(key))) {
            synchronized (this) {
                if (!bool.containsKey(key))
                    bool.put(key, new ArrayList<JsonObj>());
            }
            l = (List<JsonObj>) (bool.get(key));
        }
        return l;
    }

    public OrJson filters(JsonObj obj) {
        if (obj == null)
            return this;

        String key = obj.getKey();
        if (StringUtils.hasText(key)) {
            if (key.equals(this.getKey())) {
                List<JsonObj> filters = ((OrJson) obj).getList("filters");
                for (JsonObj o : filters) {
                    this.filters(o);
                }
            } else {
                getList("filters").add(new JsonObj(key, obj));
            }
        } else {
            getList("filters").add(obj);
        }
        return this;
    }
}
