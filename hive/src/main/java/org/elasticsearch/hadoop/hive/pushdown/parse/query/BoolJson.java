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
 * Bool operator.
 * <p>
 * It is equal to bool operation in elasticsearch syntax. And using it after elasticsearch 5.X version
 */
public class BoolJson extends JsonObj {
    public String getKey() {
        return "bool";
    }

    public List<JsonObj> getList(String key) {
        JsonObj bool = this;
        List<JsonObj> l = (List<JsonObj>) (bool.get(key));
        if (l == null && ("must".equals(key) || "should".equals(key) || "must_not".equals(key) || "filter".equals(key))) {
            synchronized (this) {
                if (!bool.containsKey(key))
                    bool.put(key, new ArrayList<JsonObj>());
            }
            l = (List<JsonObj>) (bool.get(key));
        }
        return l;
    }

    public BoolJson filter(JsonObj obj) {
        if (obj == null)
            return this;

        String key = obj.getKey();
        if (StringUtils.hasText(key))
            getList("filter").add(new JsonObj(obj.getKey(), obj));
        else {
            getList("filter").add(obj);
        }
        return this;
    }

    public BoolJson must(JsonObj obj) {
        if (obj == null)
            return this;

        String key = obj.getKey();
        if (StringUtils.hasText(key))
            getList("must").add(new JsonObj(obj.getKey(), obj));
        else {
            getList("must").add(obj);
        }
        return this;
    }

    public BoolJson mustNot(JsonObj obj) {
        if (obj == null)
            return this;

        String key = obj.getKey();
        if (StringUtils.hasText(key))
            getList("must_not").add(new JsonObj(obj.getKey(), obj));
        else {
            getList("must_not").add(obj);
        }
        return this;
    }

    public BoolJson should(JsonObj obj) {
        if (obj == null)
            return this;

        String key = obj.getKey();
        if (StringUtils.hasText(key))
            getList("should").add(new JsonObj(obj.getKey(), obj));
        else {
            getList("should").add(obj);
        }
        return this;
    }

    public BoolJson merge(BoolJson other) {
        if (other.containsKey("must")) {
            this.getList("must").addAll(other.getList("must"));
        }
        if (other.containsKey("must_not")) {
            this.getList("must_not").addAll(other.getList("must_not"));
        }
        if (other.containsKey("should")) {
            this.getList("should").addAll(other.getList("should"));
        }
        if (other.containsKey("filter")) {
            this.getList("filter").addAll(other.getList("filter"));
        }

        return this;
    }
}
