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

/**
 * Range opeartor.
 * <p>
 * It is equal to range operation in elasticsearch syntax
 */
public class RangeJson extends JsonObj {

    @Override
    public String getKey() {
        return "range";
    }

    public RangeJson between(String field, Object start, Object end) {
        JsonObj obj = this.containsKey(field) ? this.getJsonObj(field) : new JsonObj();
        obj.put("gte", start);
        obj.put("lte", end);
        this.put(field, obj);
        return this;
    }

    public RangeJson gt(String field, Object v) {
        JsonObj obj = this.containsKey(field) ? this.getJsonObj(field) : new JsonObj();
        obj.put("gt", v);
        this.put(field, obj);
        return this;
    }

    public RangeJson lt(String field, Object v) {
        JsonObj obj = this.containsKey(field) ? this.getJsonObj(field) : new JsonObj();
        obj.put("lt", v);
        this.put(field, obj);
        return this;
    }

    public RangeJson gte(String field, Object v) {
        JsonObj obj = this.containsKey(field) ? this.getJsonObj(field) : new JsonObj();
        obj.put("gte", v);
        this.put(field, obj);
        return this;
    }

    public RangeJson lte(String field, Object v) {
        JsonObj obj = this.containsKey(field) ? this.getJsonObj(field) : new JsonObj();
        obj.put("lte", v);
        this.put(field, obj);
        return this;
    }

    /**
     * Assuming that field is always on the left.
     *
     * @param operator
     * @param field
     * @param v
     * @return
     */
    public RangeJson singleRange(String operator, String field, Object v) {
        if (">".equals(operator))
            return gt(field, v);
        else if ("<".equals(operator))
            return lt(field, v);
        else if (">=".equals(operator))
            return gte(field, v);
        else if ("<=".equals(operator)) {
            return lte(field, v);
        }
        return null;
    }
}
