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

import java.util.List;

/**
 * Term opeartor.
 * <p>
 * It is equal to term operation in elasticsearch syntax
 */
public class TermJson extends JsonObj {
    public TermJson() {
        key = "term";
    }

    public TermJson(String field, List<String> terms) {
        this();
        term(field, terms);
    }

    public TermJson(String field, String term) {
        this();
        term(field, term);
    }

    public TermJson term(String field, List<String> terms) {
        if (terms.size() == 1)
            put(field, terms.get(0));
        else if (terms.size() > 1) {
            put(field, terms);
            key = "terms";
        }
        return this;
    }

    public TermJson term(String field, String t) {
        put(field, t);
        return this;
    }

    @Override
    public String getKey() {
        return key;
    }
}
