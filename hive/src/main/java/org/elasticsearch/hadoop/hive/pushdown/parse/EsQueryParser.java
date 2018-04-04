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
package org.elasticsearch.hadoop.hive.pushdown.parse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.hadoop.hive.pushdown.parse.query.BoolJson;
import org.elasticsearch.hadoop.hive.pushdown.parse.query.JsonObj;
import org.elasticsearch.hadoop.hive.pushdown.parse.query.JsonObjManager;
import org.elasticsearch.hadoop.rest.query.QueryBuilder;

import java.io.IOException;
import java.util.Arrays;

/**
 * parsing es.query value from Hive <tt>TBLPROPERTIES</tt>, then tranfroming to JsonObj.
 * <p>
 * there are three formats of es.query: UrlSearch, DslSearch, PathSearch.
 */
public class EsQueryParser {

    protected static Log log = LogFactory.getLog(EsQueryParser.class);

    protected final QueryBuilder rawQuery;
    protected final boolean isES50;

    public EsQueryParser(QueryBuilder queryBuilder, boolean isES50) {
        this.rawQuery = queryBuilder;
        this.isES50 = isES50;
    }

    /**
     * parse es.query,
     * <p>
     * then choose and execute the appropriate method according to the es.query props
     *
     * @return
     */
    public JsonObj parse() {
        if (rawQuery == null)
            return null;

        try {
            //transform QueryBuilder to custom jsonObj
            JsonObj jsonObj = new ObjectMapper().readValue(rawQuery.toString(), JsonObj.class);
            if (jsonObj == null) return null;

            if (isES50) {
                //if is es5.X or more, using bool to wrapper
                BoolJson wrapper = new BoolJson();
                wrapper.put("filter", jsonObj);
                return wrapper;
            } else {
                //if is less than es5.X, using and to wrapper
                JsonObj and = JsonObjManager.and(isES50, Arrays.asList(jsonObj));
                return and;
            }
        } catch (IOException e) {
            log.error("[EsQueryParser]:" + e.getMessage(), e);
        }
        return null;
    }
}
