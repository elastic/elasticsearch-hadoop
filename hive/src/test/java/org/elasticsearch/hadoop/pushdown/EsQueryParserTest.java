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
package org.elasticsearch.hadoop.pushdown;

import org.apache.hadoop.conf.Configuration;
import org.elasticsearch.hadoop.cfg.HadoopSettingsManager;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.hive.pushdown.parse.EsQueryParser;
import org.elasticsearch.hadoop.hive.pushdown.parse.query.JsonObj;
import org.elasticsearch.hadoop.rest.query.QueryBuilder;
import org.elasticsearch.hadoop.rest.query.QueryUtils;
import org.junit.Assert;
import org.junit.Test;

public class EsQueryParserTest {

    @Test
    public void testDslQueryGtEs5() {
        Settings settings = HadoopSettingsManager.loadFrom(new Configuration());
        settings.setQuery("{\"term\":{\"id\":\"1\"}}");
        QueryBuilder queryBuilder = QueryUtils.parseQuery(settings);
        EsQueryParser parser = new EsQueryParser(queryBuilder, true);
        JsonObj jsonObj = parser.parse();
        Assert.assertNotNull(jsonObj);
        Assert.assertEquals("{\"bool\":{\"filter\":{\"term\":{\"id\":\"1\"}}}}", jsonObj.toQuery());
    }

    @Test
    public void testDslQueryLtEs5() {
        Settings settings = HadoopSettingsManager.loadFrom(new Configuration());
        settings.setQuery("{\"term\":{\"id\":\"1\"}}");
        QueryBuilder queryBuilder = QueryUtils.parseQuery(settings);
        EsQueryParser parser = new EsQueryParser(queryBuilder, false);
        JsonObj jsonObj = parser.parse();
        Assert.assertNotNull(jsonObj);
        Assert.assertEquals("{\"and\":{\"filters\":[{\"term\":{\"id\":\"1\"}}]}}", jsonObj.toQuery());
    }

    @Test
    public void testUrlQueryGtLs5() {
        Settings settings = HadoopSettingsManager.loadFrom(new Configuration());
        settings.setQuery("?q=id:1");
        QueryBuilder queryBuilder = QueryUtils.parseQuery(settings);
        EsQueryParser parser = new EsQueryParser(queryBuilder, true);
        JsonObj jsonObj = parser.parse();
        Assert.assertNotNull(jsonObj);
        Assert.assertEquals("{\"bool\":{\"filter\":{\"query_string\":{\"query\":\"id:1\"}}}}", jsonObj.toQuery());
    }


    @Test
    public void testUrlQueryLtLs5() {
        Settings settings = HadoopSettingsManager.loadFrom(new Configuration());
        settings.setQuery("?q=id:1");
        QueryBuilder queryBuilder = QueryUtils.parseQuery(settings);
        EsQueryParser parser = new EsQueryParser(queryBuilder, false);
        JsonObj jsonObj = parser.parse();
        Assert.assertNotNull(jsonObj);
        Assert.assertEquals("{\"and\":{\"filters\":[{\"query_string\":{\"query\":\"id:1\"}}]}}", jsonObj.toQuery());
    }


}
