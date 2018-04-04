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


import org.elasticsearch.hadoop.hive.pushdown.parse.query.AndJson;
import org.elasticsearch.hadoop.hive.pushdown.parse.query.BoolJson;
import org.elasticsearch.hadoop.hive.pushdown.parse.query.ExistsJson;
import org.elasticsearch.hadoop.hive.pushdown.parse.query.MatchJson;
import org.elasticsearch.hadoop.hive.pushdown.parse.query.MatchPhraseJson;
import org.elasticsearch.hadoop.hive.pushdown.parse.query.MissingJson;
import org.elasticsearch.hadoop.hive.pushdown.parse.query.NotJson;
import org.elasticsearch.hadoop.hive.pushdown.parse.query.OrJson;
import org.elasticsearch.hadoop.hive.pushdown.parse.query.RangeJson;
import org.elasticsearch.hadoop.hive.pushdown.parse.query.TermJson;
import org.junit.Assert;
import org.junit.Test;

public class QueryTest {

    @Test
    public void testExistsJson() throws Exception {
        ExistsJson json = new ExistsJson("id");
        Assert.assertEquals("{\"exists\":{\"field\":\"id\"}}", json.toQuery());
    }

    @Test
    public void testMissingJson() throws Exception {
        MissingJson json = new MissingJson("id");
        Assert.assertEquals("{\"missing\":{\"field\":\"id\"}}", json.toQuery());
    }

    @Test
    public void testTermJson() throws Exception {
        TermJson json = new TermJson("id", "1");
        Assert.assertEquals("{\"term\":{\"id\":\"1\"}}", json.toQuery());
    }

    @Test
    public void testMatchJson() throws Exception {
        MatchJson json = new MatchJson("id", "1");
        Assert.assertEquals("{\"match\":{\"id\":{\"query\":\"1\"}}}", json.toQuery());
    }

    @Test
    public void testMatchPhraseJson() throws Exception {
        MatchPhraseJson json = new MatchPhraseJson("id", "1");
        Assert.assertEquals("{\"match_phrase\":{\"id\":{\"query\":\"1\"}}}", json.toQuery());
    }

    @Test
    public void testRangeBetweenJson() throws Exception {
        RangeJson json = new RangeJson();
        json.between("id", 1, 10);
        Assert.assertEquals("{\"range\":{\"id\":{\"gte\":1,\"lte\":10}}}", json.toQuery());
    }

    @Test
    public void testRangeGtJson() throws Exception {
        RangeJson json = new RangeJson();
        json.gt("id", 1);
        Assert.assertEquals("{\"range\":{\"id\":{\"gt\":1}}}", json.toQuery());
    }

    @Test
    public void testRangeGteJson() throws Exception {
        RangeJson json = new RangeJson();
        json.gte("id", 1);
        Assert.assertEquals("{\"range\":{\"id\":{\"gte\":1}}}", json.toQuery());
    }

    @Test
    public void testRangeLtJson() throws Exception {
        RangeJson json = new RangeJson();
        json.lt("id", 10);
        Assert.assertEquals("{\"range\":{\"id\":{\"lt\":10}}}", json.toQuery());
    }

    @Test
    public void testRangeLteJson() throws Exception {
        RangeJson json = new RangeJson();
        json.lte("id", 10);
        Assert.assertEquals("{\"range\":{\"id\":{\"lte\":10}}}", json.toQuery());
    }

    @Test
    public void testAndJson() throws Exception {
        AndJson andJson = new AndJson();
        ExistsJson condition1 = new ExistsJson("id");
        andJson.filters(condition1);
        Assert.assertEquals("{\"and\":{\"filters\":[{\"exists\":{\"field\":\"id\"}}]}}", andJson.toQuery());
    }

    @Test
    public void testOrJson() throws Exception {
        OrJson orJson = new OrJson();
        ExistsJson condition1 = new ExistsJson("id");
        orJson.filters(condition1);
        Assert.assertEquals("{\"or\":{\"filters\":[{\"exists\":{\"field\":\"id\"}}]}}", orJson.toQuery());
    }

    @Test
    public void testNotJson() throws Exception {
        ExistsJson condition1 = new ExistsJson("id");
        NotJson orJson = new NotJson(condition1);
        Assert.assertEquals("{\"not\":{\"filter\":{\"exists\":{\"field\":\"id\"}}}}", orJson.toQuery());
    }

    @Test
    public void testBoolJson() throws Exception {
        BoolJson boolJson = new BoolJson();
        ExistsJson condition1 = new ExistsJson("id");
        boolJson.filter(condition1);
        MatchJson condition2 = new MatchJson("id", "1");
        boolJson.mustNot(condition2);
        MatchPhraseJson condition3 = new MatchPhraseJson("id", "1");
        boolJson.should(condition3);
        Assert.assertEquals("{\"bool\":{\"filter\":[{\"exists\":{\"field\":\"id\"}}],\"must_not\":[{\"match\":{\"id\":{\"query\":\"1\"}}}],\"should\":[{\"match_phrase\":{\"id\":{\"query\":\"1\"}}}]}}", boolJson.toQuery());
    }

}
