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
package org.elasticsearch.hadoop.integration.pig;

import java.util.Collection;

import org.elasticsearch.hadoop.Provisioner;
import org.elasticsearch.hadoop.QueryTestParams;
import org.elasticsearch.hadoop.mr.RestUtils;
import org.elasticsearch.hadoop.util.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.*;

import static org.hamcrest.Matchers.*;

@RunWith(Parameterized.class)
public class AbstractPigSearchJsonTest extends AbstractPigTests {

    private static int testInstance = 0;
    private static String previousQuery;
    private boolean readMetadata;

    @Parameters
    public static Collection<Object[]> queries() {
        return QueryTestParams.params();
    }

    private final String query;

    public AbstractPigSearchJsonTest(String query, boolean metadata) {
        this.query = query;
        this.readMetadata = metadata;

        if (!query.equals(previousQuery)) {
            previousQuery = query;
            testInstance++;
        }
    }

    @Before
    public void before() throws Exception {
        RestUtils.touch("json-pig");
        RestUtils.refresh("json-pig");
    }

    //@Test
    public void testNestedField() throws Exception {
        String data = "{ \"data\" : { \"map\" : { \"key\" :  10  } } }";
        RestUtils.postData("json-pig/nestedmap" + testInstance, StringUtils.toUTF(data));
        RestUtils.refresh("json-pig");

        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                "DEFINE EsStorage org.elasticsearch.hadoop.pig.EsStorage('es.mapping.names=nested:data.map.key');" +
                //"A = LOAD 'json-pig/nestedmap' USING EsStorage() AS (nested:tuple(key:int));" +
                "A = LOAD 'json-pig/nestedmap" + testInstance + "' USING EsStorage() AS (nested:chararray);" +
                "B = ORDER A BY nested DESC;" +
                "X = LIMIT B 3;" +
                "STORE A INTO '" + tmpPig() + "/testnestedfield';";
        pig.executeScript(script);

        String results = getResults("" + tmpPig() + "/testnestedfield");
        assertThat(results, containsString("10"));

//        script =
//                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
//                "DEFINE EsStorage org.elasticsearch.hadoop.pig.EsStorage('es.query=" + query + "','es.mapping.names=nested:data.map');" +
//                "A = LOAD 'json-pig/nestedmap' USING EsStorage() AS (key:chararray, value:);" +
//                "DESCRIBE A;" +
//                "X = LIMIT A 3;" +
//                "DUMP X;";
//        pig.executeScript(script);
    }



    @Test
    public void testTuple() throws Exception {
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                "DEFINE EsStorage org.elasticsearch.hadoop.pig.EsStorage('es.query=" + query + "','es.read.metadata=" + readMetadata +"');" +
                "A = LOAD 'json-pig/tupleartists' USING EsStorage();" +
                "X = LIMIT A 3;" +
                //"DESCRIBE A;";
                "STORE A INTO '" + tmpPig() + "/testtuple';";
        pig.executeScript(script);

        String results = getResults("" + tmpPig() + "/testtuple");

        // remove time itself
        assertThat(results, containsString(tabify("12", "Behemoth", "http://www.last.fm/music/Behemoth", "http://userserve-ak.last.fm/serve/252/54196161.jpg", "2011-10-06T")));
        assertThat(results, containsString(tabify("918", "Megadeth", "http://www.last.fm/music/Megadeth","http://userserve-ak.last.fm/serve/252/8129787.jpg", "2871-10-06T")));
        assertThat(results, containsString(tabify("982", "Foo Fighters", "http://www.last.fm/music/Foo+Fighters","http://userserve-ak.last.fm/serve/252/59495563.jpg", "2933-10-06T")));
    }

    @Test
    public void testTupleWithSchema() throws Exception {
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                "DEFINE EsStorage org.elasticsearch.hadoop.pig.EsStorage('es.query=" + query + "','es.read.metadata=" + readMetadata +"');" +
                "A = LOAD 'json-pig/tupleartists' USING EsStorage() AS (name:chararray);" +
                "B = ORDER A BY name DESC;" +
                "X = LIMIT B 3;" +
                "STORE B INTO '" + tmpPig() + "/testtupleschema';";
        pig.executeScript(script);

        String results = getResults("" + tmpPig() + "/testtupleschema");
        assertThat(results, containsString("999"));
        assertThat(results, containsString("12"));
        assertThat(results, containsString("230"));
    }

    @Test
    public void testFieldAlias() throws Exception {
        String script =
                      "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                       "DEFINE EsStorage org.elasticsearch.hadoop.pig.EsStorage('es.query="+ query + "','es.read.metadata=" + readMetadata +"');"
                      + "A = LOAD 'json-pig/fieldalias' USING EsStorage();"
                      + "X = LIMIT A 3;"
                      + "STORE A INTO '" + tmpPig() + "/testfieldalias';";
        pig.executeScript(script);

        String results = getResults("" + tmpPig() + "/testfieldalias");

        assertThat(results, containsString(tabify("12", "Behemoth", "http://www.last.fm/music/Behemoth", "http://userserve-ak.last.fm/serve/252/54196161.jpg", "2011-10-06T")));
        assertThat(results, containsString(tabify("918", "Megadeth", "http://www.last.fm/music/Megadeth","http://userserve-ak.last.fm/serve/252/8129787.jpg", "2871-10-06T")));
        assertThat(results, containsString(tabify("982", "Foo Fighters", "http://www.last.fm/music/Foo+Fighters","http://userserve-ak.last.fm/serve/252/59495563.jpg", "2933-10-06T")));
    }

    @Test
    public void testMissingIndex() throws Exception {
        String script =
                      "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                      "DEFINE EsStorage org.elasticsearch.hadoop.pig.EsStorage('es.index.read.missing.as.empty=true','es.query=" + query + "','es.read.metadata=" + readMetadata +"');"
                      + "A = LOAD 'foo/bar' USING EsStorage();"
                      + "X = LIMIT A 3;"
                      + "STORE A INTO '" + tmpPig() + "/testmissingindex';";
        pig.executeScript(script);

        String results = getResults("" + tmpPig() + "/testmissingindex");
        assertThat(results.length(), is(0));
    }

    @Test
    public void testParentChild() throws Exception {
        String script =
                      "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                      "DEFINE EsStorage org.elasticsearch.hadoop.pig.EsStorage('es.index.read.missing.as.empty=true','es.query=" + query + "','es.read.metadata=" + readMetadata +"');"
                      + "A = LOAD 'json-pig/child' USING EsStorage();"
                      + "X = LIMIT A 3;"
                      + "STORE A INTO '" + tmpPig() + "/testparentchild';";
        pig.executeScript(script);

        String results = getResults("" + tmpPig() + "/testparentchild");

        assertThat(results, containsString(tabify("12", "Behemoth", "http://www.last.fm/music/Behemoth", "http://userserve-ak.last.fm/serve/252/54196161.jpg", "2011-10-06T")));
        assertThat(results, containsString(tabify("918", "Megadeth", "http://www.last.fm/music/Megadeth","http://userserve-ak.last.fm/serve/252/8129787.jpg", "2871-10-06T")));
        assertThat(results, containsString(tabify("982", "Foo Fighters", "http://www.last.fm/music/Foo+Fighters","http://userserve-ak.last.fm/serve/252/59495563.jpg", "2933-10-06T")));
    }

    @Test
    public void testDynamicPattern() throws Exception {
        Assert.assertTrue(RestUtils.exists("json-pig/pattern-1"));
        Assert.assertTrue(RestUtils.exists("json-pig/pattern-500"));
        Assert.assertTrue(RestUtils.exists("json-pig/pattern-990"));
    }

    @Test
    public void testDynamicPatternFormat() throws Exception {
        Assert.assertTrue(RestUtils.exists("json-pig/pattern-format-2010-10-06"));
        Assert.assertTrue(RestUtils.exists("json-pig/pattern-format-2200-10-06"));
        Assert.assertTrue(RestUtils.exists("json-pig/pattern-format-2873-10-06"));
    }

    private static String tmpPig() {
        return "tmp-pig/json-search-" + testInstance;
    }
}