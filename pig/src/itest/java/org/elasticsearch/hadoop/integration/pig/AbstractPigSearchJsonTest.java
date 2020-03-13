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
import org.elasticsearch.hadoop.EsAssume;
import org.elasticsearch.hadoop.rest.RestUtils;
import org.elasticsearch.hadoop.util.EsMajorVersion;
import org.elasticsearch.hadoop.util.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.LazyTempFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.elasticsearch.hadoop.util.TestUtils.resource;
import static org.junit.Assert.*;

import static org.hamcrest.Matchers.*;

@RunWith(Parameterized.class)
public class AbstractPigSearchJsonTest extends AbstractPigTests {

    private static int testInstance = 0;
    private static String previousQuery;
    private boolean readMetadata;
    private final EsMajorVersion VERSION = TestUtils.getEsClusterInfo().getMajorVersion();

    @ClassRule
    public static LazyTempFolder tempFolder = new LazyTempFolder();

    @Parameters
    public static Collection<Object[]> queries() {
        return new QueryTestParams(tempFolder).params();
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
        RestUtils.refresh("json-pig*");
    }

    @Test
    public void testTuple() throws Exception {
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                "DEFINE EsStorage org.elasticsearch.hadoop.pig.EsStorage('es.query=" + query + "','es.read.metadata=" + readMetadata +"');" +
                "A = LOAD '"+resource("json-pig-tupleartists", "data", VERSION)+"' USING EsStorage();" +
                "X = LIMIT A 3;" +
                //"DESCRIBE A;";
                "STORE A INTO '" + tmpPig() + "/testtuple';";
        pig.executeScript(script);

        String results = getResults("" + tmpPig() + "/testtuple");

        // remove time itself
        assertThat(results, containsString(tabify("12", "Behemoth", "http://www.last.fm/music/Behemoth", "http://userserve-ak.last.fm/serve/252/54196161.jpg", "2001-10-06T")));
        assertThat(results, containsString(tabify("918", "Megadeth", "http://www.last.fm/music/Megadeth","http://userserve-ak.last.fm/serve/252/8129787.jpg", "2017-10-06T")));
        assertThat(results, containsString(tabify("982", "Foo Fighters", "http://www.last.fm/music/Foo+Fighters","http://userserve-ak.last.fm/serve/252/59495563.jpg", "2017-10-06T")));
    }

    @Test
    public void testTupleWithSchema() throws Exception {
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                "DEFINE EsStorage org.elasticsearch.hadoop.pig.EsStorage('es.query=" + query + "','es.read.metadata=" + readMetadata +"');" +
                "A = LOAD '"+resource("json-pig-tupleartists", "data", VERSION)+"' USING EsStorage() AS (name:chararray);" +
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
                      + "A = LOAD '"+resource("json-pig-fieldalias", "data", VERSION)+"' USING EsStorage();"
                      + "X = LIMIT A 3;"
                      + "STORE A INTO '" + tmpPig() + "/testfieldalias';";
        pig.executeScript(script);

        String results = getResults("" + tmpPig() + "/testfieldalias");

        assertThat(results, containsString(tabify("12", "Behemoth", "http://www.last.fm/music/Behemoth", "http://userserve-ak.last.fm/serve/252/54196161.jpg", "2001-10-06T")));
        assertThat(results, containsString(tabify("918", "Megadeth", "http://www.last.fm/music/Megadeth","http://userserve-ak.last.fm/serve/252/8129787.jpg", "2017-10-06T")));
        assertThat(results, containsString(tabify("982", "Foo Fighters", "http://www.last.fm/music/Foo+Fighters","http://userserve-ak.last.fm/serve/252/59495563.jpg", "2017-10-06T")));
    }

    @Test
    public void testMissingIndex() throws Exception {
        String script =
                      "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                      "DEFINE EsStorage org.elasticsearch.hadoop.pig.EsStorage('es.index.read.missing.as.empty=true','es.query=" + query + "','es.read.metadata=" + readMetadata +"');"
                      + "A = LOAD '"+resource("foo", "bar", VERSION)+"' USING EsStorage();"
                      + "X = LIMIT A 3;"
                      + "STORE A INTO '" + tmpPig() + "/testmissingindex';";
        pig.executeScript(script);

        String results = getResults("" + tmpPig() + "/testmissingindex");
        assertThat(results.length(), is(0));
    }

    @Test
    public void testParentChild() throws Exception {
        EsAssume.versionOnOrBefore(EsMajorVersion.V_5_X, "Parent Child Disabled in 6.0");
        String script =
                      "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                      "DEFINE EsStorage org.elasticsearch.hadoop.pig.EsStorage('es.index.read.missing.as.empty=true','es.query=" + query + "','es.read.metadata=" + readMetadata +"');"
                      + "A = LOAD 'json-pig-pc/child' USING EsStorage();"
                      + "X = LIMIT A 3;"
                      + "STORE A INTO '" + tmpPig() + "/testparentchild';";
        pig.executeScript(script);

        String results = getResults("" + tmpPig() + "/testparentchild");

        assertThat(results, containsString(tabify("12", "Behemoth", "http://www.last.fm/music/Behemoth", "http://userserve-ak.last.fm/serve/252/54196161.jpg", "2001-10-06T")));
        assertThat(results, containsString(tabify("918", "Megadeth", "http://www.last.fm/music/Megadeth","http://userserve-ak.last.fm/serve/252/8129787.jpg", "2017-10-06T")));
        assertThat(results, containsString(tabify("982", "Foo Fighters", "http://www.last.fm/music/Foo+Fighters","http://userserve-ak.last.fm/serve/252/59495563.jpg", "2017-10-06T")));
    }

    @Test
    public void testDynamicPattern() throws Exception {
        Assert.assertTrue(RestUtils.exists(resource("json-pig-pattern-1", "data", VERSION)));
        Assert.assertTrue(RestUtils.exists(resource("json-pig-pattern-5", "data", VERSION)));
        Assert.assertTrue(RestUtils.exists(resource("json-pig-pattern-9", "data", VERSION)));
    }

    @Test
    public void testDynamicPatternFormat() throws Exception {
        Assert.assertTrue(RestUtils.exists(resource("json-pig-pattern-format-2001-10-06", "data", VERSION)));
        Assert.assertTrue(RestUtils.exists(resource("json-pig-pattern-format-2005-10-06", "data", VERSION)));
        Assert.assertTrue(RestUtils.exists(resource("json-pig-pattern-format-2017-10-06", "data", VERSION)));
    }

    private static String tmpPig() {
        return "tmp-pig/json-search-" + testInstance;
    }
}